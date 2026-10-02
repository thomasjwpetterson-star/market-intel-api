import sys
import json
import subprocess
import tempfile
import os
import datetime

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =========================
# ARGUMENTS
# =========================
# RUN_ID is optional for automation:
# - If RUN_ID is provided: process that single bronze batch.
# - If RUN_ID is NOT provided: read latest run_id from STATE_URI (S3 JSON).
#
# Required for automation: STATE_URI (so we can locate the latest run_id).
def _read_state_run_id(state_uri: str) -> str:
    if not state_uri.startswith("s3://"):
        raise ValueError(f"STATE_URI must be s3://... got {state_uri}")

    with tempfile.TemporaryDirectory() as d:
        local = os.path.join(d, "state.json")
        subprocess.check_call(["aws", "s3", "cp", state_uri, local])
        with open(local, "r") as f:
            data = json.load(f)

    rid = data.get("run_id")
    if not rid:
        raise RuntimeError(f"STATE file missing run_id: {data}")
    return rid


# Prefer: JOB_NAME + RUN_ID + STATE_URI
# Fallback: JOB_NAME + STATE_URI (derive RUN_ID)
try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "RUN_ID", "STATE_URI"])
except Exception:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "STATE_URI"])
    args["RUN_ID"] = _read_state_run_id(args["STATE_URI"])

RUN_ID = args["RUN_ID"]
STATE_URI = args["STATE_URI"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# =========================
# CONFIG
# =========================
BUCKET = "a-and-d-intel-lake-newaccount"

source_prime_path = f"s3://{BUCKET}/bronze/usaspending/dataset=prime_contracts/run_id={RUN_ID}/"
target_prime_path = f"s3://{BUCKET}/silver/usaspending/dataset=prime_contracts/"

KEY_COL = "contract_transaction_unique_key"
PART_COL = "action_date_fiscal_year"
MOD_COL = "last_modified_date"   # newest record wins

print(f"STATE_URI={STATE_URI}")
print(f"RUN_ID={RUN_ID}")
print(f"Source={source_prime_path}")
print(f"Target={target_prime_path}")

# =========================
# READ NEW BRONZE BATCH
# =========================
dyf_new = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [source_prime_path],
        "recurse": True
    },
    format="csv",
    format_options={
        "withHeader": True,
        "separator": ",",
        "quoteChar": "\"",
        "escaper": "\"",
        "multiLine": True,
        "optimizePerformance": False
    },
    transformation_ctx="read_prime_run"
)

if dyf_new.count() == 0:
    print("No rows found for this RUN_ID. Exiting.")
    job.commit()
    sys.exit(0)

df_new = dyf_new.toDF()

required_columns = {KEY_COL, PART_COL, MOD_COL}
missing_columns = required_columns - set(df_new.columns)
if missing_columns:
    raise RuntimeError(f"New USAspending batch is missing required columns: {sorted(missing_columns)}")

missing_keys = df_new.where(
    F.col(KEY_COL).isNull() | (F.trim(F.col(KEY_COL)) == "")
).count()
if missing_keys:
    raise RuntimeError(f"New USAspending batch contains {missing_keys} rows without transaction keys")

# Normalize types we need for sorting / partitioning
df_new = (
    df_new
    .withColumn("ingest_ts", F.current_timestamp())
    .withColumn(MOD_COL, F.to_timestamp(F.col(MOD_COL)))  # safe if already timestamp-like
    .withColumn(PART_COL, F.col(PART_COL).cast("string"))
)

# The daily feed maintains the active fiscal year and its predecessor.  A
# modification-date query can also return sparse corrections to much older
# records; rewriting every historical FY partition each day would turn a small
# incremental feed into a full-table rewrite.  Older years remain governed by
# the versioned full/delta archive reconciliation.
today = datetime.date.today()
current_fy = today.year + (1 if today.month >= 10 else 0)
allowed_partitions = {str(current_fy), str(current_fy - 1)}
outside_daily_scope = df_new.where(~F.col(PART_COL).isin(sorted(allowed_partitions))).count()
if outside_daily_scope:
    print(
        f"Ignoring {outside_daily_scope} modified rows outside daily FY scope "
        f"{sorted(allowed_partitions)}; archive reconciliation owns older years."
    )
df_new = df_new.where(F.col(PART_COL).isin(sorted(allowed_partitions)))
if df_new.limit(1).count() == 0:
    print("No current/prior-FY rows found in this modified-date batch. Exiting.")
    job.commit()
    sys.exit(0)

# What partitions are impacted by this run?
impacted_partitions = [r[0] for r in df_new.select(PART_COL).distinct().collect() if r[0] is not None]
print(f"Impacted partitions ({PART_COL}) = {impacted_partitions}")

# =========================
# READ EXISTING SILVER (ONLY IMPACTED PARTITIONS)
# =========================
# This production table already exists.  A read failure must stop the job: if
# it were treated as an empty table, the dynamic overwrite below could replace
# a complete fiscal-year partition with only the latest incremental batch.
df_existing = (
    spark.read
    .parquet(target_prime_path)
    .where(F.col(PART_COL).isin(impacted_partitions))
)
df_all = df_existing.unionByName(df_new, allowMissingColumns=True)
print("Read existing silver impacted partitions and unioned with new batch.")

# =========================
# DEDUPE/UPSERT (LATEST WINS)
# =========================
# Keep exactly 1 row per contract_transaction_unique_key:
# highest last_modified_date wins; tie-break by ingest_ts.
w = Window.partitionBy(KEY_COL).orderBy(
    F.col(MOD_COL).desc_nulls_last(),
    F.col("ingest_ts").desc()
)

df_out = (
    df_all
    .withColumn("rn", F.row_number().over(w))
    .where(F.col("rn") == 1)
    .drop("rn")
)

# =========================
# WRITE BACK (OVERWRITE IMPACTED PARTITIONS ONLY)
# =========================
# This prevents duplication while keeping the table partitioned by FY.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(
    df_out
    .write
    .mode("overwrite")
    .partitionBy(PART_COL)
    .format("parquet")
    .option("compression", "snappy")
    .save(target_prime_path)
)

print("Write complete (dynamic overwrite for impacted partitions).")
job.commit()
