import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

bucket = "a-and-d-intel-lake-newaccount"
bronze_fy2020 = f"s3://{bucket}/bronze/usaspending/dataset=prime_contracts/fy=2020/"
silver_root = f"s3://{bucket}/silver/usaspending/dataset=prime_contracts/"

key_col = "contract_transaction_unique_key"
partition_col = "action_date_fiscal_year"
modified_col = "last_modified_date"

print("=== FY2020 USASPENDING SILVER REBUILD ===")
print(f"Bronze source: {bronze_fy2020}")
print(f"Silver target: {silver_root}")

dynamic_frame = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [bronze_fy2020], "recurse": True},
    format="csv",
    format_options={
        "withHeader": True,
        "separator": ",",
        "quoteChar": '"',
        "escaper": '"',
        "multiLine": True,
        "optimizePerformance": False,
    },
    transformation_ctx="read_bronze_fy2020",
)

if dynamic_frame.count() == 0:
    raise RuntimeError("No rows found in the FY2020 bronze prefix; refusing to overwrite silver.")

df_new = (
    dynamic_frame.toDF()
    .withColumn("ingest_ts", F.current_timestamp())
    .withColumn(partition_col, F.col(partition_col).cast("string"))
    .withColumn(modified_col, F.to_timestamp(F.col(modified_col)))
    .where(F.col(partition_col) == "2020")
)

if key_col not in df_new.columns:
    raise RuntimeError(f"Required transaction identity column is missing: {key_col}")

window = Window.partitionBy(key_col).orderBy(
    F.col(modified_col).desc_nulls_last(),
    F.col("ingest_ts").desc_nulls_last(),
)

df_out = (
    df_new
    .where(F.col(key_col).isNotNull() & (F.trim(F.col(key_col)) != ""))
    .withColumn("row_rank", F.row_number().over(window))
    .where(F.col("row_rank") == 1)
    .drop("row_rank")
)

validation = df_out.agg(
    F.count("*").alias("rows"),
    F.countDistinct(key_col).alias("distinct_transaction_keys"),
    F.sum(F.col("federal_action_obligation").cast("double")).alias("obligations"),
).first()

rows = int(validation["rows"] or 0)
distinct_keys = int(validation["distinct_transaction_keys"] or 0)
obligations = float(validation["obligations"] or 0.0)

print(f"Validated rows: {rows:,}")
print(f"Distinct transaction keys: {distinct_keys:,}")
print(f"FY2020 obligations: ${obligations:,.2f}")

if rows != distinct_keys:
    raise RuntimeError("FY2020 output is not unique by contract_transaction_unique_key.")

# Official USAspending DoD FY2020 contract obligations are approximately $422.45B.
# This broad guard catches another partial load without requiring an exact API match.
if not 350_000_000_000 <= obligations <= 500_000_000_000:
    raise RuntimeError(
        f"FY2020 obligations failed the completeness guard: ${obligations:,.2f}. "
        "Silver was not overwritten."
    )

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(
    df_out.write
    .mode("overwrite")
    .partitionBy(partition_col)
    .format("parquet")
    .option("compression", "snappy")
    .save(silver_root)
)

print("FY2020 silver partition rebuilt successfully.")
job.commit()
