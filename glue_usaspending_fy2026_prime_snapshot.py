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
source_path = f"s3://{bucket}/bronze/usaspending/dataset=prime_contracts/fy=2026/"
target_path = f"s3://{bucket}/silver/usaspending/dataset=prime_contracts/"
partition_col = "action_date_fiscal_year"
key_col = "contract_transaction_unique_key"
modified_col = "last_modified_date"

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("inferSchema", "false")
    .load(source_path)
    .withColumn(partition_col, F.col(partition_col).cast("string"))
    .withColumn("_modified_ts", F.to_timestamp(F.col(modified_col)))
    .withColumn("ingest_ts", F.current_timestamp())
)

source_rows = df.count()
wrong_fy_rows = df.where(F.col(partition_col) != "2026").count()
missing_key_rows = df.where(F.col(key_col).isNull() | (F.trim(F.col(key_col)) == "")).count()

if not 2_700_000 <= source_rows <= 3_200_000:
    raise RuntimeError(f"Unexpected FY2026 prime source row count: {source_rows}")
if wrong_fy_rows:
    raise RuntimeError(f"Found {wrong_fy_rows} prime rows outside FY2026")
if missing_key_rows:
    raise RuntimeError(f"Found {missing_key_rows} prime rows without a transaction key")

rank = Window.partitionBy(key_col).orderBy(
    F.col("_modified_ts").desc_nulls_last(),
    F.col("ingest_ts").desc_nulls_last(),
)
df_out = (
    df.withColumn("_rn", F.row_number().over(rank))
    .where(F.col("_rn") == 1)
    .drop("_rn", "_modified_ts")
)
output_rows = df_out.count()

if not 2_700_000 <= output_rows <= source_rows:
    raise RuntimeError(f"Unexpected FY2026 prime output row count: {output_rows}")

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
(
    df_out.write.mode("overwrite")
    .partitionBy(partition_col)
    .format("parquet")
    .option("compression", "snappy")
    .save(target_path)
)

print(f"Published strict FY2026 prime snapshot: {source_rows} source rows -> {output_rows} rows")
job.commit()
