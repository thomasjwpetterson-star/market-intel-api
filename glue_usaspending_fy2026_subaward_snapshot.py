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
source_path = f"s3://{bucket}/bronze/usaspending/dataset=sub_contracts/fy=2026/"
target_path = f"s3://{bucket}/silver/usaspending/dataset=sub_contracts/"
partition_col = "subaward_action_date_fiscal_year"
report_id_col = "subaward_sam_report_id"
modified_col = "subaward_sam_report_last_modified_date"

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

df = df.withColumn(
    "_dedup_key",
    F.when(
        F.col(report_id_col).isNotNull() & (F.trim(F.col(report_id_col)) != ""),
        F.col(report_id_col),
    ).otherwise(
        F.concat_ws(
            "|",
            F.coalesce(F.col("prime_award_unique_key"), F.lit("")),
            F.coalesce(F.col("subaward_number"), F.lit("")),
            F.coalesce(F.col("subaward_action_date"), F.lit("")),
            F.coalesce(F.col("subaward_amount"), F.lit("")),
            F.coalesce(F.col("subawardee_uei"), F.lit("")),
        )
    ),
)

source_rows = df.count()
wrong_fy_rows = df.where(F.col(partition_col) != "2026").count()
missing_report_id_rows = df.where(
    F.col(report_id_col).isNull() | (F.trim(F.col(report_id_col)) == "")
).count()

if not 25_000 <= source_rows <= 50_000:
    raise RuntimeError(f"Unexpected FY2026 subaward source row count: {source_rows}")
if wrong_fy_rows:
    raise RuntimeError(f"Found {wrong_fy_rows} subaward rows outside FY2026")
if missing_report_id_rows:
    raise RuntimeError(f"Found {missing_report_id_rows} subaward rows without a SAM report ID")

rank = Window.partitionBy("_dedup_key").orderBy(
    F.col("_modified_ts").desc_nulls_last(),
    F.col("ingest_ts").desc_nulls_last(),
)
df_out = (
    df.withColumn("_rn", F.row_number().over(rank))
    .where(F.col("_rn") == 1)
    .drop("_rn", "_modified_ts", "_dedup_key")
)
output_rows = df_out.count()

if not 25_000 <= output_rows <= source_rows:
    raise RuntimeError(f"Unexpected FY2026 subaward output row count: {output_rows}")

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
(
    df_out.write.mode("overwrite")
    .partitionBy(partition_col)
    .format("parquet")
    .option("compression", "snappy")
    .save(target_path)
)

print(f"Published strict FY2026 subaward snapshot: {source_rows} source rows -> {output_rows} rows")
job.commit()
