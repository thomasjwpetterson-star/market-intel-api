import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

bucket = "a-and-d-intel-lake-newaccount"
source_path = f"s3://{bucket}/bronze/dla/contract_history_normalized/"
target_path = f"s3://{bucket}/silver/dla/fact_contract_history/"

schema = StructType([
    StructField("NIIN", StringType(), True),
    StructField("SECURITY_CLASSIFICATION", StringType(), True),
    StructField("FSC", StringType(), True),
    StructField("UNIT", StringType(), True),
    StructField("CAGE", StringType(), True),
    StructField("CONTRACT_NUMBER", StringType(), True),
    StructField("ORDER_QTY", StringType(), True),
    StructField("AWARD_DATE", StringType(), True),
    StructField("NETPRICE", StringType(), True),
    StructField("PO_NUM", StringType(), True),
    StructField("PO_ITMNO", StringType(), True),
    StructField("ITEM_NAME", StringType(), True),
    StructField("PART_NUMBER", StringType(), True),
    StructField("STD_U_PRICE", StringType(), True),
    StructField("NSN", StringType(), True),
])

df = (
    spark.read.format("csv")
    .schema(schema)
    .option("sep", "|")
    .option("quote", '"')
    .option("escape", '"')
    .option("header", "true")
    .option("recursiveFileLookup", "true")
    .load(source_path)
    .withColumn("source_file", F.input_file_name())
)

if df.limit(1).count() == 0:
    raise RuntimeError("No normalized DLA contract-history rows were found.")

for original_name in df.columns:
    clean_name = (
        original_name.strip().lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace(".", "")
    )
    if original_name != clean_name:
        df = df.withColumnRenamed(original_name, clean_name)

for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        df = df.withColumn(field.name, F.trim(F.col(field.name)))

df = (
    df.withColumn("nsn", F.regexp_replace(F.col("nsn"), "[- ]", ""))
    .withColumn("award_date", F.to_date(F.col("award_date"), "yyyyMMdd"))
    .withColumn("award_year", F.year(F.col("award_date")))
    .withColumn(
        "source_snapshot_year",
        F.regexp_extract(F.col("source_file"), r"year=([^/]+)", 1),
    )
    .withColumn(
        "source_snapshot_month",
        F.regexp_extract(F.col("source_file"), r"month=([^/]+)", 1),
    )
    .withColumn("silver_rebuild_ts", F.current_timestamp())
    .where(F.col("award_year").isNotNull())
)

# A DLA financial line may have several part-number reference rows. Keep those
# relationships, while removing only exact copies repeated across source snapshots.
# Quantity and price are deliberately part of the identity so differing financial
# versions remain inspectable instead of being discarded arbitrarily.
identity_columns = [
    "niin",
    "security_classification",
    "fsc",
    "unit",
    "cage",
    "contract_number",
    "order_qty",
    "award_date",
    "netprice",
    "po_num",
    "po_itmno",
    "item_name",
    "part_number",
    "std_u_price",
    "nsn",
]

latest_source = Window.partitionBy(*identity_columns).orderBy(
    F.when(F.col("source_snapshot_year").rlike(r"^\d{4}$"), F.col("source_snapshot_year").cast("int"))
    .otherwise(F.lit(-1))
    .desc(),
    F.when(F.col("source_snapshot_month").rlike(r"^\d{2}$"), F.col("source_snapshot_month").cast("int"))
    .otherwise(F.lit(-1))
    .desc(),
    F.col("source_file").desc(),
)

df_out = (
    df.withColumn("source_rank", F.row_number().over(latest_source))
    .where(F.col("source_rank") == 1)
    .drop("source_rank")
)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(
    df_out.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("award_year")
    .option("compression", "snappy")
    .save(target_path)
)

print("DLA contract-history rebuild complete with exact-row identity and source lineage.")
job.commit()
