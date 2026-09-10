import sys
from datetime import date

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SNAPSHOT_DATE",
        "CONTRACTHIST_PATH",
        "PROCHIST_PATH",
    ],
)

try:
    parsed_snapshot_date = date.fromisoformat(args["SNAPSHOT_DATE"])
except ValueError as exc:
    raise RuntimeError("SNAPSHOT_DATE must use YYYY-MM-DD format.") from exc

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

BUCKET = "a-and-d-intel-lake-newaccount"
FINANCIAL_TARGET = f"s3://{BUCKET}/silver/dla/fact_procurement_history_v2/"
REFERENCE_TARGET = (
    f"s3://{BUCKET}/silver/dla/fact_contract_history_part_references_v2/"
)

CONTRACT_SCHEMA = StructType(
    [
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
    ]
)

PROCUREMENT_SCHEMA = StructType(
    [
        StructField("NIIN", StringType(), True),
        StructField("SECURITY_CLASSIFICATION", StringType(), True),
        StructField("FSC", StringType(), True),
        StructField("CLIN", StringType(), True),
        StructField("UNIT", StringType(), True),
        StructField("CAGE", StringType(), True),
        StructField("CONTRACT_NUMBER", StringType(), True),
        StructField("REFERENCED_PIID", StringType(), True),
        StructField("ORDER_QTY", StringType(), True),
        StructField("AWARD_DATE", StringType(), True),
        StructField("NETPRICE", StringType(), True),
        StructField("STD_U_PRICE", StringType(), True),
        StructField("PO_NUM", StringType(), True),
        StructField("PO_ITMNO", StringType(), True),
        StructField("PIINSPIINMOD", StringType(), True),
        StructField("SOLIC_AMENDMENT_NUMBER", StringType(), True),
    ]
)

FINANCIAL_KEY_COLUMNS = [
    "niin",
    "fsc",
    "unit",
    "cage",
    "contract_number",
    "order_qty",
    "award_date_raw",
    "netprice",
    "po_num",
    "po_itmno",
]

CONTRACT_REFERENCE_COLUMNS = [
    "niin",
    "security_classification",
    "fsc",
    "unit",
    "cage",
    "contract_number",
    "order_qty",
    "award_date_raw",
    "netprice",
    "po_num",
    "po_itmno",
    "item_name",
    "part_number",
    "std_u_price",
    "nsn",
]


def read_pipe_file(path, schema):
    frame = (
        spark.read.format("csv")
        .schema(schema)
        .option("sep", "|")
        .option("quote", '"')
        .option("escape", '"')
        .option("header", "true")
        .load(path)
        .withColumn("source_file", F.input_file_name())
    )
    for original_name in frame.columns:
        clean_name = (
            original_name.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace("/", "_")
            .replace(".", "")
        )
        if original_name != clean_name:
            frame = frame.withColumnRenamed(original_name, clean_name)
    for field in frame.schema.fields:
        if isinstance(field.dataType, StringType):
            frame = frame.withColumn(
                field.name,
                F.when(F.trim(F.col(field.name)) == "", None).otherwise(
                    F.trim(F.col(field.name))
                ),
            )
    return frame


def add_financial_line_id(frame):
    key_values = [
        F.coalesce(F.col(column).cast("string"), F.lit("<NULL>"))
        for column in FINANCIAL_KEY_COLUMNS
    ]
    return frame.withColumn(
        "financial_line_id",
        F.sha2(F.concat_ws("\u001f", *key_values), 256),
    )


snapshot_date = F.to_date(F.lit(args["SNAPSHOT_DATE"]), "yyyy-MM-dd")

contract_rows = read_pipe_file(args["CONTRACTHIST_PATH"], CONTRACT_SCHEMA)
procurement_rows = read_pipe_file(args["PROCHIST_PATH"], PROCUREMENT_SCHEMA)

if contract_rows.limit(1).count() == 0 or procurement_rows.limit(1).count() == 0:
    raise RuntimeError("A DLA monthly source file is empty; no output was written.")

contract_source_row_count = contract_rows.count()
procurement_source_row_count = procurement_rows.count()
print(
    "DLA source rows: "
    f"contracthist={contract_source_row_count:,}, "
    f"prochist={procurement_source_row_count:,}"
)

contract_rows = (
    contract_rows.withColumnRenamed("award_date", "award_date_raw")
    .withColumn("niin", F.lpad(F.regexp_replace(F.col("niin"), "[^A-Za-z0-9]", ""), 9, "0"))
    .withColumn("fsc", F.lpad(F.regexp_replace(F.col("fsc"), "[^0-9]", ""), 4, "0"))
    .withColumn("cage", F.upper(F.regexp_replace(F.col("cage"), "[^A-Za-z0-9]", "")))
    .withColumn("nsn", F.regexp_replace(F.col("nsn"), "[^A-Za-z0-9]", ""))
)
procurement_rows = (
    procurement_rows.withColumnRenamed("award_date", "award_date_raw")
    .withColumn("niin", F.lpad(F.regexp_replace(F.col("niin"), "[^A-Za-z0-9]", ""), 9, "0"))
    .withColumn("fsc", F.lpad(F.regexp_replace(F.col("fsc"), "[^0-9]", ""), 4, "0"))
    .withColumn("cage", F.upper(F.regexp_replace(F.col("cage"), "[^A-Za-z0-9]", "")))
)

contract_rows = add_financial_line_id(contract_rows)
procurement_rows = add_financial_line_id(procurement_rows)

procurement_attributes = [
    "security_classification",
    "clin",
    "referenced_piid",
    "std_u_price",
    "piinspiinmod",
    "solic_amendment_number",
]

procurement_lines = procurement_rows.groupBy(
    "financial_line_id", *FINANCIAL_KEY_COLUMNS
).agg(
    F.count("*").alias("procurement_source_rows"),
    F.countDistinct(F.struct(*[F.col(c) for c in procurement_attributes])).alias(
        "procurement_attribute_variants"
    ),
    *[F.first(F.col(c), ignorenulls=True).alias(c) for c in procurement_attributes],
    F.first("source_file", ignorenulls=True).alias("procurement_source_file"),
)

conflicting_lines = procurement_lines.where(
    F.col("procurement_attribute_variants") > 1
).count()
if conflicting_lines:
    raise RuntimeError(
        f"Found {conflicting_lines:,} procurement keys with conflicting lineage fields."
    )

reference_rows = contract_rows.dropDuplicates(CONTRACT_REFERENCE_COLUMNS)
reference_summary = reference_rows.groupBy("financial_line_id").agg(
    F.count("*").alias("source_reference_rows"),
    F.countDistinct("part_number").alias("reference_part_number_count"),
    F.countDistinct("item_name").alias("reference_item_name_count"),
    F.countDistinct("nsn").alias("reference_nsn_count"),
    F.sort_array(F.collect_set("part_number")).alias("part_numbers"),
    F.sort_array(F.collect_set("item_name")).alias("item_names"),
    F.sort_array(F.collect_set("nsn")).alias("reported_nsns"),
    F.first("source_file", ignorenulls=True).alias("contract_source_file"),
)

joined = procurement_lines.join(reference_summary, "financial_line_id", "left")

unmatched_procurement_lines = joined.where(
    F.col("source_reference_rows").isNull()
).count()
unmatched_contract_lines = reference_summary.join(
    procurement_lines.select("financial_line_id"),
    "financial_line_id",
    "left_anti",
).count()
if unmatched_procurement_lines or unmatched_contract_lines:
    raise RuntimeError(
        "Contract/procurement source keys do not reconcile: "
        f"procurement_only={unmatched_procurement_lines:,}, "
        f"contract_only={unmatched_contract_lines:,}."
    )

financial_output = (
    joined.withColumn("award_date", F.to_date("award_date_raw", "yyyyMMdd"))
    .withColumn("award_year", F.year("award_date"))
    .withColumn(
        "fiscal_year",
        F.year("award_date") + F.when(F.month("award_date") >= 10, 1).otherwise(0),
    )
    .withColumn("source_snapshot_date", snapshot_date)
    .withColumn("order_quantity", F.col("order_qty").cast("decimal(20,3)"))
    .withColumn("net_price", F.col("netprice").cast("decimal(20,6)"))
    .withColumn("standard_unit_price", F.col("std_u_price").cast("decimal(20,6)"))
    .withColumn(
        "line_value",
        (F.col("order_quantity") * F.col("net_price")).cast("decimal(30,6)"),
    )
    .withColumn(
        "item_name",
        F.when(F.size("item_names") > 0, F.element_at("item_names", 1)),
    )
    .withColumn(
        "nsn",
        F.when(
            F.col("fsc").rlike("^[0-9]{4}$") & F.col("niin").rlike("^[0-9]{9}$"),
            F.concat("fsc", "niin"),
        ).when(F.size("reported_nsns") > 0, F.element_at("reported_nsns", 1)),
    )
    .withColumn(
        "part_number",
        F.when(
            F.col("reference_part_number_count") == 1,
            F.element_at("part_numbers", 1),
        ),
    )
    .withColumn(
        "part_number_reference_status",
        F.when(F.col("reference_part_number_count") == 0, "NO_PART_REFERENCE")
        .when(F.col("reference_part_number_count") == 1, "SINGLE_PART_REFERENCE")
        .otherwise("MULTIPLE_PART_REFERENCES"),
    )
    .drop("award_date_raw")
)

financial_output = financial_output.select(
    "financial_line_id",
    "niin",
    "fsc",
    "unit",
    "cage",
    "contract_number",
    "order_qty",
    "netprice",
    "po_num",
    "po_itmno",
    "procurement_source_rows",
    "procurement_attribute_variants",
    "security_classification",
    "clin",
    "referenced_piid",
    "std_u_price",
    "piinspiinmod",
    "solic_amendment_number",
    "procurement_source_file",
    "source_reference_rows",
    "reference_part_number_count",
    "reference_item_name_count",
    "reference_nsn_count",
    "part_numbers",
    "item_names",
    "reported_nsns",
    "contract_source_file",
    "award_date",
    "fiscal_year",
    "order_quantity",
    "net_price",
    "standard_unit_price",
    "line_value",
    "item_name",
    "nsn",
    "part_number",
    "part_number_reference_status",
    "source_snapshot_date",
    "award_year",
)

validation = financial_output.agg(
    F.count("*").alias("line_count"),
    F.min("award_date").alias("min_award_date"),
    F.max("award_date").alias("max_award_date"),
    F.sum("line_value").alias("total_value"),
    F.sum(F.when(F.col("order_quantity").isNull(), 1).otherwise(0)).alias(
        "invalid_quantity_rows"
    ),
    F.sum(F.when(F.col("net_price").isNull(), 1).otherwise(0)).alias(
        "invalid_price_rows"
    ),
    F.sum(F.when(F.col("award_date").isNull(), 1).otherwise(0)).alias(
        "invalid_award_date_rows"
    ),
    F.sum(F.when(F.col("reference_nsn_count") > 1, 1).otherwise(0)).alias(
        "conflicting_nsn_rows"
    ),
).first()

line_count = int(validation["line_count"] or 0)
total_value = float(validation["total_value"] or 0)
min_award_date = validation["min_award_date"]
max_award_date = validation["max_award_date"]
invalid_quantity_rows = int(validation["invalid_quantity_rows"] or 0)
invalid_price_rows = int(validation["invalid_price_rows"] or 0)
invalid_award_date_rows = int(validation["invalid_award_date_rows"] or 0)
conflicting_nsn_rows = int(validation["conflicting_nsn_rows"] or 0)

print(
    "DLA snapshot validation: "
    f"lines={line_count:,}, value={total_value:,.2f}, "
    f"dates={min_award_date}..{max_award_date}, "
    f"invalid_qty={invalid_quantity_rows:,}, invalid_price={invalid_price_rows:,}, "
    f"invalid_date={invalid_award_date_rows:,}, conflicting_nsn={conflicting_nsn_rows:,}"
)

if not 500_000 <= line_count <= 800_000:
    raise RuntimeError(f"Unexpected procurement-line count: {line_count:,}")
if not 1_000_000_000 <= total_value <= 15_000_000_000:
    raise RuntimeError(f"Unexpected procurement value: {total_value:,.2f}")
if min_award_date is None or max_award_date is None:
    raise RuntimeError("Award-date coverage is missing.")
if min_award_date.year != parsed_snapshot_date.year:
    raise RuntimeError("Snapshot does not begin in its stated calendar year.")
if max_award_date > parsed_snapshot_date:
    raise RuntimeError("Snapshot contains an award date after its snapshot date.")
if invalid_quantity_rows or invalid_price_rows or invalid_award_date_rows:
    raise RuntimeError("Date, quantity or price parsing failed; no output was written.")
# Contract-history can repeat one procurement line against reference numbers carrying
# historical FSC variants. The financial NSN is therefore derived from the line's
# own FSC and NIIN; all reported variants remain available in reported_nsns.

reference_output = (
    reference_rows.withColumn("award_date", F.to_date("award_date_raw", "yyyyMMdd"))
    .withColumn("award_year", F.year("award_date"))
    .withColumn("source_snapshot_date", snapshot_date)
    .withColumn(
        "relationship_source", F.lit("DLA_CONTRACT_HISTORY_AWARD_REFERENCE")
    )
    .drop("award_date_raw")
    .select(
        "niin",
        "security_classification",
        "fsc",
        "unit",
        "cage",
        "contract_number",
        "order_qty",
        "netprice",
        "po_num",
        "po_itmno",
        "item_name",
        "part_number",
        "std_u_price",
        "nsn",
        "source_file",
        "financial_line_id",
        "award_date",
        "relationship_source",
        "source_snapshot_date",
        "award_year",
    )
)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(
    financial_output.write.format("parquet")
    .mode("overwrite")
    .option("compression", "snappy")
    .partitionBy("source_snapshot_date", "award_year")
    .save(FINANCIAL_TARGET)
)

(
    reference_output.write.format("parquet")
    .mode("overwrite")
    .option("compression", "snappy")
    .partitionBy("source_snapshot_date", "award_year")
    .save(REFERENCE_TARGET)
)

print("DLA monthly procurement snapshot published to versioned silver tables.")
job.commit()
