"""Glue Spark job: transform a validated USAspending archive without publishing it.

The output is isolated below the acquisition run.  It never writes to the
production Silver table locations or Glue Catalog, so a schema or volume issue
cannot alter a serving view.
"""

import json
import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "RUN_ID", "MANIFEST_KEY"])
bucket = args["BUCKET"]
run_id = args["RUN_ID"]
manifest_key = args["MANIFEST_KEY"]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)
s3 = boto3.client("s3")


def load_manifest():
    response = s3.get_object(Bucket=bucket, Key=manifest_key)
    manifest = json.loads(response["Body"].read())
    if manifest.get("run_id") != run_id:
        raise RuntimeError("USAspending candidate manifest run ID mismatch")
    if manifest.get("status") != "validated-candidate":
        raise RuntimeError("USAspending acquisition candidate is not validated")
    if manifest.get("production_mutations"):
        raise RuntimeError("Candidate manifest unexpectedly declares production mutations")
    return manifest


def read_csvs(paths):
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("inferSchema", "false")
        .load(paths)
    )


def transform_dataset(dataset, artifacts):
    if dataset == "prime_contracts":
        key_col = "contract_transaction_unique_key"
        fy_col = "action_date_fiscal_year"
        modified_col = "last_modified_date"
    elif dataset == "sub_contracts":
        key_col = "subaward_sam_report_id"
        fy_col = "subaward_action_date_fiscal_year"
        modified_col = "subaward_sam_report_last_modified_date"
    else:
        raise RuntimeError(f"Unsupported USAspending dataset: {dataset}")

    metrics = []
    for fiscal_year in sorted({int(item["fiscal_year"]) for item in artifacts}):
        selected = [item for item in artifacts if int(item["fiscal_year"]) == fiscal_year]
        paths = [f"s3://{bucket}/{item['candidate_key']}" for item in selected]
        df = read_csvs(paths)
        missing_columns = {key_col, fy_col, modified_col} - set(df.columns)
        if missing_columns:
            raise RuntimeError(
                f"{dataset} FY{fiscal_year} is missing columns: {sorted(missing_columns)}"
            )
        df = (
            df.withColumn(fy_col, F.col(fy_col).cast("string"))
            .withColumn("_modified_ts", F.to_timestamp(F.col(modified_col)))
            .withColumn("_source_file", F.input_file_name())
        )
        source_rows = df.count()
        wrong_fy_rows = df.where(F.col(fy_col) != str(fiscal_year)).count()
        missing_key_rows = df.where(
            F.col(key_col).isNull() | (F.trim(F.col(key_col)) == "")
        ).count()
        if source_rows <= 0 or wrong_fy_rows or missing_key_rows:
            raise RuntimeError(
                f"Invalid {dataset} FY{fiscal_year}: rows={source_rows}, "
                f"wrong_fy={wrong_fy_rows}, missing_keys={missing_key_rows}"
            )

        rank = Window.partitionBy(key_col).orderBy(
            F.col("_modified_ts").desc_nulls_last(),
            F.col("_source_file").desc(),
        )
        output = (
            df.withColumn("_rn", F.row_number().over(rank))
            .where(F.col("_rn") == 1)
            .drop("_rn", "_modified_ts", "_source_file")
        )
        output_rows = output.count()
        if output_rows <= 0 or output_rows > source_rows:
            raise RuntimeError(
                f"Invalid {dataset} FY{fiscal_year} dedupe result: "
                f"{source_rows} -> {output_rows}"
            )
        output_path = (
            f"s3://{bucket}/mimir/raw-source-candidates/"
            f"usaspending-contract-archive/{run_id}/silver/"
            f"dataset={dataset}/fy={fiscal_year}/"
        )
        output.write.mode("overwrite").format("parquet").option(
            "compression", "snappy"
        ).save(output_path)
        metrics.append(
            {
                "dataset": dataset,
                "fiscal_year": fiscal_year,
                "source_rows": source_rows,
                "output_rows": output_rows,
                "duplicate_rows_removed": source_rows - output_rows,
                "missing_key_rows": missing_key_rows,
                "wrong_fiscal_year_rows": wrong_fy_rows,
                "column_count": len(output.columns),
                "output_prefix": output_path,
            }
        )
    return metrics


manifest = load_manifest()
all_metrics = []
for dataset in ("prime_contracts", "sub_contracts"):
    artifacts = [
        item for item in manifest.get("artifacts", []) if item.get("dataset") == dataset
    ]
    if not artifacts:
        raise RuntimeError(f"No {dataset} artifacts in USAspending candidate manifest")
    all_metrics.extend(transform_dataset(dataset, artifacts))

result = {
    "manifest_version": 1,
    "source_id": "usaspending-contract-archive",
    "run_id": run_id,
    "source_manifest_key": manifest_key,
    "created_at": datetime.now(timezone.utc).isoformat(),
    "status": "transformed-candidate",
    "metrics": all_metrics,
    "production_mutations": [],
    "promotion_required": True,
}
result_key = (
    f"mimir/raw-source-candidates/usaspending-contract-archive/"
    f"{run_id}/silver_manifest.json"
)
s3.put_object(
    Bucket=bucket,
    Key=result_key,
    Body=(json.dumps(result, indent=2, sort_keys=True) + "\n").encode("utf-8"),
    ContentType="application/json",
    ServerSideEncryption="AES256",
)
print(json.dumps({"silver_manifest_key": result_key, "metrics": all_metrics}, indent=2))
job.commit()
