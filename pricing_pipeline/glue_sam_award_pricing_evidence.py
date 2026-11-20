"""Materialize attachment quantity/price evidence linked to USAspending actions.

Grain: one SAM attachment line candidate x one exact SAM/USAspending bridge row.
Unlinked attachment rows are retained once.  Candidate arithmetic is exposed,
but only narrowly scoped rows are marked eligible for review; nothing enters a
Mimir price range automatically.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F


# Quantity candidates are not executed prices. A physical support (e.g. a
# thigh support) must not be classified as a service by the noun SUPPORT.
ANCILLARY_QUANTITY_PATTERN = (
    r"WARRANTY|DATA REQUIREMENT|MANUAL|CALIBRATION CERTIFICATE|TRAINING|MAINTENANCE|"
    r"\b(?:TECHNICAL|LOGISTICS?|ENGINEERING|PROGRAM(?:ME)?|OPERATIONS?|MISSION|"
    r"CUSTOMER|ADMINISTRATIVE|FACILIT(?:Y|IES))\s+SUPPORT\b|\bSUPPORT\s+SERVICES?\b"
)
# Numeric ordering must also accept parser versions with semantic suffixes.
PARSER_VERSION_RANK_PATTERN = r"-v([0-9]+)(?:-|$)"

TRANSFORM_VERSION = "mimir-sam-award-pricing-evidence-2026-11-v3-source-scope"
ARGS = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "RUN_ID"])
BUCKET = ARGS["BUCKET"]
STARTED_AT = datetime.now(timezone.utc)
RUN_ID = STARTED_AT.strftime("%Y%m%dT%H%M%SZ") if ARGS["RUN_ID"] == "scheduled" else ARGS["RUN_ID"]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(ARGS["JOB_NAME"], ARGS)

spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set(
    "spark.sql.catalog.glue_catalog.warehouse",
    f"s3://{BUCKET}/silver/pricing/warehouse/",
)
spark.conf.set(
    "spark.sql.catalog.glue_catalog.catalog-impl",
    "org.apache.iceberg.aws.glue.GlueCatalog",
)
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

TARGET = "glue_catalog.market_intel_silver.fact_sam_award_pricing_evidence"
BRIDGE = "glue_catalog.market_intel_silver.bridge_sam_usaspending_awards"
SOURCE = f"s3://{BUCKET}/silver/pricing/sam_attachment_line_items/run_id=*/candidates.jsonl"
STAGING = f"s3://{BUCKET}/silver/pricing/sam_award_pricing_evidence_staging/run_id={RUN_ID}/"
MANIFEST_KEY = f"silver/pricing/sam_award_pricing_evidence_manifests/run_id={RUN_ID}/manifest.json"


parser_rank = Window.partitionBy("line_candidate_id").orderBy(
    F.regexp_extract("parser_version", PARSER_VERSION_RANK_PATTERN, 1).cast("int").desc_nulls_last(),
    F.col("parser_version").desc(),
    F.col("transform_run_id").desc(),
)
raw_lines = spark.read.json(SOURCE)
# Older parser runs predate source semantics. Preserve new fields without
# requiring a destructive backfill or inventing a role for older evidence.
for column in ("document_role", "source_value_basis"):
    if column not in raw_lines.columns:
        raw_lines = raw_lines.withColumn(column, F.lit(None).cast("string"))
lines = (
    raw_lines
    .where("line_candidate_id IS NOT NULL AND notice_id IS NOT NULL AND attachment_sha256 IS NOT NULL")
    .withColumn("candidate_rank", F.row_number().over(parser_rank))
    .where("candidate_rank = 1")
    .drop("candidate_rank")
    .withColumn("quantity", F.col("quantity").cast("decimal(20,6)"))
    .withColumn("source_unit_price_usd", F.col("source_unit_price_usd").cast("decimal(30,6)"))
    .withColumn("extended_line_amount_usd", F.col("extended_line_amount_usd").cast("decimal(30,6)"))
    .withColumn("attachment_derived_unit_price_usd", F.col("derived_unit_price_usd").cast("decimal(30,6)"))
    .drop("derived_unit_price_usd")
    .withColumn(
        "line_normalized_solicitation_id",
        F.upper(F.trim(F.coalesce(F.col("solicitation_id"), F.lit("")))),
    )
    .withColumn(
        "quantity_scope_status",
        F.when(F.col("quantity").isNull(), F.lit("NO_QUANTITY"))
        .when(
            F.col("clin").isNull()
            & F.upper(F.coalesce("source_row_text", F.lit(""))).rlike(
                r"WORKBOOK|RESPONSES?|CHARACTER COUNT|SEPARATE TAB|TEMPLATE[-_ ]"
            ),
            F.lit("NON_PROCUREMENT_INSTRUCTIONAL_COUNT"),
        )
        .when(
            F.upper(F.coalesce("item_description", F.lit(""))).rlike(
                ANCILLARY_QUANTITY_PATTERN
            ),
            F.lit("ANCILLARY_OR_SERVICE_QUANTITY"),
        )
        .otherwise(F.lit("PROCUREMENT_LINE_QUANTITY_CANDIDATE")),
    )
)

notice_line_scope = lines.groupBy("notice_id").agg(
    F.countDistinct(
        F.when(
            (F.col("quantity") > 0)
            & (~F.upper(F.coalesce("item_description", F.lit(""))).rlike(
                ANCILLARY_QUANTITY_PATTERN
            )),
            F.col("line_candidate_id"),
        )
    ).cast("bigint").alias("notice_procurement_quantity_line_count"),
    F.countDistinct("line_candidate_id").cast("bigint").alias("notice_line_candidate_count"),
)
lines = lines.join(notice_line_scope, "notice_id", "left")

bridge = spark.table(BRIDGE).select(
    "sam_usaspending_award_bridge_id",
    F.col("notice_id").alias("bridge_notice_id"),
    F.col("notice_version_id").alias("bridge_notice_version_id"),
    F.col("notice_source_system").alias("bridge_notice_source_system"),
    F.col("normalized_solicitation_id").alias("bridge_normalized_solicitation_id"),
    F.col("normalized_award_number").alias("bridge_normalized_award_number"),
    "link_method",
    "link_confidence",
    "contract_transaction_unique_key",
    "contract_award_unique_key",
    "amount_evidence_id",
    "award_id_piid",
    "parent_award_id_piid",
    "solicitation_identifier",
    "modification_number",
    "action_date",
    "action_class",
    "primary_amount_usd",
    "primary_amount_basis",
    "signed_action_obligation_usd",
    "current_total_value_of_award_usd",
    "base_and_exercised_options_value_usd",
    "potential_total_value_of_award_usd",
    "transaction_description",
    "prime_award_base_transaction_description",
    "product_or_service_code",
    "product_or_service_code_description",
    "awarding_agency_name",
    "awarding_sub_agency_name",
    "awarding_office_name",
    "recipient_name",
    "recipient_uei",
    "cage_code",
    "usaspending_permalink",
    F.col("source_s3_uri").alias("usaspending_source_s3_uri"),
    F.col("source_s3_version_id").alias("usaspending_source_s3_version_id"),
    F.col("source_etag").alias("usaspending_source_etag"),
    F.col("source_text_sha256").alias("usaspending_source_sha256"),
    F.col("source_locator").alias("usaspending_source_locator"),
    "award_action_count",
    "positive_obligation_action_count",
    "solicitation_award_count",
    "solicitation_notice_count",
)

direct_links = (
    lines.join(bridge, lines.notice_id == bridge.bridge_notice_id, "inner")
    .withColumn("attachment_to_bridge_link_method", F.lit("SAME_SAM_NOTICE_ID"))
    .withColumn("attachment_to_bridge_link_priority", F.lit(1))
)
solicitation_links = (
    lines.where("line_normalized_solicitation_id <> ''")
    .join(
        bridge,
        F.col("line_normalized_solicitation_id")
        == F.col("bridge_normalized_solicitation_id"),
        "inner",
    )
    .withColumn(
        "attachment_to_bridge_link_method",
        F.lit("EXACT_SOLICITATION_ID_TO_AWARD_BRIDGE"),
    )
    .withColumn("attachment_to_bridge_link_priority", F.lit(2))
)
linked_rank = Window.partitionBy(
    "line_candidate_id", "sam_usaspending_award_bridge_id"
).orderBy("attachment_to_bridge_link_priority")
linked = (
    direct_links.unionByName(solicitation_links)
    .withColumn("attachment_bridge_duplicate_rank", F.row_number().over(linked_rank))
    .where("attachment_bridge_duplicate_rank = 1")
    .drop("attachment_bridge_duplicate_rank")
)
unlinked = lines.join(
    linked.select("line_candidate_id").distinct(), "line_candidate_id", "left_anti"
).withColumn(
    "attachment_to_bridge_link_method", F.lit("NO_EXACT_AWARD_BRIDGE_LINK")
).withColumn(
    "attachment_to_bridge_link_priority", F.lit(3)
)
joined = linked.unionByName(unlinked, allowMissingColumns=True)
context_text = F.upper(
    F.concat_ws(
        " ",
        F.coalesce(F.col("item_description"), F.lit("")),
        F.coalesce(F.col("source_row_text"), F.lit("")),
        F.coalesce(F.col("transaction_description"), F.lit("")),
        F.coalesce(F.col("prime_award_base_transaction_description"), F.lit("")),
    )
)
scope_text = F.regexp_replace(
    context_text,
    r"(?:DATA )?PRODUCT SERVICE CODE\s*:?[ ]*[A-Z0-9-]*",
    "",
)
package_or_service = scope_text.rlike(
    r"IDIQ|INDEFINITE DELIVERY|CEILING|NOT TO EXCEED|FIRST ARTICLE|TEST(?:ING)?|PACKAGE|"
    r"\bSERVICES?\b|SUPPORT|MAINTENANCE|REPAIR|OVERHAUL|TRAINING|LEASE|CONSTRUCTION|"
    r"SOFTWARE|ENGINEERING|RESEARCH|DEVELOPMENT|PROGRAM MANAGEMENT|DATA REQUIREMENT"
)
physical_unit = F.upper(F.trim(F.coalesce(F.col("quantity_unit"), F.lit("")))).isin(
    "EA", "EACH", "UNIT", "UNITS", "SYSTEM", "SYSTEMS", "SET", "SETS", "KIT", "KITS"
)
exact_positive_action = (
    (F.col("primary_amount_basis") == "FEDERAL_ACTION_OBLIGATION")
    & (F.col("primary_amount_usd") > 0)
)
single_scope = (
    (F.col("quantity") > 0)
    & physical_unit
    & (F.col("quantity_scope_status") == "PROCUREMENT_LINE_QUANTITY_CANDIDATE")
    & (F.col("notice_procurement_quantity_line_count") == 1)
    & (F.col("positive_obligation_action_count") == 1)
    & (F.col("solicitation_award_count") == 1)
    & (~package_or_service)
)

award_amount_per_quantity_diagnostic = F.when(
    (F.col("quantity") > 0) & exact_positive_action,
    (F.col("primary_amount_usd") / F.col("quantity")).cast("decimal(30,6)"),
)
scoped_award_derived_price = F.when(single_scope, award_amount_per_quantity_diagnostic)
candidate_price = F.coalesce(
    F.col("source_unit_price_usd"),
    F.col("attachment_derived_unit_price_usd"),
    scoped_award_derived_price,
)

records = (
    joined
    # Keep broad arithmetic as a diagnostic, but expose it as a candidate price
    # only after the single-line / single-award / single-action scope gate.  This
    # prevents a line x award-action join from being reported as millions of
    # independent prices.
    .withColumn("award_derived_unit_price_usd", award_amount_per_quantity_diagnostic)
    .withColumn("candidate_unit_price_usd", candidate_price)
    .withColumn(
        "pricing_evidence_type",
        F.when(F.col("source_unit_price_usd").isNotNull(), F.lit("ATTACHMENT_SOURCE_UNIT_PRICE"))
        .when(
            F.col("attachment_derived_unit_price_usd").isNotNull(),
            F.lit("ATTACHMENT_SAME_ROW_DERIVED_UNIT_PRICE"),
        )
        .when(
            scoped_award_derived_price.isNotNull(),
            F.lit("REVIEW_CANDIDATE_USASPENDING_ACTION_AMOUNT_PER_SOLICITATION_QUANTITY"),
        )
        .when(
            award_amount_per_quantity_diagnostic.isNotNull(),
            F.lit("UNALLOCATED_ACTION_AMOUNT_QUANTITY_CONTEXT"),
        )
        .when(F.col("quantity").isNotNull(), F.lit("SOLICITATION_QUANTITY_WITHOUT_LINKED_ACTION_AMOUNT"))
        .otherwise(F.lit("UNALLOCATED_ATTACHMENT_AMOUNT_CONTEXT")),
    )
    .withColumn(
        "scope_validation_status",
        F.when(F.col("sam_usaspending_award_bridge_id").isNull(), F.lit("NO_EXACT_USASPENDING_AWARD_LINK"))
        .when(F.col("quantity_scope_status") != "PROCUREMENT_LINE_QUANTITY_CANDIDATE", F.col("quantity_scope_status"))
        .when(~physical_unit, F.lit("QUANTITY_UNIT_REQUIRES_REVIEW"))
        .when(package_or_service, F.lit("PACKAGE_SERVICE_OR_IDIQ_SCOPE_REQUIRES_REVIEW"))
        .when(F.col("notice_procurement_quantity_line_count") != 1, F.lit("MULTIPLE_SOLICITATION_QUANTITY_LINES_REQUIRE_ALLOCATION"))
        .when(F.col("solicitation_award_count") != 1, F.lit("MULTIPLE_AWARDS_REQUIRE_ALLOCATION"))
        .when(F.col("positive_obligation_action_count") != 1, F.lit("MULTIPLE_OR_NO_POSITIVE_ACTIONS_REQUIRE_RECONSTRUCTION"))
        .when(~exact_positive_action, F.lit("LINKED_AMOUNT_IS_NOT_POSITIVE_ACTION_OBLIGATION"))
        .when(single_scope, F.lit("SINGLE_LINE_SINGLE_AWARD_ACTION_CANDIDATE_REQUIRES_REVIEW"))
        .otherwise(F.lit("AMOUNT_QUANTITY_SCOPE_REQUIRES_REVIEW")),
    )
    .withColumn(
        "promotion_eligible_after_review",
        single_scope
        & candidate_price.isNotNull()
        & candidate_price.between(F.lit(1), F.lit(1_000_000_000)),
    )
    .withColumn("include_in_mimir_range", F.lit(False))
    .withColumn(
        "pricing_evidence_id",
        F.sha2(
            F.concat_ws(
                "\u001f",
                F.lit("SAM_AWARD_PRICING_EVIDENCE_V1"),
                F.col("line_candidate_id"),
                F.coalesce(F.col("sam_usaspending_award_bridge_id"), F.lit("NO_BRIDGE")),
            ),
            256,
        ),
    )
    .withColumn("transform_version", F.lit(TRANSFORM_VERSION))
    .withColumn("pricing_transform_run_id", F.lit(RUN_ID))
    .withColumn("created_at", F.lit(STARTED_AT))
    .select(
        "pricing_evidence_id",
        "sam_usaspending_award_bridge_id",
        "line_candidate_id",
        "notice_id",
        F.col("notice_version_id").alias("attachment_notice_version_id"),
        "bridge_notice_id",
        "bridge_notice_version_id",
        F.col("solicitation_id").alias("sam_solicitation_id"),
        F.col("award_number").alias("sam_award_number"),
        "bridge_notice_source_system",
        "line_normalized_solicitation_id",
        "bridge_normalized_solicitation_id",
        "bridge_normalized_award_number",
        "attachment_to_bridge_link_method",
        "link_method",
        "link_confidence",
        "contract_transaction_unique_key",
        "contract_award_unique_key",
        "amount_evidence_id",
        "award_id_piid",
        "parent_award_id_piid",
        "solicitation_identifier",
        "modification_number",
        "action_date",
        "action_class",
        "primary_amount_usd",
        "primary_amount_basis",
        "signed_action_obligation_usd",
        "current_total_value_of_award_usd",
        "base_and_exercised_options_value_usd",
        "potential_total_value_of_award_usd",
        "clin",
        F.col("item_description").alias("priced_item_name"),
        "quantity",
        "quantity_unit",
        "source_unit_price_usd",
        "extended_line_amount_usd",
        "attachment_derived_unit_price_usd",
        "award_derived_unit_price_usd",
        "candidate_unit_price_usd",
        "pricing_evidence_type",
        F.col("unit_price_method").alias("attachment_unit_price_method"),
        F.col("arithmetic_validation_status").alias("attachment_arithmetic_validation_status"),
        F.col("evidence_type").alias("attachment_evidence_type"),
        F.col("document_role").alias("attachment_document_role"),
        F.col("source_value_basis").alias("attachment_source_value_basis"),
        "quantity_scope_status",
        "scope_validation_status",
        "notice_procurement_quantity_line_count",
        "notice_line_candidate_count",
        "award_action_count",
        "positive_obligation_action_count",
        "solicitation_award_count",
        "solicitation_notice_count",
        "promotion_eligible_after_review",
        "include_in_mimir_range",
        "transaction_description",
        "prime_award_base_transaction_description",
        "product_or_service_code",
        "product_or_service_code_description",
        "awarding_agency_name",
        "awarding_sub_agency_name",
        "awarding_office_name",
        "recipient_name",
        "recipient_uei",
        "cage_code",
        "usaspending_permalink",
        "usaspending_source_s3_uri",
        "usaspending_source_s3_version_id",
        "usaspending_source_etag",
        "usaspending_source_sha256",
        "usaspending_source_locator",
        "attachment_id",
        "resource_id",
        "attachment_name",
        "attachment_sha256",
        "attachment_s3_uri",
        F.col("source_locator").alias("attachment_source_locator"),
        F.col("source_row_text").alias("attachment_source_row_text"),
        F.col("source_row_sha256").alias("attachment_source_row_sha256"),
        F.col("parser_version").alias("attachment_parser_version"),
        F.col("transform_run_id").alias("attachment_transform_run_id"),
        "transform_version",
        "pricing_transform_run_id",
        "created_at",
    )
)

duplicate_count = records.groupBy("pricing_evidence_id").count().where("count > 1").count()
missing_attachment_provenance_count = records.where(
    "attachment_s3_uri IS NULL OR attachment_sha256 IS NULL "
    "OR attachment_source_locator IS NULL OR attachment_source_row_sha256 IS NULL"
).count()
unsafe_promotion_count = records.where(
    "include_in_mimir_range OR (promotion_eligible_after_review AND candidate_unit_price_usd IS NULL)"
).count()
if duplicate_count or missing_attachment_provenance_count or unsafe_promotion_count:
    raise RuntimeError(
        "SAM award pricing evidence validation failed: "
        f"duplicates={duplicate_count:,}, missing_attachment_provenance={missing_attachment_provenance_count:,}, "
        f"unsafe_promotion={unsafe_promotion_count:,}"
    )

records.write.mode("overwrite").parquet(STAGING)
replay = spark.read.parquet(STAGING)
replay.writeTo(TARGET).using("iceberg").tableProperty(
    "format-version", "2"
).tableProperty("write.parquet.compression-codec", "zstd").partitionedBy(
    F.years("action_date"), "pricing_evidence_type"
).createOrReplace()

counts = replay.agg(
    F.count(F.lit(1)).alias("pricing_evidence_row_count"),
    F.countDistinct("line_candidate_id").alias("line_candidate_count"),
    F.countDistinct("notice_id").alias("notice_count"),
    F.countDistinct("contract_award_unique_key").alias("linked_award_count"),
    F.sum(F.when(F.col("quantity").isNotNull(), 1).otherwise(0)).alias("rows_with_quantity"),
    F.sum(F.when(F.col("candidate_unit_price_usd").isNotNull(), 1).otherwise(0)).alias("rows_with_candidate_price"),
    F.sum(F.when(F.col("promotion_eligible_after_review"), 1).otherwise(0)).alias("review_eligible_rows"),
).first().asDict()
manifest = {
    "run_id": RUN_ID,
    "generated_at": STARTED_AT.isoformat(),
    "transform_version": TRANSFORM_VERSION,
    "target": TARGET,
    "bridge": BRIDGE,
    "source": SOURCE,
    "staging_replay_uri": STAGING,
    **counts,
    "duplicate_pricing_evidence_id_count": duplicate_count,
    "missing_attachment_provenance_count": missing_attachment_provenance_count,
    "unsafe_promotion_count": unsafe_promotion_count,
    "policy": (
        "Quantity and amount evidence are retained broadly. Unit prices derived from USAspending "
        "actions remain review candidates; no row enters a Mimir range automatically."
    ),
}
boto3.client("s3", region_name="us-east-1").put_object(
    Bucket=BUCKET,
    Key=MANIFEST_KEY,
    Body=(json.dumps(manifest, indent=2, default=str) + "\n").encode("utf-8"),
    ContentType="application/json",
)
print(json.dumps(manifest, indent=2, default=str))
job.commit()
