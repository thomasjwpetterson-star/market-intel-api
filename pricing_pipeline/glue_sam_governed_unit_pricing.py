"""Materialize governed SAM attachment unit prices as a compact Iceberg fact.

Grain: one physical attachment source line.  USAspending award/action data is
context only and is retained only when the source line resolves to one unique
action or award.  Award totals are never used to derive a price in this job.
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


def non_procurement_document_class(attachment_sha256, attachment_name, source_row_text=""):
    """Document meaning takes precedence over valid numeric row arithmetic.

    The exact reviewed hash remains excluded if renamed. Filename matches flag
    similar property documents for review, without claiming a verified value
    basis for a different source. A property clause in an executed line alone
    is not enough to reject a genuine purchase.
    """
    import re

    if attachment_sha256 == (
        "84075394258b3f88d553c39b5950b58e045113c016aaaf4f270b5005f268a967"
    ):
        return "GOVERNMENT_FURNISHED_PROPERTY_INVENTORY"
    name = re.sub(r"[^A-Z0-9]+", " ", str(attachment_name or "").upper()).strip()
    row = re.sub(r"[^A-Z0-9]+", " ", str(source_row_text or "").upper()).strip()
    if re.search(r"\bGOVERNMENT FURNISHED PROPERTY\b|\bGFP\b", name):
        return "NON_PROCUREMENT_DOCUMENT_REQUIRES_REVIEW"
    if re.search(r"\b(INVENTORY|ASSET REGISTER|PROPERTY REGISTER)\b", name) and re.search(
        r"\b(IN STOCK|IN REPAIR|UNIT COST|EXTENDED COST|SERIAL NBR|BATCH VALUATION)\b", row
    ):
        return "NON_PROCUREMENT_DOCUMENT_REQUIRES_REVIEW"
    return None


TRANSFORM_VERSION = "mimir-sam-governed-unit-pricing-2026-11-v13-document-role"
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

SOURCE = "glue_catalog.market_intel_silver.fact_sam_award_pricing_evidence"
NOTICE_VERSION_SOURCE = (
    "glue_catalog.market_intel_silver.ref_sam_archived_opportunity_notice_versions"
)
TARGET = "glue_catalog.market_intel_silver.fact_sam_governed_unit_pricing"
STAGING = f"s3://{BUCKET}/silver/pricing/sam_governed_unit_pricing_staging/run_id={RUN_ID}/"
MANIFEST_KEY = f"silver/pricing/sam_governed_unit_pricing_manifests/run_id={RUN_ID}/manifest.json"

physical_units = [
    "EA", "EACH", "UNIT", "UNITS", "SYSTEM", "SYSTEMS",
    "SET", "SETS", "KIT", "KITS", "ASSEMBLY", "ASSEMBLIES",
]

evidence = spark.table(SOURCE)
award_notice_versions = (
    spark.table(NOTICE_VERSION_SOURCE)
    .where(F.col("notice_type") == "Award Notice")
    .select(F.col("sam_notice_version_id").alias("attachment_notice_version_id"))
    .distinct()
)
notice_versions = spark.table(NOTICE_VERSION_SOURCE).select(
    F.col("sam_notice_version_id").alias("attachment_notice_version_id"),
    F.col("posted_at").alias("sam_notice_posted_at"),
    F.to_date("posted_at").alias("sam_notice_posted_date"),
    F.col("source_date_quality_status").alias("sam_notice_date_quality_status"),
    F.col("record_payload_sha256").alias("sam_notice_record_payload_sha256"),
    F.col("source_occurrences").alias("sam_notice_source_occurrences"),
    F.col("source_occurrence_count").alias("sam_notice_source_occurrence_count"),
    F.col("first_ingested_at").alias("sam_notice_first_ingested_at"),
    F.col("last_ingested_at").alias("sam_notice_last_ingested_at"),
)

direct_rows = (
    evidence
    .where(F.col("line_candidate_id").isNotNull())
    .where(F.col("priced_item_name").isNotNull() & (F.trim("priced_item_name") != ""))
    .where(F.col("quantity") > 0)
    .where(F.col("quantity_scope_status") == "PROCUREMENT_LINE_QUANTITY_CANDIDATE")
    .where(F.upper(F.trim("quantity_unit")).isin(*physical_units))
    .where(
        F.col("attachment_s3_uri").isNotNull()
        & F.col("attachment_sha256").isNotNull()
        & F.col("attachment_source_locator").isNotNull()
        & F.col("attachment_source_row_sha256").isNotNull()
    )
    .where(
        (
            (F.col("source_unit_price_usd") > 0)
            & (F.col("attachment_arithmetic_validation_status") == "VALIDATED_WITHIN_TOLERANCE")
        )
        | (
            (F.col("attachment_derived_unit_price_usd") > 0)
            & (F.col("extended_line_amount_usd") > 0)
            & (F.col("attachment_arithmetic_validation_status") == "EXACT_ARITHMETIC_DERIVATION")
        )
    )
)

# Repair one narrow parser-failure cohort from the immutable source-row text.
# The row must begin with one CLIN, one item, quantity/unit, unit price and line
# amount, and the reparsed values must reconcile arithmetically. This does not
# use a notice, award or action amount.
standard_row_pattern = (
    r"^\s*([A-Z0-9-]{1,20})\s+(.+?)\s+"
    r"([0-9][0-9,]*(?:\.[0-9]+)?)\s+"
    r"(EA|EACH|UNIT|UNITS|SYSTEM|SYSTEMS|SET|SETS|KIT|KITS|ASSEMBLY|ASSEMBLIES)\s+"
    r"\$\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s+"
    r"\$\s*([0-9][0-9,]*(?:\.[0-9]+)?)"
)
repair_rows = (
    evidence
    .where(F.col("line_candidate_id").isNotNull())
    .where(F.col("attachment_arithmetic_validation_status") == "SOURCE_VALUES_DO_NOT_RECONCILE")
    .where(F.col("attachment_source_row_text").isNotNull())
    .where(
        F.col("attachment_s3_uri").isNotNull()
        & F.col("attachment_sha256").isNotNull()
        & F.col("attachment_source_locator").isNotNull()
        & F.col("attachment_source_row_sha256").isNotNull()
    )
    .withColumn("repair_clin", F.regexp_extract("attachment_source_row_text", standard_row_pattern, 1))
    .withColumn("repair_item_name", F.trim(F.regexp_extract("attachment_source_row_text", standard_row_pattern, 2)))
    .withColumn(
        "repair_quantity",
        F.regexp_replace(
            F.regexp_extract("attachment_source_row_text", standard_row_pattern, 3), ",", ""
        ).cast("decimal(20,6)"),
    )
    .withColumn("repair_quantity_unit", F.upper(F.regexp_extract("attachment_source_row_text", standard_row_pattern, 4)))
    .withColumn(
        "repair_unit_price_usd",
        F.regexp_replace(
            F.regexp_extract("attachment_source_row_text", standard_row_pattern, 5), ",", ""
        ).cast("decimal(30,6)"),
    )
    .withColumn(
        "repair_line_amount_usd",
        F.regexp_replace(
            F.regexp_extract("attachment_source_row_text", standard_row_pattern, 6), ",", ""
        ).cast("decimal(30,6)"),
    )
    .where(
        (F.col("repair_clin") != "")
        & (F.col("repair_item_name") != "")
        & (F.col("repair_quantity") > 0)
        & (F.col("repair_unit_price_usd") > 0)
        & (F.col("repair_line_amount_usd") > 0)
    )
    .where(
        F.abs(
            F.col("repair_quantity") * F.col("repair_unit_price_usd")
            - F.col("repair_line_amount_usd")
        ) <= F.greatest(
            F.lit(0.02), F.abs(F.col("repair_line_amount_usd")) * F.lit(0.0001)
        )
    )
    .withColumn("clin", F.col("repair_clin"))
    .withColumn("priced_item_name", F.col("repair_item_name"))
    .withColumn("quantity", F.col("repair_quantity"))
    .withColumn("quantity_unit", F.col("repair_quantity_unit"))
    .withColumn("source_unit_price_usd", F.col("repair_unit_price_usd"))
    .withColumn("extended_line_amount_usd", F.col("repair_line_amount_usd"))
    .withColumn("attachment_derived_unit_price_usd", F.col("repair_unit_price_usd"))
    .withColumn("attachment_arithmetic_validation_status", F.lit("EXACT_SAME_ROW_REGEX_REPAIR"))
    .drop(
        "repair_clin", "repair_item_name", "repair_quantity", "repair_quantity_unit",
        "repair_unit_price_usd", "repair_line_amount_usd",
    )
)

# Some federal purchase-order tables put quantity and price columns before the
# item description. Admit that layout only when its first five columns reconcile
# and the item is bounded by a known metadata heading or end-of-row.
quantity_first_pattern = (
    r"^\s*([A-Z0-9-]{1,20})\s+"
    r"([0-9][0-9,]*(?:\.[0-9]+)?)\s+"
    r"(EA|EACH|UNIT|UNITS|SYSTEM|SYSTEMS|SET|SETS|KIT|KITS|ASSEMBLY|ASSEMBLIES)\s+"
    r"\$\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s+"
    r"\$\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s+(.+)$"
)
quantity_first_item_pattern = (
    r"^(.+?)(?:\s+(?:PRINCIPAL NAICS CODE|PRODUCT/SERVICE CODE|LOCAL STOCK NUMBER|"
    r"FUNDING/REQ\.? NUMBER|CONTRACT PERIOD).*)?$"
)
quantity_first_repairs = (
    evidence
    .where(F.col("line_candidate_id").isNotNull())
    .where(F.col("attachment_arithmetic_validation_status") == "SOURCE_VALUES_DO_NOT_RECONCILE")
    .where(F.col("attachment_source_row_text").isNotNull())
    .where(
        F.col("attachment_s3_uri").isNotNull()
        & F.col("attachment_sha256").isNotNull()
        & F.col("attachment_source_locator").isNotNull()
        & F.col("attachment_source_row_sha256").isNotNull()
    )
    .withColumn("repair_clin", F.regexp_extract("attachment_source_row_text", quantity_first_pattern, 1))
    .withColumn(
        "repair_quantity",
        F.regexp_replace(
            F.regexp_extract("attachment_source_row_text", quantity_first_pattern, 2), ",", ""
        ).cast("decimal(20,6)"),
    )
    .withColumn("repair_quantity_unit", F.upper(F.regexp_extract("attachment_source_row_text", quantity_first_pattern, 3)))
    .withColumn(
        "repair_unit_price_usd",
        F.regexp_replace(
            F.regexp_extract("attachment_source_row_text", quantity_first_pattern, 4), ",", ""
        ).cast("decimal(30,6)"),
    )
    .withColumn(
        "repair_line_amount_usd",
        F.regexp_replace(
            F.regexp_extract("attachment_source_row_text", quantity_first_pattern, 5), ",", ""
        ).cast("decimal(30,6)"),
    )
    .withColumn(
        "repair_item_name",
        F.trim(F.regexp_extract(
            F.regexp_extract("attachment_source_row_text", quantity_first_pattern, 6),
            quantity_first_item_pattern,
            1,
        )),
    )
    .where(
        (F.col("repair_clin") != "")
        & (F.col("repair_item_name") != "")
        & (F.col("repair_quantity") > 0)
        & (F.col("repair_unit_price_usd") > 0)
        & (F.col("repair_line_amount_usd") > 0)
    )
    .where(
        F.abs(
            F.col("repair_quantity") * F.col("repair_unit_price_usd")
            - F.col("repair_line_amount_usd")
        ) <= F.greatest(
            F.lit(0.02), F.abs(F.col("repair_line_amount_usd")) * F.lit(0.0001)
        )
    )
    .withColumn("clin", F.col("repair_clin"))
    .withColumn("priced_item_name", F.col("repair_item_name"))
    .withColumn("quantity", F.col("repair_quantity"))
    .withColumn("quantity_unit", F.col("repair_quantity_unit"))
    .withColumn("source_unit_price_usd", F.col("repair_unit_price_usd"))
    .withColumn("extended_line_amount_usd", F.col("repair_line_amount_usd"))
    .withColumn("attachment_derived_unit_price_usd", F.col("repair_unit_price_usd"))
    .withColumn("attachment_arithmetic_validation_status", F.lit("EXACT_SAME_ROW_REGEX_REPAIR"))
    .drop(
        "repair_clin", "repair_item_name", "repair_quantity", "repair_quantity_unit",
        "repair_unit_price_usd", "repair_line_amount_usd",
    )
)
repair_rows = repair_rows.unionByName(quantity_first_repairs)

# A large FMS equipment-stock format renders currency symbols after both the
# unit price and extended amount. The prior parser retained quantity but missed
# both values. Require both trailing-dollar values and exact row arithmetic.
trailing_dollar_pattern = (
    r"^(.+?)\s+([0-9][0-9,]*(?:\.[0-9]+)?)\s+"
    r"(EA|EACH|UNIT|UNITS|SYSTEM|SYSTEMS|SET|SETS|KIT|KITS|ASSEMBLY|ASSEMBLIES)\s+"
    r".*?([0-9][0-9,]*\.[0-9]{2})\$\s+([0-9][0-9,]*\.[0-9]{2})\$(?:\s+.*)?$"
)
trailing_dollar_repairs = (
    evidence
    .where(F.col("line_candidate_id").isNotNull())
    .where(F.col("attachment_evidence_type") == "QUANTITY_ONLY")
    .where(F.col("quantity_scope_status") == "PROCUREMENT_LINE_QUANTITY_CANDIDATE")
    .where(F.col("attachment_source_row_text").isNotNull())
    .where(
        F.col("attachment_s3_uri").isNotNull()
        & F.col("attachment_sha256").isNotNull()
        & F.col("attachment_source_locator").isNotNull()
        & F.col("attachment_source_row_sha256").isNotNull()
    )
    .withColumn("repair_item_name", F.trim(F.regexp_extract("attachment_source_row_text", trailing_dollar_pattern, 1)))
    .withColumn(
        "repair_quantity",
        F.regexp_replace(
            F.regexp_extract("attachment_source_row_text", trailing_dollar_pattern, 2), ",", ""
        ).cast("decimal(20,6)"),
    )
    .withColumn("repair_quantity_unit", F.upper(F.regexp_extract("attachment_source_row_text", trailing_dollar_pattern, 3)))
    .withColumn(
        "repair_unit_price_usd",
        F.regexp_replace(
            F.regexp_extract("attachment_source_row_text", trailing_dollar_pattern, 4), ",", ""
        ).cast("decimal(30,6)"),
    )
    .withColumn(
        "repair_line_amount_usd",
        F.regexp_replace(
            F.regexp_extract("attachment_source_row_text", trailing_dollar_pattern, 5), ",", ""
        ).cast("decimal(30,6)"),
    )
    .where(
        (F.col("repair_item_name") != "")
        & (F.col("repair_quantity") > 0)
        & (F.col("repair_unit_price_usd") > F.lit(0.01))
        & (F.col("repair_line_amount_usd") > 0)
    )
    .where(
        F.abs(
            F.col("repair_quantity") * F.col("repair_unit_price_usd")
            - F.col("repair_line_amount_usd")
        ) <= F.greatest(
            F.lit(0.02), F.abs(F.col("repair_line_amount_usd")) * F.lit(0.0001)
        )
    )
    .withColumn("priced_item_name", F.col("repair_item_name"))
    .withColumn("quantity", F.col("repair_quantity"))
    .withColumn("quantity_unit", F.col("repair_quantity_unit"))
    .withColumn("source_unit_price_usd", F.col("repair_unit_price_usd"))
    .withColumn("extended_line_amount_usd", F.col("repair_line_amount_usd"))
    .withColumn("attachment_derived_unit_price_usd", F.col("repair_unit_price_usd"))
    .withColumn("attachment_arithmetic_validation_status", F.lit("EXACT_TRAILING_DOLLAR_SAME_ROW_REPAIR"))
    .drop(
        "repair_item_name", "repair_quantity", "repair_quantity_unit",
        "repair_unit_price_usd", "repair_line_amount_usd",
    )
)
repair_rows = repair_rows.unionByName(trailing_dollar_repairs)

# One reviewed equipment-list attachment has a stable four-column layout:
# source item code, equipment description, quantity, and extended amount. The
# full 21-row cohort was audited: every row has quantity 1 and one same-row
# amount; software rows remain excluded by the downstream item classifier.
equipment_list_attachment_sha256 = (
    "6f2e0fc4bb73993ba7c0f0cd226a38bd85bc9dca33a67c55efe27de1999b14bf"
)
equipment_list_pattern = (
    r"^\s*([0-9]{6})\s+(.+?)\s+"
    r"([0-9][0-9,]*(?:\.[0-9]+)?)\s+"
    r"\$\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*$"
)
equipment_list_repairs = (
    evidence
    .where(F.col("line_candidate_id").isNotNull())
    .where(F.col("attachment_evidence_type") == "LINE_AMOUNT_ONLY")
    .where(F.col("attachment_sha256") == equipment_list_attachment_sha256)
    .where(F.col("attachment_source_row_text").isNotNull())
    .where(
        F.col("attachment_s3_uri").isNotNull()
        & F.col("attachment_source_locator").isNotNull()
        & F.col("attachment_source_row_sha256").isNotNull()
    )
    .withColumn("repair_item_code", F.regexp_extract("attachment_source_row_text", equipment_list_pattern, 1))
    .withColumn("repair_item_name", F.trim(F.regexp_extract("attachment_source_row_text", equipment_list_pattern, 2)))
    .withColumn(
        "repair_quantity",
        F.regexp_replace(
            F.regexp_extract("attachment_source_row_text", equipment_list_pattern, 3), ",", ""
        ).cast("decimal(20,6)"),
    )
    .withColumn(
        "repair_line_amount_usd",
        F.regexp_replace(
            F.regexp_extract("attachment_source_row_text", equipment_list_pattern, 4), ",", ""
        ).cast("decimal(30,6)"),
    )
    .where(
        (F.col("repair_item_code") != "")
        & (F.col("repair_item_name") != "")
        & (F.col("repair_quantity") > 0)
        & (F.col("repair_line_amount_usd") > F.lit(0.01))
    )
    .withColumn("priced_item_name", F.col("repair_item_name"))
    .withColumn("quantity", F.col("repair_quantity"))
    .withColumn("quantity_unit", F.lit("UNIT"))
    .withColumn("source_unit_price_usd", F.lit(None).cast("decimal(30,6)"))
    .withColumn("extended_line_amount_usd", F.col("repair_line_amount_usd"))
    .withColumn(
        "attachment_derived_unit_price_usd",
        (F.col("repair_line_amount_usd") / F.col("repair_quantity")).cast("decimal(30,6)"),
    )
    .withColumn(
        "attachment_arithmetic_validation_status",
        F.lit("EXACT_EQUIPMENT_LIST_AMOUNT_QUANTITY_REPAIR"),
    )
    .drop("repair_item_code", "repair_item_name", "repair_quantity", "repair_line_amount_usd")
)
repair_rows = repair_rows.unionByName(equipment_list_repairs)

# Executed award tables commonly contain one complete CLIN row with quantity,
# a source-stated USD unit price, and matching firm/funded extended amounts.
# The general parser preserved the quantity but missed the "USD 123.45" price
# syntax. Admit only exact Award Notice table rows whose embedded numeric PSC,
# repeated line totals, and quantity arithmetic all reconcile.
award_table_row_pattern = (
    r"(?i)^\s*([A-Z0-9-]{4,20})\s+(.+?)\s+"
    r"([0-9][0-9,]*(?:\.[0-9]+)?)\s+"
    r"(EA|EACH|UNIT|UNITS|SYSTEM|SYSTEMS|SET|SETS|KIT|KITS|ASSEMBLY|ASSEMBLIES)\s+"
    r"USD\s+([0-9][0-9,]*\.[0-9]{2}).*$"
)
award_table_item_pattern = (
    r"(?i)^(.+?)(?:\s+(?:PART NUMBER DESCRIPTION|PRODUCT SERVICE CODE|"
    r"PRICING ARRANGEMENT|ACRN|PR NUMBER|PR LINE ITEM NUMBER|CIN|"
    r"CLIN ONLY - REQUISITION DOCUMENT NUMBER):|$)"
)
award_table_firm_price_pattern = (
    r"(?i)FIRM PRICE\s+USD\s+([0-9][0-9,]*\.[0-9]{2})"
)
award_table_funded_amount_pattern = (
    r"(?i)FUNDED AMOUNT\s+USD\s+([0-9][0-9,]*\.[0-9]{2})"
)
award_table_psc_pattern = r"(?i)PRODUCT SERVICE CODE:\s*([A-Z0-9]{4})"
award_table_repairs = (
    evidence
    .join(award_notice_versions, "attachment_notice_version_id", "left_semi")
    .where(F.col("line_candidate_id").isNotNull())
    .where(F.col("attachment_evidence_type") == "QUANTITY_ONLY")
    .where(F.col("quantity_scope_status") == "PROCUREMENT_LINE_QUANTITY_CANDIDATE")
    .where(F.col("attachment_source_row_text").isNotNull())
    .where(F.col("attachment_source_locator").contains(";table="))
    .where(
        F.col("attachment_s3_uri").isNotNull()
        & F.col("attachment_sha256").isNotNull()
        & F.col("attachment_source_locator").isNotNull()
        & F.col("attachment_source_row_sha256").isNotNull()
    )
    .withColumn("repair_clin", F.regexp_extract("attachment_source_row_text", award_table_row_pattern, 1))
    .withColumn(
        "repair_item_segment",
        F.trim(F.regexp_extract("attachment_source_row_text", award_table_row_pattern, 2)),
    )
    .withColumn(
        "repair_item_name",
        F.trim(F.regexp_extract("repair_item_segment", award_table_item_pattern, 1)),
    )
    .withColumn(
        "repair_quantity",
        F.regexp_replace(
            F.regexp_extract("attachment_source_row_text", award_table_row_pattern, 3), ",", ""
        ).cast("decimal(20,6)"),
    )
    .withColumn(
        "repair_quantity_unit",
        F.upper(F.regexp_extract("attachment_source_row_text", award_table_row_pattern, 4)),
    )
    .withColumn(
        "repair_unit_price_usd",
        F.regexp_replace(
            F.regexp_extract("attachment_source_row_text", award_table_row_pattern, 5), ",", ""
        ).cast("decimal(30,6)"),
    )
    .withColumn(
        "repair_firm_price_usd",
        F.regexp_replace(
            F.regexp_extract(
                "attachment_source_row_text", award_table_firm_price_pattern, 1
            ),
            ",",
            "",
        ).cast("decimal(30,6)"),
    )
    .withColumn(
        "repair_funded_amount_usd",
        F.regexp_replace(
            F.regexp_extract(
                "attachment_source_row_text", award_table_funded_amount_pattern, 1
            ),
            ",",
            "",
        ).cast("decimal(30,6)"),
    )
    .withColumn(
        "repair_source_psc",
        F.upper(F.regexp_extract("attachment_source_row_text", award_table_psc_pattern, 1)),
    )
    .where(
        (F.col("repair_clin") != "")
        & (F.col("repair_item_name") != "")
        & (F.col("repair_quantity") > 0)
        & (F.col("repair_unit_price_usd") > F.lit(0.01))
        & (F.col("repair_firm_price_usd") > 0)
        & (F.col("repair_funded_amount_usd") > 0)
        & F.col("repair_source_psc").rlike(r"^[0-9]{4}$")
    )
    .where(
        F.abs(F.col("repair_firm_price_usd") - F.col("repair_funded_amount_usd"))
        <= F.greatest(
            F.lit(0.02), F.abs(F.col("repair_funded_amount_usd")) * F.lit(0.0001)
        )
    )
    .where(
        F.abs(
            F.col("repair_quantity") * F.col("repair_unit_price_usd")
            - F.col("repair_funded_amount_usd")
        ) <= F.greatest(
            F.lit(0.02), F.abs(F.col("repair_funded_amount_usd")) * F.lit(0.0001)
        )
    )
    .withColumn("clin", F.col("repair_clin"))
    .withColumn("priced_item_name", F.col("repair_item_name"))
    .withColumn("quantity", F.col("repair_quantity"))
    .withColumn("quantity_unit", F.col("repair_quantity_unit"))
    .withColumn("source_unit_price_usd", F.col("repair_unit_price_usd"))
    .withColumn("extended_line_amount_usd", F.col("repair_funded_amount_usd"))
    .withColumn("attachment_derived_unit_price_usd", F.col("repair_unit_price_usd"))
    .withColumn("product_or_service_code", F.col("repair_source_psc"))
    .withColumn(
        "attachment_arithmetic_validation_status",
        F.lit("EXACT_AWARD_TABLE_USD_UNIT_PRICE_REPAIR"),
    )
    .drop(
        "repair_clin",
        "repair_item_segment",
        "repair_item_name",
        "repair_quantity",
        "repair_quantity_unit",
        "repair_unit_price_usd",
        "repair_firm_price_usd",
        "repair_funded_amount_usd",
        "repair_source_psc",
    )
)
repair_rows = repair_rows.unionByName(award_table_repairs)

source_rows = (
    direct_rows.unionByName(repair_rows)
    # The v6 PDF text parser already governed the Oregon 5744 cart on this
    # award. The v7 five-cell table recovery sees the same physical PDF row
    # under a second locator. Retain the original observation, not both.
    .where(~(
        (F.col("attachment_sha256") == "5f35757152d12af6734cd318c35ece9cb211bdf40017e8b20ee55f0dbb8a8891")
        & (F.col("attachment_source_locator") == "page=8;table=1#row=6")
    ))
    .withColumn(
        "classification_text",
        F.upper(F.concat_ws(
            " ",
            F.coalesce("priced_item_name", F.lit("")),
            F.when(
                F.col("attachment_arithmetic_validation_status")
                == "EXACT_AWARD_TABLE_USD_UNIT_PRICE_REPAIR",
                F.regexp_replace(
                    F.coalesce("attachment_source_row_text", F.lit("")),
                    r"(?i)PRODUCT SERVICE CODE:",
                    "PSC:",
                ),
            ).otherwise(F.coalesce("attachment_source_row_text", F.lit(""))),
            F.coalesce("attachment_name", F.lit("")),
            F.coalesce("attachment_source_locator", F.lit("")),
        )),
    )
    .withColumn(
        "physical_source_line_key",
        F.concat_ws(
            "|", "attachment_sha256", "attachment_source_locator",
            "attachment_source_row_sha256",
        ),
    )
)

context_counts = source_rows.groupBy("physical_source_line_key").agg(
    F.countDistinct("contract_transaction_unique_key").alias("linked_action_context_count"),
    F.countDistinct("contract_award_unique_key").alias("linked_award_context_count"),
)

source_rank = Window.partitionBy("physical_source_line_key").orderBy(
    F.when(F.col("contract_transaction_unique_key").isNotNull(), F.lit(0)).otherwise(F.lit(1)),
    F.col("action_date").desc_nulls_last(),
    F.col("pricing_evidence_id"),
)

classified = (
    source_rows.join(context_counts, "physical_source_line_key", "inner")
    .withColumn("source_line_rank", F.row_number().over(source_rank))
    .where(F.col("source_line_rank") == 1)
    .withColumn(
        "unit_price_usd",
        F.when(
            (F.col("source_unit_price_usd") > 0)
            & F.col("attachment_arithmetic_validation_status").isin(
                "VALIDATED_WITHIN_TOLERANCE", "EXACT_SAME_ROW_REGEX_REPAIR",
                "EXACT_TRAILING_DOLLAR_SAME_ROW_REPAIR",
                "EXACT_AWARD_TABLE_USD_UNIT_PRICE_REPAIR",
            ),
            F.col("source_unit_price_usd"),
        ).otherwise(F.col("attachment_derived_unit_price_usd")).cast("decimal(30,6)"),
    )
    .withColumn(
        "price_basis",
        F.when(
            F.col("attachment_arithmetic_validation_status") == "EXACT_TRAILING_DOLLAR_SAME_ROW_REPAIR",
            F.lit("SAM_ATTACHMENT_TRAILING_DOLLAR_ROW_REPAIRED_UNIT_PRICE"),
        ).when(
            F.col("attachment_arithmetic_validation_status") == "EXACT_AWARD_TABLE_USD_UNIT_PRICE_REPAIR",
            F.lit("SAM_ATTACHMENT_AWARD_TABLE_SOURCE_STATED_UNIT_PRICE"),
        ).when(
            F.col("attachment_arithmetic_validation_status") == "EXACT_EQUIPMENT_LIST_AMOUNT_QUANTITY_REPAIR",
            F.lit("SAM_ATTACHMENT_EQUIPMENT_LIST_AMOUNT_DIVIDED_BY_QUANTITY"),
        ).when(
            F.col("attachment_arithmetic_validation_status") == "EXACT_SAME_ROW_REGEX_REPAIR",
            F.lit("SAM_ATTACHMENT_SOURCE_ROW_REGEX_REPAIRED_UNIT_PRICE"),
        ).when(
            (F.col("source_unit_price_usd") > 0)
            & (F.col("attachment_arithmetic_validation_status") == "VALIDATED_WITHIN_TOLERANCE"),
            F.lit("SAM_ATTACHMENT_SOURCE_STATED_UNIT_PRICE"),
        ).otherwise(F.lit("SAM_ATTACHMENT_SAME_ROW_AMOUNT_DIVIDED_BY_QUANTITY")),
    )
)

# This check precedes item/PSC classification. Provenance and multiplication
# alone do not make a property valuation a procurement price.
document_class = F.udf(non_procurement_document_class, "string")
classified = classified.withColumn(
    "excluded_document_class",
    document_class("attachment_sha256", "attachment_name", "attachment_source_row_text"),
)
text = F.col("classification_text")
template_pattern = (
    r"(^|[^A-Z])(TEMPLATE|SAMPLE FORM|BLANK FORM|INSTRUCTIONS? TO (OFFERORS?|BIDDERS?)|"
    r"WAGE DETERMINATION|WAGE SCHEDULE|SERVICE CONTRACT ACT|DAVIS[- ]BACON|"
    r"LABOR CATEGOR(Y|IES)|HOURLY RATE)([^A-Z]|$)"
)
service_pattern = (
    r"(^|[^A-Z])(SERVICES?|WARRANT(Y|IES)|MAINTENANCE|REPAIRS?|OVERHAUL|TRAINING|"
    r"TECHNICAL SUPPORT|PROGRAM MANAGEMENT|ENGINEERING SERVICES?|RESEARCH AND DEVELOPMENT|"
    r"FIRST ARTICLE|INSTALLATION|FREIGHT|SHIPPING|DELIVERY CHARGE|DATA REQUIREMENT|"
    r"REPORTS?|MANUALS?|MEALS?|FOOD SERVICE|NOT TO EXCEED|NTE TOTAL)([^A-Z]|$)"
)
software_pattern = (
    r"(^|[^A-Z])(SOFTWARE|LICENSE|LICENCE|SUBSCRIPTION|TOOLBOX|MICROSOFT OFFICE)([^A-Z]|$)"
)
option_pattern = r"(^|[^A-Z])(UNFUNDED|UNEXERCISED|OPTION|OPTIONAL)([^A-Z]|$)"
non_equipment_pattern = (
    r"(^|[^A-Z])(GIFT CARD|AUTOPSY|"
    r"STORAGE AND TRANSPORTATION OF BODIES|BRAIN ONLY|DEMENTIA WORKUP|"
    r"PRODUCTION[ A-Z-]*LOT TEST|GOVERNMENT PRODUCTION LOT TEST|"
    r"SPARES PROCUREMENT)([^A-Z]|$)"
)
normalized_item_name = F.trim(
    F.regexp_replace(F.upper(F.coalesce(F.col("priced_item_name"), F.lit(""))), "[^A-Z0-9]+", " ")
)
unit_only_pattern = (
    r"^(EA|EACH|UNIT|UNITS|SYSTEM|SYSTEMS|SET|SETS|KIT|KITS|"
    r"ASSEMBLY|ASSEMBLIES)( (EA|EACH|UNIT|UNITS|SYSTEM|SYSTEMS|SET|SETS|"
    r"KIT|KITS|ASSEMBLY|ASSEMBLIES))*$"
)

classified = classified.withColumn(
    "item_class",
    F.when(
        F.col("excluded_document_class").isNotNull(),
        F.col("excluded_document_class"),
    ).when(
        F.coalesce(F.col("product_or_service_code"), F.lit("")).rlike(r"^[A-Z]"),
        F.lit("SERVICE_PSC_LINE"),
    )
    .when(text.rlike(template_pattern), F.lit("TEMPLATE_WAGE_OR_LABOR_RATE"))
    .when(text.rlike(service_pattern), F.lit("SERVICE_OR_ANCILLARY_LINE"))
    .when(text.rlike(software_pattern), F.lit("SOFTWARE_OR_LICENSE_LINE"))
    .when(text.rlike(option_pattern), F.lit("OPTION_OR_UNFUNDED_LINE"))
    .when(text.rlike(non_equipment_pattern), F.lit("NON_EQUIPMENT_BENEFIT_TEST_OR_MORTUARY_LINE"))
    .when(
        normalized_item_name.isin("AMAZON", "WALMART", "TARGET", "CVS"),
        F.lit("NON_EQUIPMENT_BENEFIT_TEST_OR_MORTUARY_LINE"),
    )
    .when(
        (F.length(normalized_item_name) == 0)
        | normalized_item_name.rlike(unit_only_pattern)
        | normalized_item_name.rlike(r"^[0-9 ]+$")
        | (normalized_item_name == F.lit("PRODUCTION")),
        F.lit("UNIDENTIFIABLE_OR_COLUMN_SHIFTED_ITEM_LINE"),
    )
    .when(text.rlike(r"HOURS?[ ]+@[ ]+EACH"), F.lit("SERVICE_OR_ANCILLARY_LINE"))
    .otherwise(F.lit("PROCUREMENT_EQUIPMENT_LINE")),
)

unique_action = F.col("linked_action_context_count") == 1
unique_award = F.col("linked_award_context_count") == 1
records = (
    classified
    .where(F.col("unit_price_usd").between(F.lit(0.01), F.lit(1_000_000_000)))
    .where(F.col("item_class").isin(
        "PROCUREMENT_EQUIPMENT_LINE", "GOVERNMENT_FURNISHED_PROPERTY_INVENTORY",
        "NON_PROCUREMENT_DOCUMENT_REQUIRES_REVIEW",
    ))
    .withColumn(
        "pricing_record_id",
        F.sha2(F.concat_ws(
            "\u001f", F.lit("SAM_GOVERNED_UNIT_PRICE_V1"),
            "physical_source_line_key", "price_basis",
        ), 256),
    )
    .withColumn("quantity", F.col("quantity").cast("decimal(20,6)"))
    .withColumn("line_amount_usd", F.col("extended_line_amount_usd").cast("decimal(30,6)"))
    .withColumn(
        "quality_status",
        F.when(F.col("item_class") == "PROCUREMENT_EQUIPMENT_LINE",
               F.lit("AUTO_ADMITTED_BY_COHORT_RULE"))
        .otherwise(F.lit("EXCLUDED_NON_PROCUREMENT_DOCUMENT")),
    )
    .withColumn(
        "clin", F.when(F.col("item_class") == "PROCUREMENT_EQUIPMENT_LINE", F.col("clin"))
        .otherwise(F.lit(None).cast("string")),
    )
    .withColumn("provenance_status", F.lit("FULL_ATTACHMENT_SOURCE_LINE_PROVENANCE"))
    .withColumn("include_in_mimir_range", F.col("item_class") == "PROCUREMENT_EQUIPMENT_LINE")
    .withColumn("promotion_eligible_without_review", F.col("item_class") == "PROCUREMENT_EQUIPMENT_LINE")
    .withColumn("transform_version", F.lit(TRANSFORM_VERSION))
    .withColumn("pricing_transform_run_id", F.lit(RUN_ID))
    .withColumn("created_at", F.lit(STARTED_AT))
    .select(
        "pricing_record_id", "physical_source_line_key", "line_candidate_id", "notice_id",
        "attachment_notice_version_id", "sam_solicitation_id", "sam_award_number",
        F.when(unique_action, F.col("contract_transaction_unique_key")).alias("contract_transaction_unique_key"),
        F.when(unique_award, F.col("contract_award_unique_key")).alias("contract_award_unique_key"),
        F.when(unique_action, F.col("award_id_piid")).alias("award_id_piid"),
        F.when(unique_action, F.col("parent_award_id_piid")).alias("parent_award_id_piid"),
        F.when(unique_action, F.col("modification_number")).alias("modification_number"),
        F.when(unique_action, F.col("action_date")).alias("action_date"),
        "linked_action_context_count", "linked_award_context_count", "clin", "priced_item_name",
        "quantity", "quantity_unit", "unit_price_usd", "price_basis", "line_amount_usd",
        "item_class", "quality_status", "product_or_service_code",
        "product_or_service_code_description", "awarding_agency_name",
        "awarding_sub_agency_name", "awarding_office_name", "recipient_name", "cage_code",
        F.when(unique_action, F.col("transaction_description")).alias("transaction_description"),
        F.when(unique_action, F.col("prime_award_base_transaction_description")).alias("prime_award_base_transaction_description"),
        F.when(unique_action, F.col("usaspending_permalink")).alias("usaspending_permalink"),
        "attachment_id", "resource_id", "attachment_name", "attachment_s3_uri",
        "attachment_sha256", "attachment_source_locator", "attachment_source_row_text",
        "attachment_source_row_sha256", "attachment_parser_version", "attachment_transform_run_id",
        F.when(unique_action, F.col("usaspending_source_s3_uri")).alias("usaspending_source_s3_uri"),
        F.when(unique_action, F.col("usaspending_source_s3_version_id")).alias("usaspending_source_s3_version_id"),
        F.when(unique_action, F.col("usaspending_source_etag")).alias("usaspending_source_etag"),
        F.when(unique_action, F.col("usaspending_source_sha256")).alias("usaspending_source_sha256"),
        F.when(unique_action, F.col("usaspending_source_locator")).alias("usaspending_source_locator"),
        "provenance_status", "include_in_mimir_range", "promotion_eligible_without_review",
        "transform_version", "pricing_transform_run_id", "created_at",
    )
)

# Attach the exact archived notice version used to source the attachment. This
# supplies a document observation date without weakening the source-line grain
# or falling back to a later/earlier version of the same notice.
records = records.join(
    notice_versions,
    on="attachment_notice_version_id",
    how="left",
)

duplicate_count = records.groupBy("pricing_record_id").count().where("count > 1").count()
missing_provenance_count = records.where(
    "attachment_s3_uri IS NULL OR attachment_sha256 IS NULL "
    "OR attachment_source_locator IS NULL OR attachment_source_row_sha256 IS NULL"
).count()
unsafe_promotion_count = records.where(
    "(item_class = 'PROCUREMENT_EQUIPMENT_LINE' AND "
    "(NOT include_in_mimir_range OR NOT promotion_eligible_without_review OR unit_price_usd <= 0)) "
    "OR (item_class <> 'PROCUREMENT_EQUIPMENT_LINE' AND "
    "(include_in_mimir_range OR promotion_eligible_without_review OR clin IS NOT NULL "
    "OR quality_status <> 'EXCLUDED_NON_PROCUREMENT_DOCUMENT'))"
).count()
missing_notice_version_provenance_count = records.where(
    "sam_notice_record_payload_sha256 IS NULL OR sam_notice_source_occurrences IS NULL"
).count()
if (
    duplicate_count
    or missing_provenance_count
    or unsafe_promotion_count
    or missing_notice_version_provenance_count
):
    raise RuntimeError(
        "Governed SAM unit-price validation failed: "
        f"duplicates={duplicate_count:,}, missing_provenance={missing_provenance_count:,}, "
        f"unsafe_promotions={unsafe_promotion_count:,}, "
        f"missing_notice_version_provenance={missing_notice_version_provenance_count:,}"
    )

records.write.mode("overwrite").parquet(STAGING)
replay = spark.read.parquet(STAGING)
replay.writeTo(TARGET).using("iceberg").tableProperty(
    "format-version", "2"
).tableProperty("write.parquet.compression-codec", "zstd").partitionedBy(
    "price_basis"
).createOrReplace()

retained_source_observation_count = replay.count()
excluded_document_record_count = replay.where("NOT include_in_mimir_range").count()
counts = replay.where("include_in_mimir_range AND promotion_eligible_without_review").agg(
    F.count(F.lit(1)).alias("governed_price_record_count"),
    F.countDistinct("priced_item_name").alias("priced_item_name_count"),
    F.countDistinct("contract_transaction_unique_key").alias("linked_action_count"),
    F.countDistinct("contract_award_unique_key").alias("linked_award_count"),
    F.sum(F.when(F.col("price_basis") == "SAM_ATTACHMENT_SOURCE_STATED_UNIT_PRICE", 1).otherwise(0)).alias("source_stated_price_count"),
    F.sum(F.when(F.col("price_basis") == "SAM_ATTACHMENT_SAME_ROW_AMOUNT_DIVIDED_BY_QUANTITY", 1).otherwise(0)).alias("same_row_derived_price_count"),
    F.sum(F.when(F.col("price_basis") == "SAM_ATTACHMENT_SOURCE_ROW_REGEX_REPAIRED_UNIT_PRICE", 1).otherwise(0)).alias("same_row_regex_repaired_price_count"),
    F.sum(F.when(F.col("price_basis") == "SAM_ATTACHMENT_TRAILING_DOLLAR_ROW_REPAIRED_UNIT_PRICE", 1).otherwise(0)).alias("trailing_dollar_repaired_price_count"),
    F.sum(F.when(F.col("price_basis") == "SAM_ATTACHMENT_EQUIPMENT_LIST_AMOUNT_DIVIDED_BY_QUANTITY", 1).otherwise(0)).alias("equipment_list_repaired_price_count"),
    F.sum(F.when(F.col("price_basis") == "SAM_ATTACHMENT_AWARD_TABLE_SOURCE_STATED_UNIT_PRICE", 1).otherwise(0)).alias("award_table_source_stated_price_count"),
    F.sum(F.when(F.col("sam_notice_posted_date").isNotNull(), 1).otherwise(0)).alias("notice_dated_price_count"),
    F.sum(
        F.when(
            F.coalesce(F.col("action_date"), F.col("sam_notice_posted_date")) >= F.lit("2024-01-01").cast("date"),
            1,
        ).otherwise(0)
    ).alias("recent_observation_price_count"),
).first().asDict()
manifest = {
    "run_id": RUN_ID,
    "generated_at": STARTED_AT.isoformat(),
    "transform_version": TRANSFORM_VERSION,
    "source": SOURCE,
    "target": TARGET,
    "staging_replay_uri": STAGING,
    **counts,
    "retained_source_observation_count": retained_source_observation_count,
    "excluded_non_procurement_document_record_count": excluded_document_record_count,
    "duplicate_pricing_id_count": duplicate_count,
    "missing_provenance_count": missing_provenance_count,
    "missing_notice_version_provenance_count": missing_notice_version_provenance_count,
    "unsafe_promotion_count": unsafe_promotion_count,
    "policy": (
        "Only reconciled source-stated attachment prices and exact same-row amount/quantity "
        "equipment lines are admitted to procurement ranges. Non-procurement document "
        "observations are retained with explicit exclusion, no CLIN and false eligibility. "
        "Price basis preserves the extraction method and stable record identity. "
        "Award totals are context only."
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
