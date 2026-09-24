#!/usr/bin/env python3
"""Materialize compact commercial NIIN metrics from typed DLA operational facts.

The job is deliberately release-scoped and candidate-first. It reads immutable,
typed Parquet, creates narrow serving sidecars, uploads them with validation
metadata, and can register release-specific candidate tables in Glue. It never
overwrites the production ``app_cache/`` prefix or the legacy ``ops_*`` tables.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

import boto3
import duckdb
import pyarrow.parquet as pq


DEFAULT_BUCKET = "a-and-d-intel-lake-newaccount"
DEFAULT_DATABASE_SILVER = "market_intel_silver"
DEFAULT_DATABASE_GOLD = "market_intel_gold"
EXTENSION_DIRECTORY = os.getenv(
    "MIMIR_DUCKDB_EXTENSION_DIRECTORY",
    "/private/tmp/mimir-duckdb-extensions",
)


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Expected an s3:// URI, received {uri!r}")
    return parsed.netloc, parsed.path.lstrip("/").rstrip("/")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parquet_metadata(path: Path) -> dict[str, Any]:
    parquet = pq.ParquetFile(path)
    schema = [
        {"name": field.name, "type": str(field.type), "nullable": field.nullable}
        for field in parquet.schema_arrow
    ]
    return {
        "filename": path.name,
        "row_count": parquet.metadata.num_rows,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "schema": schema,
        "schema_sha256": hashlib.sha256(
            json.dumps(schema, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
    }


def connect(temp_directory: Path, needs_s3: bool) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(
        config={
            "extension_directory": EXTENSION_DIRECTORY,
            "memory_limit": "2GB",
            "temp_directory": str(temp_directory),
            "threads": "4",
        }
    )
    if needs_s3:
        connection.execute("LOAD httpfs")
        connection.execute("LOAD aws")
        connection.execute("CREATE SECRET (TYPE s3, PROVIDER credential_chain)")
    connection.execute("SET preserve_insertion_order=false")
    connection.execute("SET enable_progress_bar=false")
    return connection


def sql_path(root: str, relative: str) -> str:
    return f"{root.rstrip('/')}/{relative.lstrip('/')}"


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def one(connection: duckdb.DuckDBPyConnection, query: str) -> Any:
    row = connection.execute(query).fetchone()
    return row[0] if row else None


def materialize(
    connection: duckdb.DuckDBPyConnection,
    input_root: str,
    output_directory: Path,
    source_release: str,
    retrieval_date: str,
    solicitation_root: str | None = None,
    as_of_date: str | None = None,
) -> dict[str, Any]:
    forecast_path = sql_path(input_root, "forecast/**/*.parquet")
    inventory_path = sql_path(input_root, "inventory_backorder/**/*.parquet")
    reorder_path = sql_path(input_root, "inventory_reorder_point/**/*.parquet")
    price_path = sql_path(input_root, "price_reason/**/*.parquet")

    connection.execute(
        f"CREATE OR REPLACE TEMP VIEW source_forecast AS "
        f"SELECT * FROM read_parquet({sql_literal(forecast_path)}, hive_partitioning=true)"
    )
    connection.execute(
        f"CREATE OR REPLACE TEMP VIEW source_inventory AS "
        f"SELECT * FROM read_parquet({sql_literal(inventory_path)}, hive_partitioning=true)"
    )
    connection.execute(
        f"CREATE OR REPLACE TEMP VIEW source_reorder AS "
        f"SELECT * FROM read_parquet({sql_literal(reorder_path)}, hive_partitioning=true)"
    )
    connection.execute(
        f"CREATE OR REPLACE TEMP VIEW source_price AS "
        f"SELECT * FROM read_parquet({sql_literal(price_path)}, hive_partitioning=true)"
    )

    latest_inventory = one(
        connection,
        "SELECT max(snapshot_date) FROM source_inventory WHERE public_release_eligible",
    )
    latest_reorder = one(
        connection,
        "SELECT max(snapshot_date) FROM source_reorder WHERE public_release_eligible",
    )
    forecast_start = one(
        connection,
        "SELECT min(forecast_month) FROM source_forecast "
        "WHERE public_release_eligible AND niin IS NOT NULL",
    )
    latest_price = one(
        connection,
        "SELECT max(award_date) FROM source_price WHERE public_release_eligible",
    )
    if not all((latest_inventory, latest_reorder, forecast_start, latest_price)):
        raise RuntimeError("One or more operational sources have no public-release-eligible rows")

    supply_path = output_directory / "nsn_supply_state_lookup.parquet"
    price_summary_path = output_directory / "nsn_price_summary_lookup.parquet"
    source_release_sql = sql_literal(source_release)
    retrieval_date_sql = sql_literal(retrieval_date)

    connection.execute(
        f"""
        COPY (
          WITH inventory AS (
            SELECT
              niin,
              max(nsn) AS nsn,
              max(fsc) AS fsc,
              max(total_stock) AS total_stock,
              max(backorder_qty) AS backorder_qty,
              max(annual_demand_quantity) AS annual_demand_quantity,
              string_agg(DISTINCT condition_code, ' | ' ORDER BY condition_code)
                FILTER (WHERE condition_code IS NOT NULL AND condition_code <> '')
                AS inventory_condition_codes,
              count(*) AS inventory_source_row_count
            FROM source_inventory
            WHERE public_release_eligible
              AND snapshot_date = DATE {sql_literal(str(latest_inventory))}
            GROUP BY niin
          ),
          reorder AS (
            SELECT
              niin,
              max(nsn) AS nsn,
              max(fsc) AS fsc,
              max(total_stock) AS reorder_reported_total_stock,
              max(reorder_point) AS reorder_point,
              string_agg(DISTINCT condition_code, ' | ' ORDER BY condition_code)
                FILTER (WHERE condition_code IS NOT NULL AND condition_code <> '')
                AS reorder_condition_codes,
              count(*) AS reorder_source_row_count
            FROM source_reorder
            WHERE public_release_eligible
              AND snapshot_date = DATE {sql_literal(str(latest_reorder))}
            GROUP BY niin
          ),
          forecast AS (
            SELECT
              niin,
              sum(forecast_qty) FILTER (
                WHERE forecast_month >= DATE {sql_literal(str(forecast_start))}
                  AND forecast_month < DATE {sql_literal(str(forecast_start))} + INTERVAL 3 MONTH
              ) AS forecast_3m_qty,
              sum(forecast_qty) FILTER (
                WHERE forecast_month >= DATE {sql_literal(str(forecast_start))}
                  AND forecast_month < DATE {sql_literal(str(forecast_start))} + INTERVAL 6 MONTH
              ) AS forecast_6m_qty,
              sum(forecast_qty) FILTER (
                WHERE forecast_month >= DATE {sql_literal(str(forecast_start))}
                  AND forecast_month < DATE {sql_literal(str(forecast_start))} + INTERVAL 12 MONTH
              ) AS forecast_12m_qty,
              sum(forecast_qty) FILTER (
                WHERE forecast_month >= DATE {sql_literal(str(forecast_start))}
                  AND forecast_month < DATE {sql_literal(str(forecast_start))} + INTERVAL 24 MONTH
              ) AS forecast_24m_qty,
              count(*) AS forecast_source_row_count
            FROM source_forecast
            WHERE public_release_eligible AND niin IS NOT NULL
            GROUP BY niin
          ),
          keys AS (
            SELECT niin FROM inventory
            UNION SELECT niin FROM reorder
            UNION SELECT niin FROM forecast
          ),
          combined AS (
            SELECT
              k.niin,
              coalesce(i.nsn, r.nsn) AS nsn,
              coalesce(i.fsc, r.fsc) AS fsc,
              i.total_stock,
              i.backorder_qty,
              i.annual_demand_quantity,
              r.reorder_reported_total_stock,
              r.reorder_point,
              f.forecast_3m_qty,
              f.forecast_6m_qty,
              f.forecast_12m_qty,
              f.forecast_24m_qty,
              i.inventory_condition_codes,
              r.reorder_condition_codes,
              i.inventory_source_row_count,
              r.reorder_source_row_count,
              f.forecast_source_row_count
            FROM keys k
            LEFT JOIN inventory i USING (niin)
            LEFT JOIN reorder r USING (niin)
            LEFT JOIN forecast f USING (niin)
          )
          SELECT
            niin,
            nsn,
            fsc,
            CAST(total_stock AS DOUBLE) AS total_stock,
            CAST(backorder_qty AS DOUBLE) AS backorder_qty,
            CAST(annual_demand_quantity AS DOUBLE) AS annual_demand_quantity,
            CAST(reorder_reported_total_stock AS DOUBLE) AS reorder_assessment_stock,
            CAST(reorder_point AS DOUBLE) AS reorder_point,
            CAST(
              CASE
                WHEN reorder_point IS NULL OR reorder_reported_total_stock IS NULL THEN NULL
                WHEN reorder_point > reorder_reported_total_stock
                  THEN reorder_point - reorder_reported_total_stock
                ELSE 0
              END AS DOUBLE
            ) AS reorder_point_gap,
            coalesce(reorder_point > reorder_reported_total_stock, false)
              AS below_reorder_point,
            CAST(forecast_3m_qty AS BIGINT) AS forecast_3m_qty,
            CAST(forecast_6m_qty AS BIGINT) AS forecast_6m_qty,
            CAST(forecast_12m_qty AS BIGINT) AS forecast_12m_qty,
            CAST(forecast_24m_qty AS BIGINT) AS forecast_24m_qty,
            CAST(
              CASE
                WHEN forecast_12m_qty > 0 AND total_stock IS NOT NULL
                THEN total_stock * 12.0 / forecast_12m_qty
              END AS DOUBLE
            ) AS forecast_stock_cover_months,
            CAST(
              CASE
                WHEN annual_demand_quantity > 0 AND total_stock IS NOT NULL
                THEN total_stock * 12.0 / annual_demand_quantity
              END AS DOUBLE
            ) AS adq_stock_cover_months,
            CASE
              WHEN backorder_qty > 0 THEN 'BACKORDERED'
              WHEN reorder_point IS NOT NULL AND reorder_reported_total_stock IS NOT NULL
                   AND reorder_reported_total_stock < reorder_point THEN 'BELOW_REORDER_POINT'
              WHEN forecast_12m_qty > 0 AND coalesce(total_stock, 0) <= 0
                   THEN 'FORECAST_DEMAND_NO_STOCK'
              WHEN forecast_12m_qty > 0 AND total_stock * 12.0 / forecast_12m_qty < 3
                   THEN 'LOW_FORECAST_COVER'
              WHEN forecast_12m_qty > 0 THEN 'FORECAST_DEMAND_SIGNAL'
              WHEN total_stock IS NOT NULL OR reorder_point IS NOT NULL
                   THEN 'NO_CURRENT_RISK_SIGNAL'
              ELSE 'INSUFFICIENT_DATA'
            END AS supply_signal,
            DATE {sql_literal(str(latest_inventory))} AS inventory_snapshot_date,
            DATE {sql_literal(str(latest_reorder))} AS reorder_point_snapshot_date,
            DATE {sql_literal(str(forecast_start))} AS forecast_start_month,
            inventory_condition_codes,
            reorder_condition_codes,
            CAST(inventory_source_row_count AS INTEGER) AS inventory_source_row_count,
            CAST(reorder_source_row_count AS INTEGER) AS reorder_source_row_count,
            CAST(forecast_source_row_count AS INTEGER) AS forecast_source_row_count,
            CASE
              WHEN total_stock IS NOT NULL AND reorder_reported_total_stock IS NOT NULL
              THEN total_stock = reorder_reported_total_stock
              ELSE NULL
            END AS stock_sources_agree,
            true AS public_release_eligible,
            DATE {retrieval_date_sql} AS source_retrieval_date,
            {source_release_sql} AS source_release,
            'DLA_FOIA_OPERATIONAL' AS source_product
          FROM combined
          ORDER BY niin
        ) TO {sql_literal(str(supply_path))}
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
        """
    )

    connection.execute(
        f"""
        COPY (
          WITH eligible AS (
            SELECT *
            FROM source_price
            WHERE public_release_eligible AND net_price > 0
          ),
          ranked AS (
            SELECT
              *,
              row_number() OVER (
                PARTITION BY niin
                ORDER BY award_date DESC, contract_number DESC,
                         purchase_order_number DESC, purchase_order_item_number DESC
              ) AS recency_rank
            FROM eligible
          )
          SELECT
            niin,
            max(nsn) AS nsn,
            max(fsc) AS fsc,
            max(award_date) AS latest_price_date,
            max(net_price) FILTER (WHERE recency_rank = 1) AS latest_net_price,
            max(order_quantity) FILTER (WHERE recency_rank = 1) AS latest_order_quantity,
            max(unit_of_issue) FILTER (WHERE recency_rank = 1) AS latest_unit_of_issue,
            max(cage_code) FILTER (WHERE recency_rank = 1) AS latest_cage_code,
            max(contract_number) FILTER (WHERE recency_rank = 1) AS latest_contract_number,
            max(price_reason_type) FILTER (WHERE recency_rank = 1) AS latest_price_reason_type,
            count(*) AS price_observation_count,
            count(DISTINCT contract_number) AS price_contract_count,
            count(DISTINCT cage_code) AS price_cage_count,
            min(net_price) FILTER (
              WHERE award_date >= DATE {sql_literal(str(latest_price))} - INTERVAL 12 MONTH
            ) AS trailing_12m_min_price,
            max(net_price) FILTER (
              WHERE award_date >= DATE {sql_literal(str(latest_price))} - INTERVAL 12 MONTH
            ) AS trailing_12m_max_price,
            avg(net_price) FILTER (
              WHERE award_date >= DATE {sql_literal(str(latest_price))} - INTERVAL 12 MONTH
            ) AS trailing_12m_mean_price,
            median(net_price) FILTER (
              WHERE award_date >= DATE {sql_literal(str(latest_price))} - INTERVAL 12 MONTH
            ) AS trailing_12m_median_price,
            sum(net_price * order_quantity) FILTER (
              WHERE award_date >= DATE {sql_literal(str(latest_price))} - INTERVAL 12 MONTH
                AND order_quantity > 0
            ) / nullif(sum(order_quantity) FILTER (
              WHERE award_date >= DATE {sql_literal(str(latest_price))} - INTERVAL 12 MONTH
                AND order_quantity > 0
            ), 0) AS trailing_12m_quantity_weighted_price,
            count(*) FILTER (
              WHERE award_date >= DATE {sql_literal(str(latest_price))} - INTERVAL 12 MONTH
            ) AS trailing_12m_observation_count,
            true AS public_release_eligible,
            DATE {retrieval_date_sql} AS source_retrieval_date,
            {source_release_sql} AS source_release,
            'DLA_FOIA_PRICE_REASON' AS source_product
          FROM ranked
          GROUP BY niin
          ORDER BY niin
        ) TO {sql_literal(str(price_summary_path))}
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
        """
    )

    artifacts = {
        supply_path.name: parquet_metadata(supply_path),
        price_summary_path.name: parquet_metadata(price_summary_path),
    }
    opportunity_metrics: dict[str, Any] = {}
    if solicitation_root:
        opportunity_metrics = materialize_opportunities(
            connection=connection,
            solicitation_root=solicitation_root,
            output_directory=output_directory,
            source_release=source_release,
            as_of_date=as_of_date or retrieval_date,
        )
        artifacts.update(opportunity_metrics.pop("artifacts"))

    return {
        "source_release": source_release,
        "retrieval_date": retrieval_date,
        "inventory_snapshot_date": str(latest_inventory),
        "reorder_point_snapshot_date": str(latest_reorder),
        "forecast_start_month": str(forecast_start),
        "latest_price_date": str(latest_price),
        "artifacts": artifacts,
        **opportunity_metrics,
        "metric_contract": {
            "inventory_aggregation": "MAX at NIIN/snapshot grain because condition/source rows repeat NIIN totals",
            "forecast_windows": "Sum of public-eligible forecast rows from the source release's first forecast month",
            "price_window": "Trailing 12 months ending on the maximum public-eligible award date in the release",
            "security_rule": "Only source rows explicitly marked public_release_eligible are materialized",
        },
    }


def materialize_opportunities(
    connection: duckdb.DuckDBPyConnection,
    solicitation_root: str,
    output_directory: Path,
    source_release: str,
    as_of_date: str,
) -> dict[str, Any]:
    """Build direct-NSN active-solicitation summary and keyed detail sidecars."""
    solicitation_path = (
        solicitation_root
        if solicitation_root.endswith(".parquet") or "*" in solicitation_root
        else sql_path(solicitation_root, "**/*.parquet")
    )
    connection.execute(
        "CREATE OR REPLACE TEMP VIEW source_solicitations AS "
        f"SELECT * FROM read_parquet({sql_literal(solicitation_path)}, "
        "hive_partitioning=true, union_by_name=true)"
    )
    summary_path = output_directory / "nsn_opportunity_summary_lookup.parquet"
    detail_path = output_directory / "nsn_opportunity_detail.parquet"
    as_of_sql = sql_literal(as_of_date)
    source_release_sql = sql_literal(source_release)

    normalized = f"""
        WITH normalized AS (
            SELECT
                RIGHT(REGEXP_REPLACE(TRIM(nsn), '[^0-9]', '', 'g'), 9) AS niin,
                CASE
                    WHEN LENGTH(REGEXP_REPLACE(TRIM(nsn), '[^0-9]', '', 'g')) = 13
                    THEN REGEXP_REPLACE(TRIM(nsn), '[^0-9]', '', 'g')
                END AS nsn,
                NULLIF(TRIM(solicitation_number), '') AS solicitation_number,
                NULLIF(TRIM(solicitation_line_number), '') AS solicitation_line_number,
                TRY_STRPTIME(TRIM(return_by_date), '%m/%d/%Y')::DATE AS response_deadline,
                TRY_CAST(NULLIF(REGEXP_REPLACE(TRIM(quantity), '[^0-9.+-]', '', 'g'), '') AS DOUBLE) AS quantity,
                NULLIF(TRIM(unit_of_issue), '') AS unit_of_issue,
                NULLIF(TRIM(solicitation_type_indicator), '') AS solicitation_type_indicator,
                NULLIF(TRIM(small_business_set_aside_indicator), '') AS small_business_set_aside_indicator,
                NULLIF(TRIM(purchase_request_number), '') AS purchase_request_number,
                NULLIF(TRIM(hazardous_material_id), '') AS hazardous_material_id,
                NULLIF(TRIM(material_requirements), '') AS material_requirements,
                NULLIF(TRIM(source_of_supply_cage), '') AS source_of_supply_cage,
                NULLIF(TRIM(actual_mfg_source_cage), '') AS actual_mfg_source_cage,
                NULLIF(TRIM(actual_mfg_source_name_address), '') AS actual_mfg_source_name_address
            FROM source_solicitations
            WHERE LENGTH(REGEXP_REPLACE(COALESCE(TRIM(nsn), ''), '[^0-9]', '', 'g')) IN (9, 13)
              AND NULLIF(TRIM(solicitation_number), '') IS NOT NULL
        ), active AS (
            SELECT *
            FROM normalized
            WHERE response_deadline >= DATE {as_of_sql}
        )
    """
    connection.execute(
        f"""
        COPY (
            {normalized}
            SELECT DISTINCT
                niin, nsn, solicitation_number, solicitation_line_number,
                response_deadline, quantity, unit_of_issue,
                solicitation_type_indicator, small_business_set_aside_indicator,
                purchase_request_number, hazardous_material_id, material_requirements,
                source_of_supply_cage, actual_mfg_source_cage,
                actual_mfg_source_name_address,
                DATE {as_of_sql} AS source_as_of_date,
                {source_release_sql} AS source_release,
                'DLA_FACT_SOLICITATIONS' AS source_product
            FROM active
            ORDER BY niin, response_deadline, solicitation_number, solicitation_line_number
        ) TO {sql_literal(str(detail_path))}
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)
        """
    )
    connection.execute(
        f"""
        COPY (
            {normalized}, ranked AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY niin
                    ORDER BY response_deadline, solicitation_number, solicitation_line_number
                ) AS opportunity_rank
                FROM active
            ), totals AS (
                SELECT
                    niin,
                    COUNT(DISTINCT solicitation_number) AS active_solicitation_count,
                    MIN(response_deadline) AS next_response_deadline
                FROM active
                GROUP BY niin
            )
            SELECT
                ranked.niin, ranked.nsn,
                totals.active_solicitation_count, totals.next_response_deadline,
                ranked.solicitation_number AS next_solicitation_number,
                ranked.quantity AS next_quantity,
                ranked.unit_of_issue AS next_unit_of_issue,
                ranked.solicitation_type_indicator AS next_solicitation_type_indicator,
                ranked.small_business_set_aside_indicator AS next_small_business_set_aside_indicator,
                ranked.purchase_request_number AS next_purchase_request_number,
                DATE {as_of_sql} AS source_as_of_date,
                {source_release_sql} AS source_release,
                'DLA_FACT_SOLICITATIONS' AS source_product
            FROM ranked
            INNER JOIN totals USING (niin)
            WHERE ranked.opportunity_rank = 1
            ORDER BY ranked.niin
        ) TO {sql_literal(str(summary_path))}
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)
        """
    )
    return {
        "solicitation_as_of_date": as_of_date,
        "active_opportunity_detail_rows": int(one(
            connection,
            f"SELECT COUNT(*) FROM read_parquet({sql_literal(str(detail_path))})",
        ) or 0),
        "active_opportunity_niins": int(one(
            connection,
            f"SELECT COUNT(*) FROM read_parquet({sql_literal(str(summary_path))})",
        ) or 0),
        "artifacts": {
            summary_path.name: parquet_metadata(summary_path),
            detail_path.name: parquet_metadata(detail_path),
        },
    }


def upload_artifacts(
    s3: Any,
    output_directory: Path,
    manifest: dict[str, Any],
    prefixes: Iterable[str],
) -> list[dict[str, Any]]:
    uploaded: list[dict[str, Any]] = []
    for prefix_index, prefix in enumerate(prefixes):
        bucket, key_prefix = parse_s3_uri(prefix)
        for filename, metadata in manifest["artifacts"].items():
            # The canonical Gold candidate keeps datasets in separate folders so
            # Athena never scans two schemas from one LOCATION. The optional
            # serving candidate mirrors the flat app-cache contract.
            if prefix_index == 0:
                dataset = {
                    "nsn_supply_state_lookup.parquet": "metric_niin_supply_state",
                    "nsn_price_summary_lookup.parquet": "metric_niin_price_summary",
                    "nsn_opportunity_summary_lookup.parquet": "metric_niin_opportunity_summary",
                    "nsn_opportunity_detail.parquet": "fact_niin_active_solicitation",
                }[filename]
                key = f"{key_prefix}/{dataset}/data.parquet"
            else:
                key = f"{key_prefix}/{filename}"
            s3.upload_file(
                str(output_directory / filename),
                bucket,
                key,
                ExtraArgs={
                    "ServerSideEncryption": "AES256",
                    "Metadata": {
                        "sha256": metadata["sha256"],
                        "row-count": str(metadata["row_count"]),
                        "schema-sha256": metadata["schema_sha256"],
                        "source-release": manifest["source_release"],
                        "source-retrieval-date": manifest["retrieval_date"],
                    },
                },
            )
            head = s3.head_object(Bucket=bucket, Key=key)
            uploaded.append(
                {
                    "s3_uri": f"s3://{bucket}/{key}",
                    "version_id": head.get("VersionId"),
                    "size_bytes": int(head["ContentLength"]),
                    "sha256": (head.get("Metadata") or {}).get("sha256"),
                }
            )
        manifest_key = f"{key_prefix}/_MANIFEST.json"
        body = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")
        s3.put_object(
            Bucket=bucket,
            Key=manifest_key,
            Body=body,
            ContentType="application/json",
            ServerSideEncryption="AES256",
        )
        uploaded.append({"s3_uri": f"s3://{bucket}/{manifest_key}"})
    return uploaded


def upload_manifests(
    s3: Any,
    manifest: dict[str, Any],
    prefixes: Iterable[str],
) -> None:
    body = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")
    for prefix in prefixes:
        bucket, key_prefix = parse_s3_uri(prefix)
        s3.put_object(
            Bucket=bucket,
            Key=f"{key_prefix}/_MANIFEST.json",
            Body=body,
            ContentType="application/json",
            ServerSideEncryption="AES256",
        )


PARQUET_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
PARQUET_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
PARQUET_SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"


def storage_descriptor(location: str, columns: list[tuple[str, str]]) -> dict[str, Any]:
    return {
        "Columns": [{"Name": name, "Type": data_type} for name, data_type in columns],
        "Location": location.rstrip("/") + "/",
        "InputFormat": PARQUET_INPUT_FORMAT,
        "OutputFormat": PARQUET_OUTPUT_FORMAT,
        "SerdeInfo": {"SerializationLibrary": PARQUET_SERDE, "Parameters": {"serialization.format": "1"}},
        "StoredAsSubDirectories": False,
    }


def put_table(glue: Any, database: str, table_input: dict[str, Any]) -> None:
    try:
        glue.get_table(DatabaseName=database, Name=table_input["Name"])
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=database, TableInput=table_input)
    else:
        glue.update_table(DatabaseName=database, TableInput=table_input)


def register_candidate_tables(
    glue: Any,
    input_root: str,
    output_prefix: str,
    source_release: str,
    manifest: dict[str, Any],
) -> dict[str, str]:
    release_suffix = re.sub(r"[^0-9]", "", source_release)[:8]
    if len(release_suffix) != 8:
        raise ValueError(f"Source release must begin with a date: {source_release}")
    source_locations = {
        "forecast": sql_path(input_root, "forecast"),
        "inventory": sql_path(input_root, "inventory_backorder"),
        "reorder": sql_path(input_root, "inventory_reorder_point"),
        "price": sql_path(input_root, "price_reason"),
    }
    metric_locations = {
        "supply": output_prefix.rstrip("/") + "/metric_niin_supply_state",
        "price_summary": output_prefix.rstrip("/") + "/metric_niin_price_summary",
        "opportunity_summary": output_prefix.rstrip("/") + "/metric_niin_opportunity_summary",
        "opportunity_detail": output_prefix.rstrip("/") + "/fact_niin_active_solicitation",
    }
    names = {
        "forecast": f"fact_dla_forecast_demand_{release_suffix}_candidate",
        "inventory": f"fact_dla_inventory_snapshot_{release_suffix}_candidate",
        "reorder": f"fact_dla_reorder_point_snapshot_{release_suffix}_candidate",
        "price": f"fact_dla_price_reason_{release_suffix}_candidate",
        "supply": f"metric_niin_supply_state_{release_suffix}_candidate",
        "price_summary": f"metric_niin_price_summary_{release_suffix}_candidate",
        "opportunity_summary": f"metric_niin_opportunity_summary_{release_suffix}_candidate",
        "opportunity_detail": f"fact_niin_active_solicitation_{release_suffix}_candidate",
    }
    common_parameters = {
        "classification": "parquet",
        "candidate": "true",
        "source_release": source_release,
        "EXTERNAL": "TRUE",
    }
    definitions = {
        "forecast": [
            ("source_item_id", "string"), ("niin", "string"), ("identifier_type", "string"),
            ("forecast_qty", "bigint"), ("forecast_month", "date"),
            ("security_classification", "string"), ("public_release_eligible", "boolean"),
            ("retrieval_date", "date"), ("source_release", "string"), ("source_file", "string"),
        ],
        "inventory": [
            ("nsn", "string"), ("fsc", "string"), ("niin", "string"),
            ("total_stock", "decimal(20,3)"), ("security_classification", "string"),
            ("condition_code", "string"), ("backorder_qty", "decimal(20,3)"),
            ("annual_demand_quantity", "decimal(20,3)"), ("public_release_eligible", "boolean"),
            ("retrieval_date", "date"), ("source_release", "string"), ("source_file", "string"),
        ],
        "reorder": [
            ("nsn", "string"), ("fsc", "string"), ("niin", "string"),
            ("total_stock", "decimal(20,3)"), ("reorder_point", "decimal(20,3)"),
            ("security_classification", "string"), ("condition_code", "string"),
            ("public_release_eligible", "boolean"), ("retrieval_date", "date"),
            ("source_release", "string"), ("source_file", "string"),
        ],
        "price": [
            ("fsc", "string"), ("niin", "string"), ("nsn", "string"),
            ("security_classification", "string"), ("unit_of_issue", "string"),
            ("cage_code", "string"), ("contract_number", "string"),
            ("order_quantity", "decimal(20,3)"), ("award_date", "date"),
            ("net_price", "decimal(20,4)"), ("purchase_order_number", "string"),
            ("purchase_order_item_number", "string"), ("price_reviewer_code", "string"),
            ("price_reason_type", "string"), ("duplicate_count", "bigint"),
            ("public_release_eligible", "boolean"), ("retrieval_date", "date"),
            ("source_release", "string"), ("source_file", "string"),
        ],
    }
    for key in ("forecast", "inventory", "reorder", "price"):
        partition_keys = []
        table_parameters = dict(common_parameters)
        if key in {"inventory", "reorder"}:
            # Enum projection is intentionally a string partition. The source
            # folder values are ISO dates and consumers can CAST when needed;
            # Athena's enum projection does not accept a DATE partition type.
            partition_keys = [{"Name": "snapshot_date", "Type": "string"}]
            table_parameters.update(
                {
                    "projection.enabled": "true",
                    "projection.snapshot_date.type": "enum",
                    "projection.snapshot_date.values": ",".join(
                        manifest[f"{key if key != 'reorder' else 'reorder_point'}_snapshot_dates"]
                    ),
                    "storage.location.template": source_locations[key]
                    + "/snapshot_date=${snapshot_date}/",
                }
            )
        put_table(
            glue,
            DEFAULT_DATABASE_SILVER,
            {
                "Name": names[key],
                "Description": f"Release-scoped typed DLA operational candidate for {source_release}",
                "TableType": "EXTERNAL_TABLE",
                "Parameters": table_parameters,
                "PartitionKeys": partition_keys,
                "StorageDescriptor": storage_descriptor(source_locations[key], definitions[key]),
            },
        )

    for key, filename in (
        ("supply", "nsn_supply_state_lookup.parquet"),
        ("price_summary", "nsn_price_summary_lookup.parquet"),
        ("opportunity_summary", "nsn_opportunity_summary_lookup.parquet"),
        ("opportunity_detail", "nsn_opportunity_detail.parquet"),
    ):
        if filename not in manifest["artifacts"]:
            continue
        schema = manifest["artifacts"][filename]["schema"]
        arrow_to_glue = {
            "string": "string", "bool": "boolean", "date32[day]": "date",
            "int32": "int", "int64": "bigint", "double": "double",
            "decimal128(20, 3)": "decimal(20,3)", "decimal128(20, 4)": "decimal(20,4)",
            "decimal128(38, 8)": "decimal(38,8)",
        }
        columns = []
        for field in schema:
            field_type = arrow_to_glue.get(field["type"])
            if field_type is None:
                if field["type"].startswith("decimal128"):
                    field_type = field["type"].replace("decimal128", "decimal").replace(" ", "")
                else:
                    raise ValueError(f"No Glue type mapping for {field['type']} ({field['name']})")
            columns.append((field["name"], field_type))
        put_table(
            glue,
            DEFAULT_DATABASE_GOLD,
            {
                "Name": names[key],
                "Description": f"Compact release-scoped NIIN metric candidate for {source_release}",
                "TableType": "EXTERNAL_TABLE",
                "Parameters": dict(common_parameters),
                "PartitionKeys": [],
                "StorageDescriptor": storage_descriptor(metric_locations[key], columns),
            },
        )
    return names


def snapshot_dates(connection: duckdb.DuckDBPyConnection, view: str) -> list[str]:
    return [str(row[0]) for row in connection.execute(
        f"SELECT DISTINCT snapshot_date FROM {view} ORDER BY snapshot_date"
    ).fetchall()]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-release", required=True)
    parser.add_argument("--retrieval-date", required=True)
    parser.add_argument("--input-root")
    parser.add_argument("--output-prefix")
    parser.add_argument("--serving-candidate-prefix")
    parser.add_argument(
        "--solicitation-root",
        default=f"s3://{DEFAULT_BUCKET}/silver/dla/fact_solicitations",
    )
    parser.add_argument("--as-of-date")
    parser.add_argument(
        "--candidate-pointer-key",
        default="mimir/nsn-enrichment-candidates/candidate_manifest.json",
    )
    parser.add_argument("--register-glue", action="store_true")
    args = parser.parse_args()

    input_root = args.input_root or (
        f"s3://{DEFAULT_BUCKET}/silver/dla/operational_v2_candidate/"
        f"source_release={args.source_release}"
    )
    output_prefix = args.output_prefix or (
        f"s3://{DEFAULT_BUCKET}/gold/dla/operational_metrics_candidate/"
        f"source_release={args.source_release}"
    )
    needs_s3 = input_root.startswith("s3://")

    with tempfile.TemporaryDirectory(prefix="mimir-dla-operational-") as directory:
        work = Path(directory)
        output_directory = work / "output"
        output_directory.mkdir()
        connection = connect(work / "duckdb-temp", needs_s3=needs_s3)
        try:
            manifest = materialize(
                connection,
                input_root,
                output_directory,
                args.source_release,
                args.retrieval_date,
                solicitation_root=args.solicitation_root,
                as_of_date=args.as_of_date or args.retrieval_date,
            )
            manifest["inventory_snapshot_dates"] = snapshot_dates(connection, "source_inventory")
            manifest["reorder_point_snapshot_dates"] = snapshot_dates(connection, "source_reorder")
        finally:
            connection.close()
        manifest["generated_at"] = datetime.now(timezone.utc).isoformat()
        manifest["input_root"] = input_root
        manifest["output_prefix"] = output_prefix

        session = boto3.Session(region_name=os.getenv("AWS_REGION", "us-east-1"))
        s3 = session.client("s3")
        prefixes = [output_prefix]
        if args.serving_candidate_prefix:
            prefixes.append(args.serving_candidate_prefix)
        if args.register_glue:
            manifest["candidate_glue_tables"] = register_candidate_tables(
                session.client("glue"),
                input_root,
                output_prefix,
                args.source_release,
                manifest,
            )
        manifest["uploaded_objects"] = upload_artifacts(
            s3, output_directory, manifest, prefixes
        )
        upload_manifests(s3, manifest, prefixes)
        if args.candidate_pointer_key:
            pointer_body = (
                json.dumps(manifest, indent=2, sort_keys=True) + "\n"
            ).encode("utf-8")
            s3.put_object(
                Bucket=DEFAULT_BUCKET,
                Key=args.candidate_pointer_key.strip().lstrip("/"),
                Body=pointer_body,
                ContentType="application/json",
                ServerSideEncryption="AES256",
                Metadata={
                    "source-release": args.source_release,
                    "manifest-sha256": hashlib.sha256(pointer_body).hexdigest(),
                },
            )
        print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
