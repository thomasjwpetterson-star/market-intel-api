"""Build release-bound platform and NIIN authorized-source depth summaries."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict

import duckdb


DEFAULT_DATA_ROOT = Path(
    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data"
)


def _sql_path(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def build_platform_source_depth(data_root: Path) -> Dict[str, Any]:
    data_root = data_root.resolve()
    platform_bom = data_root / "platform_bom.parquet"
    reference = data_root / "nsn_cage_reference.parquet"
    niin_output = data_root / "niin_source_depth.parquet"
    platform_output = data_root / "platform_source_depth.parquet"
    missing = [str(path) for path in (platform_bom, reference) if not path.exists()]
    if missing:
        raise FileNotFoundError(f"platform source-depth inputs are missing: {missing}")

    connection = duckdb.connect()
    connection.execute("SET preserve_insertion_order=false")
    connection.execute("SET threads=2")
    connection.execute("SET memory_limit='1GB'")

    connection.execute(
        f"""
        COPY (
            WITH platform_niins AS (
                SELECT DISTINCT LPAD(TRIM(niin), 9, '0') AS niin
                FROM read_parquet('{_sql_path(platform_bom)}')
                WHERE niin IS NOT NULL AND TRIM(niin) <> ''
            ), source_rollup AS (
                SELECT
                    LPAD(TRIM(r.niin), 9, '0') AS niin,
                    COUNT(DISTINCT UPPER(TRIM(r.cage))) AS referenced_supplier_site_count,
                    COUNT(DISTINCT UPPER(TRIM(r.cage))) FILTER (
                        WHERE COALESCE(r.is_active_authorized_source, false)
                    ) AS active_authorized_source_count,
                    STRING_AGG(DISTINCT UPPER(TRIM(r.cage)), ' | ' ORDER BY UPPER(TRIM(r.cage)))
                        FILTER (WHERE COALESCE(r.is_active_authorized_source, false))
                        AS active_authorized_source_cages,
                    STRING_AGG(DISTINCT TRIM(r.vendor_name), ' | ' ORDER BY TRIM(r.vendor_name))
                        FILTER (
                            WHERE COALESCE(r.is_active_authorized_source, false)
                              AND COALESCE(TRIM(r.vendor_name), '') <> ''
                        ) AS active_authorized_source_names,
                    COUNT(DISTINCT UPPER(TRIM(r.cage))) FILTER (
                        WHERE REGEXP_MATCHES(
                            COALESCE(r.rncc_codes, ''),
                            '(^|[,| ]+)3($|[,| ]+)'
                        )
                          AND REGEXP_MATCHES(
                            COALESCE(r.rnvc_codes, ''),
                            '(^|[,| ]+)2($|[,| ]+)'
                        )
                          AND REGEXP_MATCHES(
                            COALESCE(r.cage_status_codes, ''),
                            '(^|[,| ]+)A($|[,| ]+)'
                        )
                    ) AS active_manufacturer_reference_count,
                    STRING_AGG(DISTINCT UPPER(TRIM(r.cage)), ' | ' ORDER BY UPPER(TRIM(r.cage)))
                        FILTER (
                            WHERE REGEXP_MATCHES(COALESCE(r.rncc_codes, ''), '(^|[,| ]+)3($|[,| ]+)')
                              AND REGEXP_MATCHES(COALESCE(r.rnvc_codes, ''), '(^|[,| ]+)2($|[,| ]+)')
                              AND REGEXP_MATCHES(COALESCE(r.cage_status_codes, ''), '(^|[,| ]+)A($|[,| ]+)')
                        ) AS active_manufacturer_reference_cages,
                    STRING_AGG(DISTINCT TRIM(r.vendor_name), ' | ' ORDER BY TRIM(r.vendor_name))
                        FILTER (
                            WHERE REGEXP_MATCHES(COALESCE(r.rncc_codes, ''), '(^|[,| ]+)3($|[,| ]+)')
                              AND REGEXP_MATCHES(COALESCE(r.rnvc_codes, ''), '(^|[,| ]+)2($|[,| ]+)')
                              AND REGEXP_MATCHES(COALESCE(r.cage_status_codes, ''), '(^|[,| ]+)A($|[,| ]+)')
                              AND COALESCE(TRIM(r.vendor_name), '') <> ''
                        ) AS active_manufacturer_reference_names,
                    MAX(r.nsn) AS nsn,
                    MAX(r.description) AS description,
                    MAX(r.fsc_code) AS fsc_code
                FROM read_parquet('{_sql_path(reference)}') r
                SEMI JOIN platform_niins p
                  ON LPAD(TRIM(r.niin), 9, '0') = p.niin
                GROUP BY 1
            )
            SELECT
                p.niin,
                s.nsn,
                s.description,
                s.fsc_code,
                COALESCE(s.referenced_supplier_site_count, 0)
                    AS referenced_supplier_site_count,
                COALESCE(s.active_authorized_source_count, 0)
                    AS active_authorized_source_count,
                s.active_authorized_source_cages,
                s.active_authorized_source_names,
                COALESCE(s.active_manufacturer_reference_count, 0)
                    AS active_manufacturer_reference_count,
                s.active_manufacturer_reference_cages,
                s.active_manufacturer_reference_names,
                CASE
                    WHEN COALESCE(s.active_authorized_source_count, 0) = 0
                        THEN 'No active authorized source found'
                    WHEN s.active_authorized_source_count = 1
                        THEN 'One active authorized source'
                    ELSE 'Multiple active authorized sources'
                END AS source_depth,
                CASE
                    WHEN COALESCE(s.active_manufacturer_reference_count, 0) = 0
                        THEN 'No active item-identifying manufacturer reference found'
                    WHEN s.active_manufacturer_reference_count = 1
                        THEN 'One active item-identifying manufacturer reference'
                    ELSE 'Multiple active item-identifying manufacturer references'
                END AS manufacturer_reference_depth
            FROM platform_niins p
            LEFT JOIN source_rollup s USING (niin)
        ) TO '{_sql_path(niin_output)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    connection.execute(
        f"""
        COPY (
            WITH platform_items AS (
                SELECT DISTINCT
                    TRIM(platform_family) AS platform_family,
                    LPAD(TRIM(niin), 9, '0') AS niin
                FROM read_parquet('{_sql_path(platform_bom)}')
                WHERE platform_family IS NOT NULL AND TRIM(platform_family) <> ''
                  AND niin IS NOT NULL AND TRIM(niin) <> ''
            )
            SELECT
                p.platform_family,
                COUNT(*) AS associated_niin_count,
                COUNT(*) FILTER (WHERE s.active_authorized_source_count = 0)
                    AS niin_count_without_active_authorized_source,
                COUNT(*) FILTER (WHERE s.active_authorized_source_count = 1)
                    AS niin_count_with_one_active_authorized_source,
                COUNT(*) FILTER (WHERE s.active_authorized_source_count > 1)
                    AS niin_count_with_multiple_active_authorized_sources,
                CAST(SUM(s.active_authorized_source_count) AS BIGINT)
                    AS active_authorized_source_relationship_count,
                COUNT(*) FILTER (WHERE s.active_manufacturer_reference_count = 0)
                    AS niin_count_without_active_manufacturer_reference,
                COUNT(*) FILTER (WHERE s.active_manufacturer_reference_count = 1)
                    AS niin_count_with_one_active_manufacturer_reference,
                COUNT(*) FILTER (WHERE s.active_manufacturer_reference_count > 1)
                    AS niin_count_with_multiple_active_manufacturer_references,
                CAST(SUM(s.active_manufacturer_reference_count) AS BIGINT)
                    AS active_manufacturer_reference_relationship_count,
                COUNT(*) FILTER (
                    WHERE s.active_authorized_source_count = 0
                      AND s.active_manufacturer_reference_count > 0
                ) AS niin_count_without_active_authorized_but_with_active_manufacturer_reference
            FROM platform_items p
            JOIN read_parquet('{_sql_path(niin_output)}') s USING (niin)
            GROUP BY 1
            ORDER BY 1
        ) TO '{_sql_path(platform_output)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    niin_stats = connection.execute(
        f"""
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE active_authorized_source_count = 0),
               COUNT(*) FILTER (WHERE active_authorized_source_count = 1),
               COUNT(*) FILTER (WHERE active_authorized_source_count > 1),
               COUNT(*) FILTER (WHERE active_manufacturer_reference_count > 0)
        FROM read_parquet('{_sql_path(niin_output)}')
        """
    ).fetchone()
    platform_count = connection.execute(
        f"SELECT COUNT(*) FROM read_parquet('{_sql_path(platform_output)}')"
    ).fetchone()[0]
    connection.close()
    return {
        "platform_count": int(platform_count),
        "associated_niin_count": int(niin_stats[0]),
        "niins_without_active_authorized_source": int(niin_stats[1]),
        "niins_with_one_active_authorized_source": int(niin_stats[2]),
        "niins_with_multiple_active_authorized_sources": int(niin_stats[3]),
        "niins_with_active_manufacturer_reference": int(niin_stats[4]),
        "outputs": [niin_output.name, platform_output.name],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    arguments = parser.parse_args()
    print(json.dumps(build_platform_source_depth(arguments.data_root), indent=2))
