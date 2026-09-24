"""Bounded-memory rehearsal of the public NSN cohort's expensive CTEs."""

from __future__ import annotations

import argparse
import json
import resource
import tempfile
import time
from pathlib import Path

import duckdb


def escaped(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def run(supplier: Path, profile: Path, reference: Path, memory_limit: str) -> dict:
    for path in (supplier, profile, reference):
        if not path.exists():
            raise FileNotFoundError(path)
    started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="mimir-public-nsn-smoke-") as scratch:
        connection = duckdb.connect()
        connection.execute(f"SET memory_limit='{memory_limit}'")
        connection.execute("SET threads=1")
        connection.execute("SET preserve_insertion_order=false")
        connection.execute(f"SET temp_directory='{escaped(Path(scratch))}'")
        result = connection.execute(
            f"""
            WITH supplier AS (
                SELECT
                    LPAD(TRIM(CAST(niin AS VARCHAR)), 9, '0') AS niin,
                    SUM(COALESCE(TRY_CAST(total_revenue AS DOUBLE), 0)) AS observed_value,
                    COUNT(DISTINCT NULLIF(TRIM(CAST(contract_id AS VARCHAR)), '')) AS contract_count,
                    COUNT(DISTINCT NULLIF(UPPER(TRIM(CAST(cage AS VARCHAR))), '')) AS supplier_count,
                    MAX(COALESCE(TRY_CAST(platform_count AS INTEGER), 0)) AS platform_count,
                    MAX(TRY_CAST(last_sold AS DATE)) AS last_activity,
                    BIT_XOR(HASH(
                        COALESCE(UPPER(TRIM(CAST(cage AS VARCHAR))), ''),
                        COALESCE(TRIM(CAST(vendor AS VARCHAR)), ''),
                        COALESCE(TRIM(CAST(contract_id AS VARCHAR)), ''),
                        COALESCE(TRIM(CAST(platform_family AS VARCHAR)), ''),
                        COALESCE(TRY_CAST(total_revenue AS DOUBLE), 0)
                    )) AS supplier_fingerprint
                FROM read_parquet('{escaped(supplier)}')
                WHERE niin IS NOT NULL AND TRIM(CAST(niin AS VARCHAR)) <> ''
                GROUP BY 1
                HAVING SUM(COALESCE(TRY_CAST(total_revenue AS DOUBLE), 0)) > 0
                   AND COUNT(DISTINCT NULLIF(TRIM(CAST(contract_id AS VARCHAR)), '')) > 0
                   AND COUNT(DISTINCT NULLIF(UPPER(TRIM(CAST(cage AS VARCHAR))), '')) > 0
            ), profile AS (
                SELECT
                    LPAD(TRIM(CAST(niin AS VARCHAR)), 9, '0') AS niin,
                    MAX(NULLIF(REGEXP_REPLACE(CAST(nsn AS VARCHAR), '[^0-9]', '', 'g'), '')) AS nsn,
                    MAX(NULLIF(TRIM(CAST(item_name AS VARCHAR)), '')) AS item_name,
                    MAX(NULLIF(TRIM(CAST(fsc_code AS VARCHAR)), '')) AS fsc_code
                FROM read_parquet('{escaped(profile)}')
                WHERE niin IS NOT NULL
                GROUP BY 1
            ), base AS (
                SELECT supplier.*, profile.nsn, profile.item_name, profile.fsc_code
                FROM supplier INNER JOIN profile USING (niin)
                WHERE NULLIF(TRIM(CAST(profile.item_name AS VARCHAR)), '') IS NOT NULL
                  AND UPPER(TRIM(CAST(profile.item_name AS VARCHAR))) NOT IN ('NAN', 'NONE', 'NULL')
                  AND supplier.observed_value >= 100
                  AND UPPER(TRIM(COALESCE(profile.fsc_code, ''))) NOT LIKE '65%'
            ), parts AS (
                SELECT
                    LPAD(TRIM(CAST(reference.niin AS VARCHAR)), 9, '0') AS niin,
                    COUNT(DISTINCT NULLIF(TRIM(CAST(part_number AS VARCHAR)), '')) AS part_number_count,
                    BIT_XOR(HASH(
                        COALESCE(UPPER(TRIM(CAST(cage AS VARCHAR))), ''),
                        COALESCE(TRIM(CAST(part_number AS VARCHAR)), ''),
                        COALESCE(TRIM(CAST(supplier_status AS VARCHAR)), '')
                    )) AS parts_fingerprint
                FROM read_parquet('{escaped(reference)}') reference
                INNER JOIN base
                  ON LPAD(TRIM(CAST(reference.niin AS VARCHAR)), 9, '0') = base.niin
                WHERE reference.niin IS NOT NULL
                GROUP BY 1
            ), eligible AS (
                SELECT base.*, COALESCE(parts.part_number_count, 0) AS part_number_count,
                       parts.parts_fingerprint
                FROM base LEFT JOIN parts USING (niin)
                WHERE (base.platform_count > 0 OR COALESCE(parts.part_number_count, 0) > 0)
                  AND (
                        base.platform_count > 0
                     OR (
                            base.observed_value >= 1000
                        AND (
                               base.contract_count >= 2
                            OR base.supplier_count >= 2
                            OR COALESCE(parts.part_number_count, 0) >= 2
                        )
                     )
                  )
            )
            SELECT count(*) AS eligible_rows,
                   count(DISTINCT niin) AS distinct_niins,
                   BIT_XOR(HASH(niin, parts_fingerprint, supplier_fingerprint)) AS fingerprint
            FROM eligible
            """
        ).fetchone()
        spill_bytes = sum(
            path.stat().st_size for path in Path(scratch).glob("**/*") if path.is_file()
        )
        connection.close()
    if result[0] != result[1]:
        raise AssertionError("Public NSN cohort is not unique by NIIN")
    return {
        "status": "PASS",
        "memory_limit": memory_limit,
        "eligible_rows": result[0],
        "distinct_niins": result[1],
        "fingerprint": str(result[2]),
        "elapsed_seconds": round(time.perf_counter() - started, 4),
        "temporary_spill_bytes": spill_bytes,
        "process_peak_rss_raw": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--supplier", type=Path, required=True)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--memory-limit", default="650MB")
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    report = run(
        arguments.supplier,
        arguments.profile,
        arguments.reference,
        arguments.memory_limit,
    )
    rendered = json.dumps(report, indent=2)
    if arguments.output:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(rendered + "\n")
    print(rendered)


if __name__ == "__main__":
    main()
