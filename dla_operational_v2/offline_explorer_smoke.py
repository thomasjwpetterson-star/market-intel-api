"""Offline integrity and bounded-memory smoke test for Explorer enrichment."""

from __future__ import annotations

import argparse
import json
import resource
import tempfile
import time
from pathlib import Path

import duckdb


def sql_path(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def scalar(connection: duckdb.DuckDBPyConnection, sql: str):
    return connection.execute(sql).fetchone()[0]


def run(
    reference: Path,
    supply: Path,
    price: Path,
    opportunity: Path,
    memory_limit: str,
) -> dict:
    for path in (reference, supply, price, opportunity):
        if not path.exists():
            raise FileNotFoundError(path)

    started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="mimir-explorer-smoke-") as scratch:
        connection = duckdb.connect()
        connection.execute(f"SET memory_limit='{memory_limit}'")
        connection.execute("SET threads=2")
        connection.execute("SET preserve_insertion_order=false")
        connection.execute(f"SET temp_directory='{sql_path(Path(scratch))}'")
        connection.execute(
            f"CREATE VIEW reference AS SELECT * FROM read_parquet('{sql_path(reference)}')"
        )
        connection.execute(
            f"CREATE VIEW supply AS SELECT * FROM read_parquet('{sql_path(supply)}')"
        )
        connection.execute(
            f"CREATE VIEW price AS SELECT * FROM read_parquet('{sql_path(price)}')"
        )
        connection.execute(
            f"CREATE VIEW opportunity AS SELECT * FROM read_parquet('{sql_path(opportunity)}')"
        )
        niin_expr = "CAST(r.niin AS VARCHAR)"
        connection.execute(
            f"""
            CREATE VIEW enriched AS
            SELECT
                r.*,
                s.supply_signal,
                s.total_stock,
                s.backorder_qty,
                s.reorder_point_gap,
                s.forecast_3m_qty,
                s.forecast_12m_qty,
                s.forecast_stock_cover_months,
                s.inventory_snapshot_date,
                s.source_release AS operational_source_release,
                p.latest_price_date,
                p.latest_net_price,
                p.price_observation_count,
                p.trailing_12m_min_price,
                p.trailing_12m_max_price,
                p.trailing_12m_median_price,
                o.active_solicitation_count,
                o.next_response_deadline,
                o.next_solicitation_number,
                o.next_quantity
            FROM reference r
            LEFT JOIN supply s ON {niin_expr} = s.niin
            LEFT JOIN price p ON {niin_expr} = p.niin
            LEFT JOIN opportunity o ON {niin_expr} = o.niin
            """
        )

        supply_rows, supply_distinct = connection.execute(
            "SELECT count(*), count(DISTINCT niin) FROM supply"
        ).fetchone()
        price_rows, price_distinct = connection.execute(
            "SELECT count(*), count(DISTINCT niin) FROM price"
        ).fetchone()
        opportunity_rows, opportunity_distinct = connection.execute(
            "SELECT count(*), count(DISTINCT niin) FROM opportunity"
        ).fetchone()
        if (
            supply_rows != supply_distinct
            or price_rows != price_distinct
            or opportunity_rows != opportunity_distinct
        ):
            raise AssertionError("Sidecars must contain exactly one row per NIIN")

        count_started = time.perf_counter()
        raw_rows = scalar(connection, "SELECT count(*) FROM reference")
        enriched_rows = scalar(connection, "SELECT count(*) FROM enriched")
        full_count_seconds = time.perf_counter() - count_started
        if raw_rows != enriched_rows:
            raise AssertionError(
                f"Lazy enrichment changed relationship grain: {raw_rows} -> {enriched_rows}"
            )

        samples = connection.execute(
            f"""
            SELECT {niin_expr} AS niin, count(*) AS relationship_rows
            FROM reference r
            INNER JOIN supply s ON {niin_expr} = s.niin
            INNER JOIN price p ON {niin_expr} = p.niin
            INNER JOIN opportunity o ON {niin_expr} = o.niin
            GROUP BY 1
            ORDER BY relationship_rows DESC, niin
            LIMIT 5
            """
        ).fetchall()
        if not samples:
            raise AssertionError("No NIIN is shared by the reference and both sidecars")

        targeted_results = []
        for niin, expected_relationship_rows in samples:
            query_started = time.perf_counter()
            rows = connection.execute(
                """
                SELECT niin, cage, part_number, supply_signal, total_stock,
                       forecast_12m_qty, latest_net_price,
                       operational_source_release, active_solicitation_count,
                       next_response_deadline, next_solicitation_number
                FROM enriched
                WHERE CAST(niin AS VARCHAR) = ?
                """,
                [niin],
            ).fetchall()
            query_seconds = time.perf_counter() - query_started
            if len(rows) != expected_relationship_rows:
                raise AssertionError(
                    f"Filtered row count changed for {niin}: "
                    f"{expected_relationship_rows} -> {len(rows)}"
                )
            metric_tuples = {
                (row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10])
                for row in rows
            }
            if len(metric_tuples) != 1:
                raise AssertionError(f"NIIN metrics are not constant across relationships: {niin}")
            sidecar_metric = connection.execute(
                """
                SELECT s.supply_signal, s.total_stock, s.forecast_12m_qty,
                       p.latest_net_price, s.source_release,
                       o.active_solicitation_count, o.next_response_deadline,
                       o.next_solicitation_number
                FROM supply s LEFT JOIN price p USING (niin)
                LEFT JOIN opportunity o USING (niin)
                WHERE s.niin = ?
                """,
                [niin],
            ).fetchone()
            if next(iter(metric_tuples)) != sidecar_metric:
                raise AssertionError(f"Explorer values differ from pinned sidecars: {niin}")
            targeted_results.append(
                {
                    "niin": niin,
                    "relationship_rows": len(rows),
                    "query_seconds": round(query_seconds, 4),
                    "supply_signal": rows[0][3],
                    "forecast_12m_qty": rows[0][5],
                    "latest_net_price": float(rows[0][6]) if rows[0][6] is not None else None,
                    "active_solicitation_count": rows[0][8],
                    "next_solicitation_number": rows[0][10],
                }
            )

        connection.close()

    return {
        "status": "PASS",
        "memory_limit": memory_limit,
        "reference_rows": raw_rows,
        "enriched_rows": enriched_rows,
        "supply_rows": supply_rows,
        "price_rows": price_rows,
        "opportunity_rows": opportunity_rows,
        "full_count_seconds": round(full_count_seconds, 4),
        "total_seconds": round(time.perf_counter() - started, 4),
        "process_peak_rss_raw": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "checks": {
            "sidecars_unique_by_niin": True,
            "relationship_row_count_preserved": True,
            "metrics_constant_across_relationship_rows": True,
            "joined_metrics_equal_sidecars": True,
        },
        "targeted_queries": targeted_results,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--supply", type=Path, required=True)
    parser.add_argument("--price", type=Path, required=True)
    parser.add_argument("--opportunity", type=Path, required=True)
    parser.add_argument("--memory-limit", default="512MB")
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    report = run(
        arguments.reference,
        arguments.supply,
        arguments.price,
        arguments.opportunity,
        arguments.memory_limit,
    )
    rendered = json.dumps(report, indent=2, default=str)
    if arguments.output:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(rendered + "\n")
    print(rendered)


if __name__ == "__main__":
    main()
