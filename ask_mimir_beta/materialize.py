"""Build compact derived-metric primitives from Mimir's frozen action cache."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List

import duckdb

from derived_metrics import (
    calculate_concentration,
    calculate_series_metrics,
    last_completed_us_fiscal_year,
    make_observation_contract,
)
from metric_registry import CALCULATION_VERSION, data_gap_rows, registry_rows


REQUIRED_TRANSACTION_COLUMNS = {
    "source_system",
    "award_key",
    "transaction_key",
    "contract_id",
    "action_date",
    "vendor_name",
    "vendor_cage",
    "sub_agency",
    "parent_agency",
    "spend_amount",
    "psc",
    "platform_family",
    "platform_attribution_status",
    "platform_attributed_spend_amount",
    "shared_use_exposure_amount",
    "year",
    "niin",
}


def file_sha256(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def columns_for(connection: duckdb.DuckDBPyConnection, source: Path) -> set[str]:
    rows = connection.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)", [str(source)]
    ).fetchall()
    return {str(row[0]) for row in rows}


def validate_source(connection: duckdb.DuckDBPyConnection, source: Path) -> Dict[str, int]:
    missing = REQUIRED_TRANSACTION_COLUMNS - columns_for(connection, source)
    if missing:
        raise ValueError(f"transaction cache is missing required columns: {sorted(missing)}")

    row = connection.execute(
        """
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT transaction_key) AS distinct_actions,
            COUNT(*) FILTER (
                WHERE transaction_key IS NULL OR TRIM(transaction_key) = ''
            ) AS missing_action_keys,
            COUNT(*) FILTER (
                WHERE award_key IS NULL OR TRIM(award_key) = ''
            ) AS missing_award_keys
        FROM read_parquet(?)
        """,
        [str(source)],
    ).fetchone()
    result = {
        "row_count": int(row[0]),
        "distinct_actions": int(row[1]),
        "missing_action_keys": int(row[2]),
        "missing_award_keys": int(row[3]),
    }
    if result["row_count"] != result["distinct_actions"]:
        raise ValueError(
            "transaction_key is not unique; derived metrics must not proceed over duplicated actions"
        )
    if result["missing_action_keys"] or result["missing_award_keys"]:
        raise ValueError(f"authoritative key coverage failed: {result}")
    return result


def materialize_primitives(
    connection: duckdb.DuckDBPyConnection,
    source: Path,
    destination: Path,
) -> None:
    source_sql = str(source).replace("'", "''")
    destination_sql = str(destination).replace("'", "''")
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW metric_action_base AS
        SELECT
            source_system,
            CASE
                WHEN source_system = 'USA_SPENDING' THEN 'prime_obligations'
                WHEN source_system = 'DLA' THEN 'dla_procurement_value'
                ELSE LOWER(source_system)
            END AS measure_type,
            award_key,
            transaction_key,
            TRY_CAST(action_date AS DATE) AS action_date,
            CAST(year AS INTEGER) AS fiscal_year,
            COALESCE(NULLIF(TRIM(vendor_cage), ''), 'UNRESOLVED') AS vendor_cage,
            COALESCE(NULLIF(TRIM(vendor_name), ''), 'Unknown supplier') AS vendor_name,
            COALESCE(NULLIF(TRIM(sub_agency), ''), 'Unknown customer') AS sub_agency,
            COALESCE(NULLIF(TRIM(psc), ''), 'UNCLASSIFIED') AS psc,
            NULLIF(TRIM(platform_family), '') AS platform_family,
            COALESCE(NULLIF(TRIM(platform_attribution_status), ''), 'UNMAPPED')
                AS platform_attribution_status,
            CAST(COALESCE(spend_amount, 0) AS DOUBLE) AS value_usd,
            CAST(COALESCE(platform_attributed_spend_amount, 0) AS DOUBLE)
                AS attributed_value_usd,
            CAST(COALESCE(shared_use_exposure_amount, 0) AS DOUBLE)
                AS shared_use_value_usd,
            LPAD(NULLIF(TRIM(niin), ''), 9, '0') AS niin
        FROM read_parquet('{source_sql}')
        WHERE year IS NOT NULL
        """
    )

    def aggregate_sql(
        scope_type: str,
        scope_id_sql: str,
        scope_name_aggregate_sql: str,
        where_sql: str = "TRUE",
    ) -> str:
        return f"""
            SELECT
                '{scope_type}' AS scope_type,
                CAST({scope_id_sql} AS VARCHAR) AS scope_id,
                CAST({scope_name_aggregate_sql} AS VARCHAR) AS scope_name,
                measure_type,
                fiscal_year,
                CAST(SUM(value_usd) AS DOUBLE) AS net_value_usd,
                CAST(SUM(CASE WHEN value_usd > 0 THEN value_usd ELSE 0 END) AS DOUBLE)
                    AS positive_value_usd,
                CAST(SUM(CASE WHEN value_usd < 0 THEN value_usd ELSE 0 END) AS DOUBLE)
                    AS deobligation_value_usd,
                CAST(SUM(ABS(value_usd)) AS DOUBLE) AS absolute_value_usd,
                COUNT(DISTINCT award_key) AS distinct_awards,
                COUNT(DISTINCT transaction_key) AS distinct_actions,
                COUNT(*) AS record_count,
                MIN(action_date) AS first_action_date,
                MAX(action_date) AS latest_action_date,
                CAST(SUM(attributed_value_usd) AS DOUBLE) AS attributed_value_usd,
                CAST(SUM(shared_use_value_usd) AS DOUBLE) AS shared_use_value_usd,
                CAST(SUM(ABS(attributed_value_usd)) AS DOUBLE)
                    AS attributed_absolute_value_usd,
                CAST(SUM(ABS(shared_use_value_usd)) AS DOUBLE)
                    AS shared_use_absolute_value_usd,
                CAST(SUM(CASE WHEN platform_attribution_status = 'UNMAPPED'
                              THEN value_usd ELSE 0 END) AS DOUBLE) AS unmapped_value_usd,
                CAST(SUM(CASE WHEN platform_attribution_status = 'UNMAPPED'
                              THEN ABS(value_usd) ELSE 0 END) AS DOUBLE)
                    AS unmapped_absolute_value_usd,
                COUNT(*) FILTER (WHERE platform_attribution_status = 'UNMAPPED')
                    AS unmapped_record_count,
                COUNT(*) FILTER (WHERE vendor_cage = 'UNRESOLVED')
                    AS unresolved_entity_record_count,
                CAST(SUM(CASE WHEN vendor_cage = 'UNRESOLVED'
                              THEN value_usd ELSE 0 END) AS DOUBLE)
                    AS unresolved_entity_value_usd,
                CAST(SUM(CASE WHEN vendor_cage = 'UNRESOLVED'
                              THEN ABS(value_usd) ELSE 0 END) AS DOUBLE)
                    AS unresolved_entity_absolute_value_usd,
                COUNT(DISTINCT source_system) AS source_system_count,
                ARRAY_TO_STRING(
                    LIST_SORT(LIST_DISTINCT(ARRAY_AGG(source_system))),
                    ' | '
                ) AS source_systems
            FROM metric_action_base
            WHERE {where_sql}
            GROUP BY {scope_id_sql}, measure_type, fiscal_year
        """

    market_sql = aggregate_sql(
        "market",
        "'ALL'",
        "MAX('Observed defense procurement')",
    )
    connection.execute(f"CREATE OR REPLACE TEMP TABLE metric_primitives_stage AS {market_sql}")
    scope_queries = [
        aggregate_sql("company_site", "vendor_cage", "MODE(vendor_name)"),
        aggregate_sql("agency", "sub_agency", "MAX(sub_agency)"),
        aggregate_sql("psc", "psc", "MAX(psc)"),
        aggregate_sql(
            "platform",
            "platform_family",
            "MAX(platform_family)",
            "platform_family IS NOT NULL",
        ),
        aggregate_sql(
            "niin",
            "niin",
            "MAX(niin)",
            "source_system = 'DLA' AND niin IS NOT NULL",
        ),
    ]
    for query in scope_queries:
        connection.execute(f"INSERT INTO metric_primitives_stage {query}")
    connection.execute(
        f"""
        COPY metric_primitives_stage TO '{destination_sql}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )


def materialize_components(
    connection: duckdb.DuckDBPyConnection,
    source: Path,
    destination: Path,
) -> None:
    source_sql = str(source).replace("'", "''")
    destination_sql = str(destination).replace("'", "''")
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW metric_component_base AS
        SELECT
            source_system,
            CASE
                WHEN source_system = 'USA_SPENDING' THEN 'prime_obligations'
                WHEN source_system = 'DLA' THEN 'dla_procurement_value'
                ELSE LOWER(source_system)
            END AS measure_type,
            CAST(year AS INTEGER) AS fiscal_year,
            award_key,
            transaction_key,
            COALESCE(NULLIF(TRIM(vendor_cage), ''), 'UNRESOLVED') AS vendor_cage,
            COALESCE(NULLIF(TRIM(vendor_name), ''), 'Unknown supplier') AS vendor_name,
            COALESCE(NULLIF(TRIM(sub_agency), ''), 'Unknown customer') AS sub_agency,
            NULLIF(TRIM(platform_family), '') AS platform_family,
            CAST(COALESCE(spend_amount, 0) AS DOUBLE) AS value_usd
        FROM read_parquet('{source_sql}')
        WHERE year IS NOT NULL
        """
    )

    def component_sql(
        scope_type: str,
        scope_id_sql: str,
        component_type: str,
        component_id_sql: str,
        component_name_sql: str,
        where_sql: str = "TRUE",
    ) -> str:
        return f"""
            SELECT
                '{scope_type}' AS scope_type,
                CAST({scope_id_sql} AS VARCHAR) AS scope_id,
                '{component_type}' AS component_type,
                CAST({component_id_sql} AS VARCHAR) AS component_id,
                MAX(CAST({component_name_sql} AS VARCHAR)) AS component_name,
                measure_type,
                fiscal_year,
                CAST(SUM(value_usd) AS DOUBLE) AS net_value_usd,
                CAST(SUM(CASE WHEN value_usd > 0 THEN value_usd ELSE 0 END) AS DOUBLE)
                    AS positive_value_usd,
                CAST(SUM(CASE WHEN value_usd < 0 THEN value_usd ELSE 0 END) AS DOUBLE)
                    AS deobligation_value_usd,
                COUNT(DISTINCT award_key) AS distinct_awards,
                COUNT(DISTINCT transaction_key) AS distinct_actions,
                COUNT(DISTINCT source_system) AS source_system_count,
                ARRAY_TO_STRING(
                    LIST_SORT(LIST_DISTINCT(ARRAY_AGG(source_system))),
                    ' | '
                ) AS source_systems
            FROM metric_component_base
            WHERE {where_sql}
            GROUP BY {scope_id_sql}, {component_id_sql}, measure_type, fiscal_year
        """

    market_supplier_sql = component_sql(
        "market", "'ALL'", "supplier_site", "vendor_cage", "vendor_name"
    )
    connection.execute(
        f"CREATE OR REPLACE TEMP TABLE metric_components_stage AS {market_supplier_sql}"
    )
    component_queries = [
        component_sql(
            "company_site", "vendor_cage", "customer", "sub_agency", "sub_agency"
        ),
        component_sql(
            "company_site",
            "vendor_cage",
            "platform",
            "platform_family",
            "platform_family",
            "platform_family IS NOT NULL",
        ),
        component_sql(
            "platform",
            "platform_family",
            "supplier_site",
            "vendor_cage",
            "vendor_name",
            "platform_family IS NOT NULL",
        ),
    ]
    for query in component_queries:
        connection.execute(f"INSERT INTO metric_components_stage {query}")
    connection.execute(
        f"""
        COPY metric_components_stage TO '{destination_sql}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )


def combined_rows(
    connection: duckdb.DuckDBPyConnection,
    primitives: Path,
    scope_type: str,
    scope_id: str,
    measure_type: str,
) -> List[dict]:
    result = connection.execute(
        """
        SELECT
            fiscal_year,
            SUM(net_value_usd) AS net_value_usd,
            SUM(positive_value_usd) AS positive_value_usd,
            SUM(deobligation_value_usd) AS deobligation_value_usd,
            SUM(absolute_value_usd) AS absolute_value_usd,
            SUM(distinct_awards) AS distinct_awards,
            SUM(distinct_actions) AS distinct_actions,
            MIN(first_action_date) AS first_action_date,
            MAX(latest_action_date) AS latest_action_date,
            SUM(record_count) AS record_count,
            SUM(attributed_value_usd) AS attributed_value_usd,
            SUM(shared_use_value_usd) AS shared_use_value_usd,
            SUM(attributed_absolute_value_usd) AS attributed_absolute_value_usd,
            SUM(shared_use_absolute_value_usd) AS shared_use_absolute_value_usd,
            SUM(unmapped_value_usd) AS unmapped_value_usd,
            SUM(unmapped_absolute_value_usd) AS unmapped_absolute_value_usd,
            SUM(unmapped_record_count) AS unmapped_record_count,
            SUM(unresolved_entity_value_usd) AS unresolved_entity_value_usd,
            SUM(unresolved_entity_absolute_value_usd)
                AS unresolved_entity_absolute_value_usd,
            SUM(unresolved_entity_record_count) AS unresolved_entity_record_count
        FROM read_parquet(?)
        WHERE scope_type = ? AND scope_id = ? AND measure_type = ?
        GROUP BY fiscal_year
        ORDER BY fiscal_year
        """,
        [str(primitives), scope_type, scope_id, measure_type],
    )
    columns = [description[0] for description in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def universe_rows_for_scope(
    connection: duckdb.DuckDBPyConnection,
    primitives: Path,
    scope_type: str,
    measure_type: str,
) -> List[dict]:
    if scope_type in {"market", "company_site", "agency", "psc"}:
        return combined_rows(connection, primitives, "market", "ALL", measure_type)

    result = connection.execute(
        """
        SELECT
            fiscal_year,
            SUM(net_value_usd) AS net_value_usd,
            SUM(positive_value_usd) AS positive_value_usd,
            SUM(deobligation_value_usd) AS deobligation_value_usd
        FROM read_parquet(?)
        WHERE scope_type = ? AND measure_type = ?
        GROUP BY fiscal_year
        ORDER BY fiscal_year
        """,
        [str(primitives), scope_type, measure_type],
    )
    columns = [description[0] for description in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def peer_rank_for_scope(
    connection: duckdb.DuckDBPyConnection,
    primitives: Path,
    scope_type: str,
    scope_id: str,
    fiscal_year: int,
    measure_type: str,
) -> Optional[int]:
    if scope_type == "market":
        return None
    row = connection.execute(
        """
        WITH ranked AS (
            SELECT
                scope_id,
                ROW_NUMBER() OVER (
                    ORDER BY net_value_usd DESC, scope_id ASC
                ) AS peer_rank
            FROM read_parquet(?)
            WHERE scope_type = ? AND fiscal_year = ? AND measure_type = ?
        )
        SELECT peer_rank FROM ranked WHERE scope_id = ?
        """,
        [str(primitives), scope_type, fiscal_year, measure_type, scope_id],
    ).fetchone()
    return int(row[0]) if row else None


def build_sample_observations(
    connection: duckdb.DuckDBPyConnection,
    primitives: Path,
    components: Path,
    output: Path,
    release_id: str,
    source_sha256: str,
    scopes: Iterable[tuple[str, str]],
    as_of_fy: int,
) -> None:
    contracts: List[dict] = []
    for scope_type, scope_id in scopes:
        measure_type = "dla_procurement_value" if scope_type == "niin" else "prime_obligations"
        rows = combined_rows(connection, primitives, scope_type, scope_id, measure_type)
        if not rows:
            continue
        universe_rows = universe_rows_for_scope(
            connection, primitives, scope_type, measure_type
        )
        scope_name = connection.execute(
            """
            SELECT MAX(scope_name) FROM read_parquet(?)
            WHERE scope_type = ? AND scope_id = ? AND measure_type = ?
            """,
            [str(primitives), scope_type, scope_id, measure_type],
        ).fetchone()[0]
        metrics = calculate_series_metrics(rows, universe_rows, as_of_fy)
        current_rank = peer_rank_for_scope(
            connection, primitives, scope_type, scope_id, as_of_fy, measure_type
        )
        previous_rank = peer_rank_for_scope(
            connection, primitives, scope_type, scope_id, as_of_fy - 1, measure_type
        )
        metrics["peer_rank"] = {
            "current": current_rank,
            "previous": previous_rank,
            "change": (
                previous_rank - current_rank
                if current_rank is not None and previous_rank is not None
                else None
            ),
        }
        if scope_type == "market":
            metrics["observed_share_pct"] = None
            metrics["share_change_pp"] = None
            metrics["positive_value_share_pct"] = None
            metrics["positive_value_share_change_pp"] = None
        component_type = {
            "market": "supplier_site",
            "company_site": "customer",
            "platform": "supplier_site",
        }.get(scope_type)
        concentration = None
        if component_type:
            component_result = connection.execute(
                """
                SELECT component_id, MAX(component_name) AS component_name,
                       SUM(positive_value_usd) AS positive_value_usd
                FROM read_parquet(?)
                WHERE scope_type = ? AND scope_id = ?
                  AND component_type = ? AND fiscal_year = ? AND measure_type = ?
                GROUP BY component_id
                """,
                [
                    str(components),
                    scope_type,
                    scope_id,
                    component_type,
                    as_of_fy,
                    measure_type,
                ],
            )
            names = [description[0] for description in component_result.description]
            component_rows = [dict(zip(names, row)) for row in component_result.fetchall()]
            concentration = calculate_concentration(component_rows)
        metrics["concentration"] = concentration
        contracts.append(
            make_observation_contract(
                release_id=release_id,
                scope_type=scope_type,
                scope_id=scope_id,
                scope_name=str(scope_name),
                measure_type=measure_type,
                analysis_fy=as_of_fy,
                source_snapshot_sha256=source_sha256,
                metrics=metrics,
                evidence_filter={
                    "scope_type": scope_type,
                    "scope_id": scope_id,
                    "measure_type": measure_type,
                },
            )
        )
    output.write_text(json.dumps(contracts, indent=2, default=str) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--transactions", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--analysis-fy", type=int)
    parser.add_argument(
        "--sample-scope",
        action="append",
        default=[],
        help="Scope to validate, formatted as scope_type:scope_id",
    )
    args = parser.parse_args()
    source = args.transactions.resolve()
    if not source.exists():
        raise SystemExit(f"missing transaction cache: {source}")
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    source_sha = file_sha256(source)
    generated = datetime.now(timezone.utc)
    release_id = f"derived-{generated.strftime('%Y%m%dT%H%M%SZ')}-{source_sha[:12]}"
    analysis_fy = args.analysis_fy or last_completed_us_fiscal_year(date.today())
    connection = duckdb.connect()
    connection.execute("SET preserve_insertion_order = false")
    connection.execute("SET threads = 4")
    connection.execute("SET memory_limit = '4GB'")
    temp_directory = output_dir / f"duckdb_tmp_{generated.strftime('%Y%m%dT%H%M%SZ')}"
    connection.execute(f"SET temp_directory = '{str(temp_directory)}';")

    validation = validate_source(connection, source)
    primitives = output_dir / "derived_metric_primitives.parquet"
    components = output_dir / "derived_metric_components.parquet"
    materialize_primitives(connection, source, primitives)
    materialize_components(connection, source, components)

    sample_scopes = [("market", "ALL")]
    for raw_scope in args.sample_scope:
        scope_type, separator, scope_id = raw_scope.partition(":")
        if not separator or not scope_id:
            raise SystemExit(f"invalid --sample-scope: {raw_scope}")
        sample_scopes.append((scope_type, scope_id))
    observations = output_dir / "sample_metric_observations.json"
    build_sample_observations(
        connection,
        primitives,
        components,
        observations,
        release_id,
        source_sha,
        sample_scopes,
        analysis_fy,
    )

    manifest = {
        "release_id": release_id,
        "calculation_version": CALCULATION_VERSION,
        "generated_at": generated.isoformat(),
        "analysis_fy": analysis_fy,
        "source": {
            "path": str(source),
            "sha256": source_sha,
            **validation,
        },
        "outputs": {
            "primitives": {
                "path": str(primitives),
                "sha256": file_sha256(primitives),
            },
            "components": {
                "path": str(components),
                "sha256": file_sha256(components),
            },
            "sample_observations": str(observations),
        },
        "metric_registry": registry_rows(),
        "material_data_gaps": data_gap_rows(),
        "limitations": [
            "The current action cache begins in FY2021; T05 five-year CAGR is not released.",
            "Corporate-parent metrics are not released until resolved parent IDs are retained in the action cache.",
            "Competition metrics are not released until competition and offers fields are retained in the action cache.",
            "Prime, DLA procurement and subcontract values remain separately identified and are not presented as one market total.",
        ],
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({"release_id": release_id, "output_dir": str(output_dir), **validation}, indent=2))


if __name__ == "__main__":
    main()
