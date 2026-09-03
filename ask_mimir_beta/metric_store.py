"""Read interface for versioned metrics and their underlying action evidence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb

from derived_metrics import calculate_concentration, calculate_series_metrics, make_observation_contract
from materialize import combined_rows, peer_rank_for_scope, universe_rows_for_scope


SCOPE_COLUMNS = {
    "company_site": "vendor_cage",
    "platform": "platform_family",
    "agency": "sub_agency",
    "psc": "psc",
    "niin": "niin",
}

MEASURE_SOURCES = {
    "prime_obligations": "USA_SPENDING",
    "dla_procurement_value": "DLA",
}


class MetricStore:
    def __init__(self, release_dir: Path, transactions: Path):
        self.release_dir = release_dir.resolve()
        self.transactions = transactions.resolve()
        self.primitives = self.release_dir / "derived_metric_primitives.parquet"
        self.components_file = self.release_dir / "derived_metric_components.parquet"
        self.manifest = json.loads((self.release_dir / "manifest.json").read_text())
        self.connection = duckdb.connect()
        self.connection.execute("SET preserve_insertion_order = false")
        self.connection.execute("SET threads = 2")
        self.connection.execute("SET memory_limit = '1GB'")

    def observation(
        self,
        scope_type: str,
        scope_id: str,
        measure_type: str,
        analysis_fy: Optional[int] = None,
    ) -> Dict[str, Any]:
        clean_scope, clean_id = self._validate_scope(scope_type, scope_id)
        clean_measure = self._validate_measure(measure_type)
        rows = combined_rows(
            self.connection, self.primitives, clean_scope, clean_id, clean_measure
        )
        if not rows:
            raise KeyError(f"metric scope was not found: {clean_scope}:{clean_id}")
        selected_fy = analysis_fy or int(self.manifest["analysis_fy"])
        universe = universe_rows_for_scope(
            self.connection, self.primitives, clean_scope, clean_measure
        )
        metrics = calculate_series_metrics(rows, universe, selected_fy)
        current_rank = peer_rank_for_scope(
            self.connection,
            self.primitives,
            clean_scope,
            clean_id,
            selected_fy,
            clean_measure,
        )
        previous_rank = peer_rank_for_scope(
            self.connection,
            self.primitives,
            clean_scope,
            clean_id,
            selected_fy - 1,
            clean_measure,
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
        if clean_scope == "market":
            metrics.update(
                {
                    "observed_share_pct": None,
                    "share_change_pp": None,
                    "positive_value_share_pct": None,
                    "positive_value_share_change_pp": None,
                }
            )

        component_type = {
            "market": "supplier_site",
            "company_site": "customer",
            "platform": "supplier_site",
        }.get(clean_scope)
        metrics["concentration"] = (
            self.concentration(
                clean_scope, clean_id, component_type, selected_fy, clean_measure
            )
            if component_type
            else None
        )
        name = self.connection.execute(
            """
            SELECT MAX(scope_name) FROM read_parquet(?)
            WHERE scope_type = ? AND scope_id = ? AND measure_type = ?
            """,
            [str(self.primitives), clean_scope, clean_id, clean_measure],
        ).fetchone()[0]
        return make_observation_contract(
            release_id=str(self.manifest["release_id"]),
            scope_type=clean_scope,
            scope_id=clean_id,
            scope_name=str(name),
            measure_type=clean_measure,
            analysis_fy=selected_fy,
            source_snapshot_sha256=str(self.manifest["source"]["sha256"]),
            metrics=metrics,
            evidence_filter={
                "scope_type": clean_scope,
                "scope_id": clean_id,
                "measure_type": clean_measure,
            },
        )

    def concentration(
        self,
        scope_type: str,
        scope_id: str,
        component_type: str,
        fiscal_year: int,
        measure_type: str,
    ) -> Dict[str, Any]:
        result = self.connection.execute(
            """
            SELECT
                component_id,
                MAX(component_name) AS component_name,
                SUM(positive_value_usd) AS positive_value_usd
            FROM read_parquet(?)
            WHERE scope_type = ? AND scope_id = ?
              AND component_type = ? AND fiscal_year = ? AND measure_type = ?
            GROUP BY component_id
            """,
            [
                str(self.components_file),
                scope_type,
                scope_id,
                component_type,
                fiscal_year,
                measure_type,
            ],
        )
        columns = [description[0] for description in result.description]
        rows = [dict(zip(columns, row)) for row in result.fetchall()]
        return calculate_concentration(rows)

    def evidence(
        self,
        scope_type: str,
        scope_id: str,
        fiscal_year: int,
        measure_type: str,
        *,
        component_type: Optional[str] = None,
        component_id: Optional[str] = None,
        sign: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        clean_scope, clean_id = self._validate_scope(scope_type, scope_id)
        clean_measure = self._validate_measure(measure_type)
        where_sql, params = self._scope_predicate(clean_scope, clean_id)
        where_parts = [where_sql, "CAST(year AS INTEGER) = ?", "source_system = ?"]
        params.extend([int(fiscal_year), MEASURE_SOURCES[clean_measure]])
        if component_type or component_id:
            component_column = self._component_column(clean_scope, component_type)
            if not component_column or component_id is None:
                raise ValueError("component_type and component_id must form a valid pair")
            where_parts.append(f"COALESCE(NULLIF(TRIM({component_column}), ''), 'Unknown') = ?")
            params.append(component_id)
        if sign == "positive":
            where_parts.append("COALESCE(spend_amount, 0) > 0")
        elif sign == "negative":
            where_parts.append("COALESCE(spend_amount, 0) < 0")
        elif sign not in (None, "net"):
            raise ValueError("sign must be net, positive, negative or omitted")

        bounded_limit = min(max(int(limit), 1), 500)
        bounded_offset = max(int(offset), 0)
        query = f"""
            SELECT
                COUNT(*) OVER () AS total_records,
                source_system,
                award_key,
                transaction_key,
                contract_id,
                action_date,
                year AS fiscal_year,
                vendor_name,
                vendor_cage,
                parent_agency,
                sub_agency,
                platform_family,
                psc,
                niin,
                spend_amount
            FROM read_parquet(?)
            WHERE {' AND '.join(where_parts)}
            ORDER BY TRY_CAST(action_date AS DATE) DESC, transaction_key ASC
            LIMIT ? OFFSET ?
        """
        result = self.connection.execute(
            query,
            [str(self.transactions), *params, bounded_limit, bounded_offset],
        )
        columns = [description[0] for description in result.description]
        records = [dict(zip(columns, row)) for row in result.fetchall()]
        total = int(records[0]["total_records"]) if records else 0
        for record in records:
            record.pop("total_records", None)
        return {
            "release_id": self.manifest["release_id"],
            "source_snapshot_sha256": self.manifest["source"]["sha256"],
            "scope_type": clean_scope,
            "scope_id": clean_id,
            "measure_type": clean_measure,
            "fiscal_year": int(fiscal_year),
            "total_records": total,
            "limit": bounded_limit,
            "offset": bounded_offset,
            "records": records,
        }

    @staticmethod
    def _validate_scope(scope_type: str, scope_id: str) -> Tuple[str, str]:
        clean_scope = str(scope_type or "").strip().lower()
        clean_id = str(scope_id or "").strip()
        if clean_scope not in {"market", *SCOPE_COLUMNS}:
            raise ValueError(f"unsupported scope_type: {scope_type}")
        if clean_scope == "market":
            clean_id = "ALL"
        if not clean_id:
            raise ValueError("scope_id is required")
        return clean_scope, clean_id

    @staticmethod
    def _validate_measure(measure_type: str) -> str:
        clean_measure = str(measure_type or "").strip().lower()
        if clean_measure not in MEASURE_SOURCES:
            raise ValueError(f"unsupported measure_type: {measure_type}")
        return clean_measure

    @staticmethod
    def _scope_predicate(scope_type: str, scope_id: str) -> Tuple[str, List[Any]]:
        if scope_type == "market":
            return "TRUE", []
        column = SCOPE_COLUMNS[scope_type]
        if scope_type == "niin":
            return "LPAD(NULLIF(TRIM(niin), ''), 9, '0') = ?", [scope_id]
        return f"COALESCE(NULLIF(TRIM({column}), ''), 'UNRESOLVED') = ?", [scope_id]

    @staticmethod
    def _component_column(scope_type: str, component_type: Optional[str]) -> Optional[str]:
        return {
            ("market", "supplier_site"): "vendor_cage",
            ("company_site", "customer"): "sub_agency",
            ("company_site", "platform"): "platform_family",
            ("platform", "supplier_site"): "vendor_cage",
        }.get((scope_type, component_type))
