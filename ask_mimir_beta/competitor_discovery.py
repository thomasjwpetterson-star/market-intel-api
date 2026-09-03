"""Evidence-led competitor discovery for named company scopes."""

from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

import duckdb


ROOT = Path(__file__).resolve().parent
DEFAULT_DATA_ROOT = Path(
    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data"
)


def _rows(cursor: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _split_values(value: Any) -> set[str]:
    return {
        item.strip()
        for item in re.split(r"[,|]", str(value or ""))
        if item.strip() and item.strip().upper() not in {"NONE", "UNMAPPED"}
    }


def _compact_log(value: float, maximum: float) -> float:
    return math.log1p(max(value, 0)) / math.log1p(max(maximum, 1))


class CompetitorDiscoveryStore:
    def __init__(
        self,
        data_root: Path = DEFAULT_DATA_ROOT,
        definitions_path: Path = ROOT / "competitor_discovery_definitions.json",
    ) -> None:
        self.data_root = data_root.resolve()
        self.definitions_path = definitions_path.resolve()
        self.definitions = json.loads(self.definitions_path.read_text())
        self.paths = {
            "profiles": self.data_root / "profiles.parquet",
            "references": self.data_root / "nsn_cage_reference.parquet",
            "transactions": self.data_root / "transactions.parquet",
            "network": self.data_root / "network.parquet",
            "contracts": self.data_root / "contracts_rolled.parquet",
            "locations": self.data_root / "cage_locations.parquet",
        }
        missing = [str(path) for path in self.paths.values() if not path.exists()]
        if missing:
            raise FileNotFoundError(f"competitor-discovery sources are missing: {missing}")
        self.connection = duckdb.connect()
        self.connection.execute("SET preserve_insertion_order=false")
        self.connection.execute("SET threads=2")
        self.connection.execute("SET memory_limit='1GB'")
        self._cache: Dict[str, Dict[str, Any]] = {}

    def get(self, target_id: str = "eaton_aerospace", limit: int = 15) -> Dict[str, Any]:
        clean_id = str(target_id).strip().lower()
        definition = self.definitions["targets"].get(clean_id)
        if not definition:
            raise KeyError(f"competitor-discovery target was not found: {target_id}")
        if clean_id not in self._cache:
            self._cache[clean_id] = self._build(clean_id, definition)
        pack = self._cache[clean_id]
        bounded = min(max(int(limit), 1), 30)
        return {**pack, "observed_peers": pack["observed_peers"][:bounded]}

    def _target_sites(self, definition: Dict[str, Any]) -> List[Dict[str, Any]]:
        return _rows(
            self.connection.execute(
                """
                SELECT p.cage_code cage, MAX(p.vendor_name) supplier_name,
                       MAX(l.city) city, MAX(l.state) state,
                       MAX(p.top_platforms) top_platforms
                FROM read_parquet(?) p
                LEFT JOIN read_parquet(?) l USING(cage_code)
                WHERE REGEXP_MATCHES(UPPER(p.vendor_name), ?)
                GROUP BY p.cage_code
                ORDER BY p.cage_code
                """,
                [
                    str(self.paths["profiles"]),
                    str(self.paths["locations"]),
                    definition["site_name_pattern"],
                ],
            )
        )

    def _relationship_rows(self, cages: List[str]) -> List[Dict[str, Any]]:
        placeholders = ",".join("?" for _ in cages)
        return _rows(
            self.connection.execute(
                f"""
                WITH target AS (
                    SELECT niin,
                           BOOL_OR(COALESCE(is_active_authorized_source,false)) target_active_authorized,
                           BOOL_OR(COALESCE(has_observed_revenue,false)) target_observed,
                           MAX(description) description,
                           MAX(platform_families) platform_families,
                           MAX(platform_family) platform_family,
                           COUNT(DISTINCT cage) target_site_count
                    FROM read_parquet(?)
                    WHERE cage IN ({placeholders})
                      AND (COALESCE(is_procurement_authorized,false)
                           OR COALESCE(has_observed_revenue,false))
                    GROUP BY niin
                )
                SELECT r.niin, r.cage, r.vendor_name, t.description,
                       t.platform_families, t.platform_family, t.target_site_count,
                       t.target_active_authorized, t.target_observed,
                       COALESCE(r.is_active_authorized_source,false) peer_active_authorized,
                       COALESCE(r.has_observed_revenue,false) peer_observed,
                       COALESCE(r.observed_spend,0) peer_observed_spend,
                       r.supplier_status
                FROM read_parquet(?) r
                JOIN target t USING(niin)
                WHERE r.cage IS NOT NULL AND TRIM(r.cage) <> ''
                  AND r.cage NOT IN ({placeholders})
                """,
                [
                    str(self.paths["references"]),
                    *cages,
                    str(self.paths["references"]),
                    *cages,
                ],
            )
        )

    def _target_context(self, cages: List[str]) -> tuple[set[str], set[str]]:
        placeholders = ",".join("?" for _ in cages)
        rows = _rows(
            self.connection.execute(
                f"""
                SELECT platform_family platform, sub_agency customer
                FROM read_parquet(?)
                WHERE vendor_cage IN ({placeholders}) AND year BETWEEN 2021 AND 2025
                UNION ALL
                SELECT platform_family platform, prime_name customer
                FROM read_parquet(?)
                WHERE sub_cage IN ({placeholders}) AND year BETWEEN 2021 AND 2025
                """,
                [
                    str(self.paths["transactions"]),
                    *cages,
                    str(self.paths["network"]),
                    *cages,
                ],
            )
        )
        platforms = {str(row["platform"]).strip() for row in rows if row["platform"] and str(row["platform"]).upper() not in {"NONE", "UNMAPPED"}}
        customers = {str(row["customer"]).strip() for row in rows if row["customer"]}
        return platforms, customers

    def _peer_context(self, cages: List[str]) -> Dict[str, Dict[str, Any]]:
        if not cages:
            return {}
        placeholders = ",".join("?" for _ in cages)
        context: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"platforms": set(), "customers": set()})
        rows = _rows(
            self.connection.execute(
                f"""
                SELECT vendor_cage cage, platform_family platform, sub_agency customer
                FROM read_parquet(?)
                WHERE vendor_cage IN ({placeholders}) AND year BETWEEN 2021 AND 2025
                UNION ALL
                SELECT sub_cage cage, platform_family platform, prime_name customer
                FROM read_parquet(?)
                WHERE sub_cage IN ({placeholders}) AND year BETWEEN 2021 AND 2025
                """,
                [
                    str(self.paths["transactions"]),
                    *cages,
                    str(self.paths["network"]),
                    *cages,
                ],
            )
        )
        for row in rows:
            cage = str(row["cage"])
            platform = str(row["platform"] or "").strip()
            customer = str(row["customer"] or "").strip()
            if platform and platform.upper() not in {"NONE", "UNMAPPED"}:
                context[cage]["platforms"].add(platform)
            if customer:
                context[cage]["customers"].add(customer)
        locations = _rows(
            self.connection.execute(
                f"""
                SELECT cage_code cage, MAX(vendor_name) location_vendor_name,
                       MAX(city) city, MAX(state) state, MAX(location_quality) location_quality
                FROM read_parquet(?) WHERE cage_code IN ({placeholders}) GROUP BY cage_code
                """,
                [str(self.paths["locations"]), *cages],
            )
        )
        for row in locations:
            context[str(row["cage"])].update(row)
        return context

    def _sample_awards(self, cages: List[str], target_cages: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        if not cages:
            return {}
        candidate_placeholders = ",".join("?" for _ in cages)
        target_placeholders = ",".join("?" for _ in target_cages)
        rows = _rows(
            self.connection.execute(
                f"""
                WITH target_scope AS (
                    SELECT DISTINCT psc, sub_agency FROM read_parquet(?)
                    WHERE vendor_cage IN ({target_placeholders})
                      AND year BETWEEN 2021 AND 2025 AND psc IS NOT NULL
                ), ranked AS (
                    SELECT c.vendor_cage cage, c.contract_id,
                           c.base_award_description description, c.psc, c.sub_agency,
                           c.total_spend,
                           ROW_NUMBER() OVER (
                               PARTITION BY c.vendor_cage ORDER BY ABS(c.total_spend) DESC NULLS LAST
                           ) rn
                    FROM read_parquet(?) c
                    WHERE c.vendor_cage IN ({candidate_placeholders})
                      AND c.year BETWEEN 2021 AND 2025
                      AND EXISTS (
                          SELECT 1 FROM target_scope t
                          WHERE t.psc=c.psc AND t.sub_agency=c.sub_agency
                      )
                )
                SELECT * FROM ranked WHERE rn <= 3
                """,
                [
                    str(self.paths["contracts"]),
                    *target_cages,
                    str(self.paths["contracts"]),
                    *cages,
                ],
            )
        )
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in rows:
            row.pop("rn", None)
            grouped[str(row.pop("cage"))].append(row)
        return grouped

    def _capabilities(self, descriptions: Iterable[str]) -> List[str]:
        text = " | ".join(descriptions).upper()
        return [
            name
            for name, pattern in self.definitions["capability_groups"].items()
            if re.search(pattern, text)
        ]

    def _build(self, target_id: str, definition: Dict[str, Any]) -> Dict[str, Any]:
        sites = self._target_sites(definition)
        target_cages = [row["cage"] for row in sites]
        relations = self._relationship_rows(target_cages)
        non_commercial = re.compile(self.definitions["non_commercial_name_pattern"])
        affiliate = re.compile(definition["affiliate_name_pattern"])
        grouped: Dict[str, Dict[str, Any]] = {}
        excluded_affiliates: set[str] = set()
        excluded_non_commercial: set[str] = set()
        for row in relations:
            cage = str(row["cage"])
            name = str(row.get("vendor_name") or "Unknown supplier")
            if affiliate.search(name.upper()):
                excluded_affiliates.add(cage)
                continue
            if non_commercial.search(name.upper()):
                excluded_non_commercial.add(cage)
                continue
            peer = grouped.setdefault(
                cage,
                {
                    "cage": cage,
                    "supplier_name": name,
                    "shared_niins": set(),
                    "shared_active_authorized_niins": set(),
                    "shared_observed_niins": set(),
                    "descriptions": set(),
                    "platforms_from_shared_items": set(),
                    "peer_observed_spend_on_shared_niins_usd": 0.0,
                },
            )
            niin = str(row["niin"])
            peer["shared_niins"].add(niin)
            if row["target_active_authorized"] and row["peer_active_authorized"]:
                peer["shared_active_authorized_niins"].add(niin)
            if row["target_observed"] and row["peer_observed"]:
                peer["shared_observed_niins"].add(niin)
            if row.get("description"):
                peer["descriptions"].add(str(row["description"]))
            peer["platforms_from_shared_items"].update(_split_values(row.get("platform_families")))
            peer["platforms_from_shared_items"].update(_split_values(row.get("platform_family")))
            peer["peer_observed_spend_on_shared_niins_usd"] += float(row.get("peer_observed_spend") or 0)

        preliminary = sorted(
            grouped.values(),
            key=lambda row: (
                -len(row["shared_active_authorized_niins"]),
                -len(row["shared_observed_niins"]),
                -len(row["shared_niins"]),
            ),
        )[:80]
        target_platforms, target_customers = self._target_context(target_cages)
        peer_context = self._peer_context([row["cage"] for row in preliminary])
        awards = self._sample_awards([row["cage"] for row in preliminary[:30]], target_cages)
        max_exact = max((len(row["shared_niins"]) for row in preliminary), default=1)
        max_auth = max((len(row["shared_active_authorized_niins"]) for row in preliminary), default=1)
        max_observed = max((len(row["shared_observed_niins"]) for row in preliminary), default=1)

        peers = []
        for row in preliminary:
            context = peer_context.get(row["cage"], {})
            platforms = set(context.get("platforms", set()))
            customers = set(context.get("customers", set()))
            shared_platforms = sorted(platforms & target_platforms)
            shared_customers = sorted(customers & target_customers)
            capabilities = self._capabilities(row["descriptions"])
            exact = len(row["shared_niins"])
            active = len(row["shared_active_authorized_niins"])
            observed = len(row["shared_observed_niins"])
            score = (
                (exact_points := 35 * _compact_log(exact, max_exact))
                + (authorized_points := 30 * _compact_log(active, max_auth))
                + (observed_points := 15 * _compact_log(observed, max_observed))
                + (platform_points := 10 * min(len(shared_platforms) / max(len(target_platforms), 1), 1))
                + (customer_points := 5 * min(len(shared_customers) / max(len(target_customers), 1), 1))
                + (capability_points := 5 * min(len(capabilities) / max(len(self.definitions["capability_groups"]), 1), 1))
            )
            peers.append(
                {
                    "cage": row["cage"],
                    "supplier_name": row["supplier_name"],
                    "city": context.get("city"),
                    "state": context.get("state"),
                    "location_quality": context.get("location_quality"),
                    "observed_competitor_score": round(score, 2),
                    "score_components": {
                        "exact_niin_overlap_points": round(exact_points, 2),
                        "shared_active_authorized_points": round(authorized_points, 2),
                        "shared_observed_procurement_points": round(observed_points, 2),
                        "platform_overlap_points": round(platform_points, 2),
                        "customer_overlap_points": round(customer_points, 2),
                        "capability_overlap_points": round(capability_points, 2),
                    },
                    "shared_exact_niin_count": exact,
                    "shared_active_authorized_niin_count": active,
                    "shared_observed_procurement_niin_count": observed,
                    "shared_platforms": shared_platforms,
                    "shared_customers": shared_customers[:12],
                    "overlapping_capability_groups": capabilities,
                    "sample_shared_niins": sorted(row["shared_niins"])[:12],
                    "sample_shared_item_descriptions": sorted(row["descriptions"])[:8],
                    "peer_observed_spend_on_shared_niins_usd": round(row["peer_observed_spend_on_shared_niins_usd"], 2),
                    "comparable_award_examples": awards.get(row["cage"], []),
                    "interpretation": (
                        "strong exact-item peer evidence"
                        if active >= 10 or observed >= 10
                        else "moderate exact-item peer evidence"
                        if active >= 3 or observed >= 3
                        else "adjacent item and market overlap"
                    ),
                }
            )
        peers.sort(
            key=lambda row: (-row["observed_competitor_score"], row["supplier_name"], row["cage"])
        )
        for index, row in enumerate(peers, start=1):
            row["rank"] = index

        return {
            "context_type": "observed_competitor_discovery",
            "calculation_version": "mimir-competitor-discovery-2026-09-v1",
            "definition_version": self.definitions["definition_version"],
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "scope": {
                "target_id": target_id,
                "display_name": definition["display_name"],
                "target_cages": target_cages,
                "observation_window": definition["observation_window"],
                "scope_note": definition["scope_note"],
            },
            "target_sites": sites,
            "target_context": {
                "observed_platforms": sorted(target_platforms),
                "observed_customers": sorted(target_customers),
            },
            "observed_peers": peers,
            "coverage": {
                "candidate_sites_before_role_exclusions": len(grouped) + len(excluded_affiliates) + len(excluded_non_commercial),
                "ranked_commercial_candidate_sites": len(peers),
                "excluded_affiliate_sites": len(excluded_affiliates),
                "excluded_government_or_standard_reference_sites": len(excluded_non_commercial),
            },
            "methodology": {
                "peer_universe": "Non-affiliated commercial CAGE sites sharing at least one authorized or observed NIIN relationship with the target sites.",
                "ranking": "Exact NIIN overlap, shared active-authorized NIINs, shared observed-procurement NIINs, platform overlap, customer overlap and capability-group overlap are scored separately and combined.",
                "role_rule": "The ranking identifies observed peers. It does not establish that every peer bid against Eaton or serves the same tier or commercial role.",
                "award_rule": "Comparable award examples share target PSC and customer context; they are not proof of direct competition for the same award.",
                "financial_rule": "Observed procurement values are supporting activity within shared NIINs and are not market share or company revenue.",
            },
        }
