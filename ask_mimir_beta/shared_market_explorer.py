"""Ask evidence read from the exact Market Explorer release selected by the platform root."""

from __future__ import annotations

import gzip
import json
import os
import re
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any

from market_segment import MARKET_SEGMENTS, market_segment_follow_up_intent


LEGACY_SCOPE_IDS = {
    "US_C_UAS": "US_COUNTER_UAS",
    "US_NAVAL_SHIPBUILDING": "domain:naval",
}


def _read(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise ValueError(f"Invalid Market Explorer artifact: {path.name}")
    return value


def _normal(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


class SharedMarketExplorerStore:
    """Expose mapped markets and technologies without recomputing financial scopes."""

    def __init__(self, root: Path | None = None) -> None:
        self.root = (root or Path(os.environ["ASK_MIMIR_MARKET_EXPLORER_DIR"])).resolve()
        self.pin = json.loads((self.root / "pin.json").read_text())
        self.market = _read(self.root / "public.json.gz")
        self.technology = _read(self.root / "technology-corpus.json.gz")
        release = self.pin["market_release_id"]
        main = self.pin["main_release_id"]
        for bundle in (self.market, self.technology):
            if bundle.get("candidateId") != release or bundle.get("mainRelease") != main:
                raise ValueError("Market and technology artifacts do not match the platform pin")
        overlay = json.loads(Path(__file__).with_name("market_explorer_taxonomy_overlay.json").read_text())
        if overlay.get("schema_version") != 1 or not overlay.get("source_sha256"):
            raise ValueError("Market Explorer taxonomy overlay is incomplete")
        self.pin["taxonomy_source_sha256"] = overlay["source_sha256"]
        self.nodes = {node["id"]: dict(node) for node in self.market["direct"]}
        present = set(self.nodes)
        for node in overlay["added"]:
            if (node["parent"] in present
                    and any(scope_id in present and change.get("parent") == node["id"]
                            for scope_id, change in overlay["changes"].items())):
                present.add(node["id"])
        for scope_id, change in overlay["changes"].items():
            if scope_id in self.nodes:
                self.nodes[scope_id].update({key: value for key, value in change.items()
                                             if key != "parent" or value in present})
        for node in overlay["added"]:
            if node["id"] in present:
                self.nodes[node["id"]] = node
        self.technologies = {node["id"]: node for node in self.technology["nodes"]}
        if len(self.nodes) < len(self.market["direct"]) or len(self.technologies) != len(self.technology["nodes"]):
            raise ValueError("Duplicate Market Explorer scope ID")
        self.children: dict[str, list[str]] = defaultdict(list)
        for node in self.nodes.values():
            parent = node.get("parent")
            if parent:
                if parent not in self.nodes:
                    raise ValueError(f"Unknown Market Explorer parent: {parent}")
                self.children[parent].append(node["id"])
        self._names = sorted(
            ((len(_normal(node["name"])), _normal(node["name"]), node["id"])
             for node in self.nodes.values() if node["level"] in {"market", "subsegment"}),
            reverse=True,
        )
        self._technology_names = sorted(
            ((len(_normal(node["label"])), _normal(node["label"]), node["id"])
             for node in self.technologies.values() if node.get("label")),
            reverse=True,
        )

    def resolve(self, text: str) -> str | None:
        normal = _normal(text)
        # The established query phrases remain routing aliases, not financial dossiers.
        for old_id, definition in MARKET_SEGMENTS.items():
            if re.search(definition["request_pattern"], text, re.IGNORECASE):
                candidate = LEGACY_SCOPE_IDS.get(old_id, old_id)
                if candidate in self.nodes:
                    return candidate
        for _, name, scope_id in self._names:
            if len(name) >= 9 and re.search(r"(?<!\w)" + re.escape(name) + r"(?!\w)", normal):
                return scope_id
        for _, name, technology_id in self._technology_names:
            if len(name) >= 12 and re.search(r"(?<!\w)" + re.escape(name) + r"(?!\w)", normal):
                return "TECH:" + technology_id
        return None

    def _descendants(self, scope_id: str) -> list[str]:
        result = [scope_id]
        for child in self.children.get(scope_id, ()):
            result.extend(self._descendants(child))
        return result

    def _market_totals(self, ids: list[str]) -> dict[str, dict[str, Any]]:
        amounts: dict[str, dict[str, Decimal]] = defaultdict(lambda: defaultdict(Decimal))
        records: dict[str, int] = defaultdict(int)
        for scope_id in ids:
            for source, value in self.nodes[scope_id].get("sources", {}).items():
                direct = value.get("direct", {})
                records[source] += int(direct.get("records") or 0)
                for year, amount in direct.get("byFiscalYear", {}).items():
                    amounts[source][year] += Decimal(str(amount))
        return {
            source: {
                "records": records[source],
                "by_fiscal_year_usd": {year: str(amount) for year, amount in sorted(years.items())},
                "net_usd": str(sum(years.values(), Decimal(0))),
            }
            for source, years in amounts.items()
        }

    def _details(self, scope_id: str, limit: int) -> dict[str, Any]:
        market_id = scope_id
        while self.nodes[market_id]["level"] == "subsegment":
            market_id = self.nodes[market_id]["parent"]
        if self.nodes[market_id]["level"] != "market":
            return {}
        path = self.root / "details" / f"{market_id}.json.gz"
        if not path.exists():
            return {}
        detail = _read(path)
        if detail.get("candidateId") != self.pin["market_release_id"] or detail.get("mainRelease") != self.pin["main_release_id"]:
            raise ValueError("Market Explorer detail has a different release pin")
        rows = detail.get("scopes", {}).get(scope_id, {})
        result = {"leading_records": {}, "leading_companies": {}}
        for source, group in rows.items():
            records = list(group.get("records", {}).values())
            records.sort(key=lambda item: abs(float(item.get("net") or 0)), reverse=True)
            result["leading_records"][source] = [{key: item.get(key) for key in (
                "id", "contract", "company", "companyId", "description", "net", "annual",
            )} for item in records[:limit]]
            companies: dict[str, dict[str, Any]] = {}
            for record in records:
                company_id = str(record.get("companyId") or record.get("company") or "")
                if not company_id:
                    continue
                company = companies.setdefault(company_id, {
                    "company_id": company_id, "name": record.get("company"),
                    "net_usd": Decimal(0), "record_count": 0,
                })
                company["net_usd"] += Decimal(str(record.get("net") or 0))
                company["record_count"] += 1
            result["leading_companies"][source] = [
                {**company, "net_usd": str(company["net_usd"])}
                for company in sorted(companies.values(), key=lambda item: abs(item["net_usd"]), reverse=True)[:limit]
            ]
        return result

    def _related_technologies(self, scope_ids: list[str], limit: int) -> list[dict[str, Any]]:
        matched = []
        selected = set(scope_ids)
        for node in self.technologies.values():
            buying = node.get("buyingMarkets") or {}
            source_activity = {}
            for source, markets in buying.items():
                years: dict[str, int] = defaultdict(int)
                for market_id, annual in markets.items():
                    if market_id in selected:
                        for year, entry in annual.items():
                            years[year] += int(entry["cents"])
                if years:
                    source_activity[source] = {year: str(Decimal(cents) / 100) for year, cents in sorted(years.items())}
            if source_activity:
                score = sum(abs(Decimal(amount)) for years in source_activity.values() for amount in years.values())
                matched.append((score, {"id": node["id"], "name": node["label"], "annual_activity_usd": source_activity}))
        matched.sort(key=lambda pair: pair[0], reverse=True)
        return [entry for _, entry in matched[:limit]]

    def get(self, segment_id: str, limit: int = 30) -> dict[str, Any]:
        bounded = min(max(int(limit), 1), 75)
        if segment_id.startswith("TECH:"):
            node = self.technologies.get(segment_id[5:])
            if node is None:
                raise KeyError(segment_id)
            totals = {source: {
                year: str(Decimal(entry["cents"]) / 100)
                for year, entry in annual.items()
            } for source, annual in node.get("totals", {}).items()}
            examples = {kind: rows[:min(bounded, 8)] for kind, rows in (node.get("examples") or {}).items()}
            buying_markets = []
            for market_id in {market for groups in (node.get("buyingMarkets") or {}).values() for market in groups}:
                activity = {}
                score = Decimal(0)
                for source, markets in (node.get("buyingMarkets") or {}).items():
                    annual = markets.get(market_id, {})
                    if annual:
                        activity[source] = {year: str(Decimal(entry["cents"]) / 100) for year, entry in annual.items()}
                        score += sum((abs(Decimal(entry["cents"])) for entry in annual.values()), Decimal(0))
                buying_markets.append((score, {"market_id": market_id, "annual_activity_usd": activity}))
            buying_markets.sort(key=lambda pair: pair[0], reverse=True)
            return {
                "context_type": "shared_technology_segment",
                "scope": {"segment_id": segment_id, "display_name": node["label"], "parent": node.get("parent"), "kind": node.get("kind")},
                "annual_activity_usd": totals,
                "buying_markets": [entry for _, entry in buying_markets[:12]],
                "examples": examples,
                "source_release": self.pin,
                "methodology": "Technology allocations are shown by source and fiscal year; related technologies may overlap.",
            }
        node = self.nodes.get(segment_id)
        if node is None:
            raise KeyError(segment_id)
        scope_ids = self._descendants(segment_id)
        details = self._details(segment_id, min(bounded, 12))
        return {
            "context_type": "shared_market_explorer_segment",
            "scope": {"segment_id": segment_id, "display_name": node["name"], "level": node["level"], "parent": node.get("parent")},
            "source_totals": self._market_totals(scope_ids),
            "subsegments": [{"id": child, "name": self.nodes[child]["name"], "source_totals": self._market_totals(self._descendants(child))}
                            for child in self.children.get(segment_id, ())],
            "leading_records": details.get("leading_records", {}),
            "leading_companies": details.get("leading_companies", {}),
            "related_technologies": self._related_technologies(scope_ids, 12),
            "source_release": self.pin,
            "methodology": "Signed, fiscal-year source measures from Market Explorer. Prime awards, first-tier reports and DLA awards are separate lanes. Child scopes may share records; do not sum their totals. Leading companies aggregate only the supporting detail records available for this scope, not every record in the market.",
        }


__all__ = ["SharedMarketExplorerStore", "market_segment_follow_up_intent"]
