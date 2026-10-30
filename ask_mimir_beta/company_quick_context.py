"""Bounded, single-scan company-site evidence for Conversation mode."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Sequence

from company_context import CompanyContextBuilder, clean_cage, rows_as_dicts
from company_conversation_index import INDEX_VERSION, usable_transaction_index
from reviewed_platform_links import LINK_VERSION, recovered_platform_sql


MAX_SITE_ROWS = 50_000


def compact_company_conversation_pack(pack: Dict[str, Any]) -> Dict[str, Any]:
    """Keep all evidence lanes, with bounded examples for a short first reply."""
    capability = pack.get("capability_evidence") or {}
    network = pack.get("reported_subcontract_relationships") or {}
    identity = pack.get("identity") or {}
    return {
        "scope": pack.get("scope"),
        "identity": {
            "sites": [
                {key: site.get(key) for key in (
                    "cage", "vendor_name", "city", "state", "official_site_label",
                    "official_capability_summary",
                ) if site.get(key)}
                for site in identity.get("sites", [])[:8]
            ],
            "site_count": identity.get("site_count"),
            "parent_resolution": identity.get("parent_resolution"),
        },
        "observed_financials": [
            {key: row.get(key) for key in ("measure_type", "net_value_usd")}
            for row in pack.get("observed_financials", [])
        ],
        "annual_financial_summary": pack.get("annual_financial_summary", []),
        "prime_awards": [
            {key: row.get(key) for key in (
                "award_key", "contract_id", "base_award_description",
                "net_prime_obligations_usd", "latest_action_date", "platform_family",
            ) if row.get(key) is not None}
            for row in pack.get("top_awards", [])[:8]
        ],
        "dla_items": [
            {key: row.get(key) for key in (
                "niin", "nsn", "description", "dla_procurement_value_usd",
                "latest_action_date",
            ) if row.get(key) is not None}
            for row in capability.get("dla_items", [])[:8]
        ],
        "reported_subcontract_routes": [
            {key: row.get(key) for key in (
                "prime_cage", "prime_name", "reported_prime_parent",
                "mimir_modelled_subcontract_value_usd", "latest_observed_date",
            ) if row.get(key) is not None}
            for row in network.get("as_subcontractor_to", [])[:8]
        ],
        "reported_subcontract_examples": [
            {key: row.get(key) for key in (
                "prime_name", "prime_contract_id", "reported_description",
                "mimir_modelled_subcontract_value_usd",
            ) if row.get(key) is not None}
            for row in capability.get("reported_subaward_descriptions", [])[:6]
        ],
        "platform_exposure": [
            {**{key: row.get(key) for key in (
                "evidence_layer", "source_system", "platform_family",
                "observed_value_usd", "latest_date",
            ) if row.get(key) is not None},
             "reported_capabilities": (row.get("reported_capabilities") or [])[:2]}
            for row in pack.get("platform_exposure", [])[:12]
        ],
        "government_buyers": [
            {key: row.get(key) for key in (
                "sub_agency", "parent_agency", "net_prime_obligations_usd",
            ) if row.get(key) is not None}
            for row in pack.get("customer_context", [])[:6]
        ],
        "scope_note": (
            "These are observed historical positions. Current notice status requires "
            "a separate search of Mimir's loaded opportunities and verification of "
            "the linked government notice before pursuit."
        ),
    }


def _date(value: Any) -> str:
    return str(value or "")[:10]


def _top(rows: Dict[Any, Dict[str, Any]], field: str, limit: int) -> list[Dict[str, Any]]:
    return sorted(
        rows.values(), key=lambda row: abs(float(row.get(field) or 0)), reverse=True
    )[:limit]


def _capture_capability(platform: Dict[str, Any], description: str, amount: float) -> None:
    description = description.strip()[:240]
    if description:
        scores = platform.setdefault("_capability_scores", {})
        scores[description] = max(scores.get(description, 0.0), abs(amount))


def build_site_conversation(
    builder: CompanyContextBuilder, cage: str, fiscal_years: Sequence[int]
) -> Dict[str, Any]:
    """Read each large source once; fall back for unusually large sites."""
    cage = clean_cage(cage)
    years = sorted({int(year) for year in fiscal_years})
    if not years:
        raise ValueError("at least one fiscal year is required")
    parent_match = None
    site_definition = None
    for parent in builder.identity.get("parents", []):
        for site in parent.get("sites", []):
            if clean_cage(site.get("cage")) == cage:
                parent_match = parent
                site_definition = site
                break
        if parent_match:
            break
    sites = builder._site_identities(
        [cage], [site_definition] if site_definition else []
    )
    if not sites:
        raise KeyError(f"CAGE was not found in the local company identity data: {cage}")
    transaction_index = usable_transaction_index(builder.paths["transactions"].parent)
    transaction_path = transaction_index or builder.paths["transactions"]
    platform_sql = (
        "raw.platform_family" if transaction_index
        else recovered_platform_sql("raw", "transactions")
    )
    transaction_rows = rows_as_dicts(builder.connection.execute(
        f"""
        SELECT source_system, award_key, transaction_key, contract_id, action_date,
               sub_agency, parent_agency, base_award_description, description,
               spend_amount, psc, year, niin, nsn,
               {platform_sql} AS platform_family
        FROM read_parquet(?) raw
        WHERE vendor_cage = ? AND year IN ({','.join('?' for _ in years)})
          AND source_system IN ('USA_SPENDING', 'DLA')
        LIMIT {MAX_SITE_ROWS + 1}
        """,
        [str(transaction_path), cage, *years],
    ))
    if len(transaction_rows) > MAX_SITE_ROWS:
        return builder.build_site(cage, years, conversation_overview=True)

    network_rows = rows_as_dicts(builder.connection.execute(
        f"""
        SELECT prime_cage, prime_name, prime_gold_parent, contract_id, award_key,
               source_dedup_key, subaward_value, subaward_value_raw,
               action_date, year, description,
               {recovered_platform_sql('raw', 'network')} AS platform_family
        FROM read_parquet(?) raw
        WHERE sub_cage = ? AND year IN ({','.join('?' for _ in years)})
        """,
        [str(builder.paths["network"]), cage, *years],
    ))

    financial: Dict[str, Dict[str, Any]] = {}
    annual: Dict[tuple[int, str], Dict[str, Any]] = {}
    awards: Dict[str, Dict[str, Any]] = {}
    items: Dict[str, Dict[str, Any]] = {}
    platforms: Dict[tuple[str, str], Dict[str, Any]] = {}
    customers: Dict[tuple[str, str], Dict[str, Any]] = {}
    for row in transaction_rows:
        source = str(row.get("source_system") or "")
        measure = "prime_obligations" if source == "USA_SPENDING" else "dla_procurement_value"
        amount = float(row.get("spend_amount") or 0)
        year = int(row["year"])
        date = _date(row.get("action_date"))
        award_key = str(row.get("award_key") or "")
        transaction_key = str(row.get("transaction_key") or "")
        lane = financial.setdefault(measure, {
            "measure_type": measure, "net_value_usd": 0.0,
            "positive_value_usd": 0.0, "deobligation_value_usd": 0.0,
            "_awards": set(), "_actions": set(), "first_action_date": "",
            "latest_action_date": "",
        })
        lane["net_value_usd"] += amount
        lane["positive_value_usd"] += max(amount, 0)
        lane["deobligation_value_usd"] += min(amount, 0)
        lane["_awards"].add(award_key)
        lane["_actions"].add(transaction_key)
        lane["first_action_date"] = min(filter(None, (lane["first_action_date"], date)), default="")
        lane["latest_action_date"] = max(lane["latest_action_date"], date)
        annual_row = annual.setdefault((year, measure), {
            "fiscal_year": year, "measure_type": measure, "net_value_usd": 0.0,
            "_awards": set(), "_actions": set(),
        })
        annual_row["net_value_usd"] += amount
        annual_row["_awards"].add(award_key)
        annual_row["_actions"].add(transaction_key)

        platform = str(row.get("platform_family") or "").strip()
        if platform and platform.upper() not in {"UNMAPPED", "UNKNOWN"}:
            key = (source, platform)
            p = platforms.setdefault(key, {
                "evidence_layer": "prime_or_dla_action", "source_system": source,
                "platform_family": platform, "observed_value_usd": 0.0,
                "_awards": set(), "contributing_sites": 1, "latest_date": "",
                "_capability_scores": {},
            })
            p["observed_value_usd"] += amount
            p["_awards"].add(award_key)
            p["latest_date"] = max(p["latest_date"], date)
            description = str(row.get("base_award_description") or row.get("description") or "").strip()
            _capture_capability(p, description, amount)

        if source == "USA_SPENDING":
            award = awards.setdefault(award_key, {
                "award_key": award_key, "contract_id": row.get("contract_id"),
                "recipient_cage": cage, "recipient_name": sites[0].get("vendor_name"),
                "base_award_description": row.get("base_award_description"),
                "net_prime_obligations_usd": 0.0, "_actions": set(),
                "first_action_date": "", "latest_action_date": "",
                "platform_family": platform, "psc": row.get("psc"),
                "public_record_url": f"https://www.usaspending.gov/award/{award_key}/",
            })
            award["net_prime_obligations_usd"] += amount
            award["_actions"].add(transaction_key)
            award["first_action_date"] = min(filter(None, (award["first_action_date"], date)), default="")
            award["latest_action_date"] = max(award["latest_action_date"], date)
            if not award["base_award_description"] and row.get("base_award_description"):
                award["base_award_description"] = row["base_award_description"]
            buyer = (str(row.get("sub_agency") or ""), str(row.get("parent_agency") or ""))
            customer = customers.setdefault(buyer, {
                "sub_agency": buyer[0], "parent_agency": buyer[1],
                "net_prime_obligations_usd": 0.0, "_awards": set(),
                "latest_action_date": "",
            })
            customer["net_prime_obligations_usd"] += amount
            customer["_awards"].add(award_key)
            customer["latest_action_date"] = max(customer["latest_action_date"], date)
        else:
            niin = str(row.get("niin") or "").zfill(9)
            if row.get("niin"):
                item = items.setdefault(niin, {
                    "niin": niin, "nsn": row.get("nsn"),
                    "description": row.get("description"),
                    "dla_procurement_value_usd": 0.0, "_awards": set(),
                    "latest_action_date": "",
                })
                item["dla_procurement_value_usd"] += amount
                item["_awards"].add(award_key)
                item["latest_action_date"] = max(item["latest_action_date"], date)

    upstream: Dict[str, Dict[str, Any]] = {}
    sub_examples: Dict[str, Dict[str, Any]] = {}
    for row in network_rows:
        amount = float(row.get("subaward_value") or 0)
        year = int(row["year"])
        date = _date(row.get("action_date"))
        annual_row = annual.setdefault((year, "mimir_modelled_reported_subcontract_value"), {
            "fiscal_year": year, "measure_type": "mimir_modelled_reported_subcontract_value",
            "net_value_usd": 0.0, "_awards": set(), "_actions": set(),
        })
        annual_row["net_value_usd"] += amount
        annual_row["_awards"].add(str(row.get("contract_id") or ""))
        annual_row["_actions"].add(str(row.get("source_dedup_key") or ""))
        prime = str(row.get("prime_cage") or "")
        route = upstream.setdefault(prime, {
            "prime_cage": prime, "prime_name": row.get("prime_name"),
            "reported_prime_parent": row.get("prime_gold_parent"),
            "mimir_modelled_subcontract_value_usd": 0.0,
            "source_reported_value_usd": 0.0, "_actions": set(),
            "selected_report_count": 0, "first_observed_date": "",
            "latest_observed_date": "",
        })
        route["mimir_modelled_subcontract_value_usd"] += amount
        route["source_reported_value_usd"] += float(row.get("subaward_value_raw") or 0)
        route["_actions"].add(str(row.get("source_dedup_key") or ""))
        route["selected_report_count"] += 1
        route["first_observed_date"] = min(filter(None, (route["first_observed_date"], date)), default="")
        route["latest_observed_date"] = max(route["latest_observed_date"], date)
        evidence_key = (prime, str(row.get("source_dedup_key") or ""))
        example = sub_examples.setdefault(evidence_key, {
            "prime_name": row.get("prime_name"), "prime_contract_id": row.get("contract_id"),
            "prime_award_key": row.get("award_key"), "reported_description": row.get("description"),
            "mimir_modelled_subcontract_value_usd": 0.0,
            "prime_award_public_url": f"https://www.usaspending.gov/award/{row.get('award_key')}/",
        })
        example["mimir_modelled_subcontract_value_usd"] += amount
        platform = str(row.get("platform_family") or "").strip()
        if platform and platform.upper() not in {"UNMAPPED", "UNKNOWN"}:
            key = ("reported_subaward", platform)
            p = platforms.setdefault(key, {
                "evidence_layer": "reported_subaward", "source_system": "USA_SPENDING_SUBAWARD",
                "platform_family": platform, "observed_value_usd": 0.0,
                "_awards": set(), "contributing_sites": 1, "latest_date": "",
                "_capability_scores": {},
            })
            p["observed_value_usd"] += amount
            p["_awards"].add(str(row.get("source_dedup_key") or ""))
            p["latest_date"] = max(p["latest_date"], date)
            description = str(row.get("description") or "").strip()
            _capture_capability(p, description, amount)

    def finish_count(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        for row in rows:
            if "_awards" in row:
                row["distinct_awards"] = len(row.pop("_awards"))
            if "_actions" in row:
                row["distinct_actions"] = len(row.pop("_actions"))
        return rows

    financial_rows = finish_count(list(financial.values()))
    annual_rows = finish_count(list(annual.values()))
    award_rows = _top(awards, "net_prime_obligations_usd", 15)
    for award in award_rows:
        award["action_count"] = len(award.pop("_actions"))
    item_rows = _top(items, "dla_procurement_value_usd", 15)
    finish_count(item_rows)
    platform_rows = _top(platforms, "observed_value_usd", 30)
    finish_count(platform_rows)
    for platform in platform_rows:
        scores = platform.pop("_capability_scores", {})
        platform["reported_capabilities"] = sorted(
            scores, key=lambda description: (-scores[description], description)
        )[:6]
    customer_rows = _top(customers, "net_prime_obligations_usd", 15)
    finish_count(customer_rows)
    upstream_rows = _top(upstream, "mimir_modelled_subcontract_value_usd", 15)
    for route in upstream_rows:
        route["selected_relationship_actions"] = len(route.pop("_actions"))
    sub_rows = _top(sub_examples, "mimir_modelled_subcontract_value_usd", 10)
    capability = {
        "psc": [], "naics": [], "dla_items": item_rows[:10],
        "prime_award_descriptions": award_rows[:10],
        "reported_subaward_descriptions": sub_rows,
        "interpretation_rule": "Observed activity supports bounded capability signals, not technical qualification for every adjacent requirement.",
    }
    source_manifest = builder._source_manifest()
    fingerprint = hashlib.sha256(json.dumps(source_manifest, sort_keys=True).encode()).hexdigest()
    scope_fingerprint = hashlib.sha256(
        "|".join(["company_site", cage, *map(str, years)]).encode()
    ).hexdigest()[:12]
    context = {
        "context_id": f"company_context_{fingerprint[:12]}_{scope_fingerprint}",
        "evidence_fingerprint": fingerprint,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "calculation_version": "mimir-company-conversation-fast-v1",
        "conversation_index_version": INDEX_VERSION if transaction_index else None,
        "scope": {
            "scope_type": "company_site", "scope_id": cage,
            "scope_name": sites[0].get("vendor_name") or f"CAGE {cage}",
            "fiscal_years": years,
            "observation_window": f"FY{years[0]}-FY{years[-1]} observed records",
        },
        "identity": {
            "sites": sites, "site_count": len(sites), "requested_cages": [cage],
            "resolved_cages": [cage],
        },
        "observed_financials": financial_rows,
        "annual_activity": sorted(annual_rows, key=lambda row: (row["fiscal_year"], row["measure_type"])),
        "site_financials": [{"cage": cage, **row} for row in financial_rows],
        "site_capability_evidence": [],
        "capability_evidence": capability,
        "platform_exposure": platform_rows,
        "platform_link_version": LINK_VERSION,
        "place_of_performance_activity": {},
        "future_demand_context": {},
        "customer_context": customer_rows,
        "reported_subcontract_relationships": {
            "as_subcontractor_to": upstream_rows,
            "reported_subcontractors": [],
            "financial_rule": "Reported subcontract value remains separate from prime obligations and DLA procurement value.",
        },
        "top_awards": award_rows,
        "evidence_index": builder._evidence_index(sites, award_rows, {}, capability),
        "quality": {"known_limitations": [
            "Current opportunity status requires a separate notice search.",
            "This Conversation overview omits detailed item competition and facility evidence.",
        ]},
        "source_manifest": source_manifest,
    }
    if parent_match:
        context["identity"]["parent_resolution"] = {
            "parent_id": parent_match["parent_id"],
            "parent_name": parent_match["parent_name"],
            "aliases": parent_match.get("aliases", []),
            "definition_version": builder.identity["version"],
            "review_status": parent_match.get("review_status"),
            "as_of_date": parent_match.get("as_of_date"),
            "sources": parent_match.get("sources", []),
            "method": "analyst-reviewed current facility bridge",
        }
    return context
