"""Read compact platform supply-chain answer packs for Ask Mimir."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parent
DEFAULT_PACK_DIR = ROOT / "validation-output" / "platform-supply-chains"


class PlatformSupplyChainStore:
    def __init__(self, pack_dir: Path = DEFAULT_PACK_DIR) -> None:
        self.pack_dir = pack_dir.resolve()
        manifest_path = self.pack_dir / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(
                f"platform supply-chain manifest was not found: {manifest_path}"
            )
        self.manifest = json.loads(manifest_path.read_text())
        self.packs: Dict[str, Dict[str, Any]] = {}
        for entry in self.manifest.get("packs", []):
            self.packs[entry["platform_id"].upper()] = json.loads(
                (self.pack_dir / entry["path"]).read_text()
            )

    def get(
        self,
        platform_id: str,
        capability_filter: str | None,
        supplier_limit: int,
    ) -> Dict[str, Any]:
        clean_id = str(platform_id).strip().upper()
        pack = self.packs.get(clean_id)
        if pack is None:
            raise KeyError(f"platform supply-chain pack was not found: {platform_id}")

        limit = min(max(int(supplier_limit), 1), 12)
        capability = str(capability_filter or "").strip().upper()
        verified = pack["component_verified_suppliers"]
        reported = pack["reported_first_tier_supplier_sites"]
        if capability:
            verified = [
                row
                for row in verified
                if capability in " ".join(row.get("component_roles", [])).upper()
            ]
            verified_cages = {
                cage for row in verified for cage in row.get("reported_site_cages", [])
            }
            reported = [row for row in reported if row.get("cage") in verified_cages]

        top_family_items = pack["broader_ch53_family"]["top_items"][:6]
        top_family_keys = {
            (row.get("platform_family"), row.get("niin"))
            for row in top_family_items
        }
        family_relationships = [
            row
            for row in pack["broader_ch53_family"].get(
                "supplier_part_relationships", []
            )
            if (row.get("platform_family"), row.get("niin")) in top_family_keys
        ][:30]

        return {
            "answer_pack_id": pack["answer_pack_id"],
            "calculation_version": pack["calculation_version"],
            "generated_at": pack["generated_at"],
            "scope": pack["scope"],
            "answer_contract": pack["answer_contract"],
            "coverage": pack["coverage"],
            "platform_prime_contractors": [
                self._compact_prime_recipient(row)
                for row in pack["platform_prime_contractors"][:4]
            ],
            "other_direct_prime_recipients": [
                self._compact_prime_recipient(row)
                for row in pack["other_direct_prime_recipients"][:5]
            ],
            "reported_first_tier_supplier_sites": [
                self._compact_reported_site(row) for row in reported[:limit]
            ],
            "capability_supported_first_tier_suppliers": [
                self._compact_capability_evidence(row)
                for row in pack["capability_supported_first_tier_suppliers"][:limit]
            ],
            "component_verified_suppliers": [
                self._compact_verified_supplier(row) for row in verified[:limit]
            ],
            "supplier_site_summary": pack.get("supplier_site_summary", []),
            "broader_ch53_family": {
                "status": pack["broader_ch53_family"]["status"],
                "coverage": pack["broader_ch53_family"]["coverage"],
                "top_items": top_family_items,
                "supplier_part_relationships": family_relationships,
            },
            "known_configuration_exclusions": [
                self._compact_configuration_exclusion(row)
                for row in pack["known_configuration_exclusions"]
            ],
            "dla_component_evidence": pack["dla_component_evidence"],
            "quality": pack["quality"],
        }

    @staticmethod
    def _compact_prime_recipient(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            key: value for key, value in row.items() if key != "sample_award_keys"
        } | {
            "sample_contract_ids": row.get("sample_contract_ids", [])[:3],
            "sample_award_descriptions": row.get("sample_award_descriptions", [])[:2],
        }

    @staticmethod
    def _compact_reported_site(row: Dict[str, Any]) -> Dict[str, Any]:
        result = {
            key: value
            for key, value in row.items()
            if key
            not in {
                "reported_location_variants",
                "sample_prime_contract_ids",
                "sample_source_report_ids",
                "reported_descriptions",
                "capability_evidence",
            }
        }
        result["sample_prime_contract_ids"] = row.get("sample_prime_contract_ids", [])[:3]
        result["reported_descriptions"] = row.get("reported_descriptions", [])[:1]
        result["capability_evidence"] = [
            PlatformSupplyChainStore._compact_capability_evidence(item)
            for item in row.get("capability_evidence", [])[:4]
        ]
        variants = row.get("reported_location_variants", [])
        if len(variants) > 1:
            result["reported_location_variants"] = variants
        return result

    @classmethod
    def _compact_verified_supplier(cls, row: Dict[str, Any]) -> Dict[str, Any]:
        def compact_attribution(attribution: Dict[str, Any]) -> Dict[str, Any]:
            return {
                key: value
                for key, value in attribution.items()
                if key != "confidence"
            }

        def compact_site(site: Dict[str, Any]) -> Dict[str, Any]:
            retained = {
                "cage",
                "supplier_name",
                "city",
                "state",
                "country",
                "location_quality",
                "observed_place_of_performance_locations",
                "mimir_modelled_subcontract_value_usd",
                "selected_report_count",
                "prime_award_count",
                "first_reported_date",
                "latest_reported_date",
                "net_prime_obligations_usd",
                "action_count",
                "award_count",
                "first_action_date",
                "latest_action_date",
            }
            result = {key: value for key, value in site.items() if key in retained}
            result["sample_prime_contract_ids"] = site.get(
                "sample_prime_contract_ids", site.get("sample_contract_ids", [])
            )[:3]
            return result

        return {
            "supplier_id": row["supplier_id"],
            "display_name": row["display_name"],
            "reported_site_cages": row["reported_site_cages"],
            "linked_direct_award_cages": row.get("linked_direct_award_cages", []),
            "component_roles": row["component_roles"],
            "site_attribution": row["site_attribution"],
            "site_role_attributions": [
                compact_attribution(attribution)
                for attribution in row.get("site_role_attributions", [])
            ],
            "relationship_context": row.get("relationship_context", []),
            "sources": row["sources"],
            "reported_subaward_site_evidence": [
                compact_site(site)
                for site in row.get("reported_subaward_site_evidence", [])
            ],
            "prime_recipient_site_evidence": [
                compact_site(site)
                for site in row.get("prime_recipient_site_evidence", [])
            ],
            "proof_status": row["proof_status"],
        }

    @staticmethod
    def _compact_capability_evidence(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            key: value
            for key, value in row.items()
            if key
            not in {
                "internal_source_report_ids",
                "source_report_id",
                "source_dedup_key",
            }
        } | {
            "sample_prime_contract_ids": row.get("sample_prime_contract_ids", [])[:3],
            "source_descriptions": row.get("source_descriptions", [])[:3],
            "prime_award_context": row.get("prime_award_context", [])[:3],
        }

    @staticmethod
    def _compact_configuration_exclusion(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            key: value
            for key, value in row.items()
            if key not in {"internal_source_report_ids"}
        }
