import hashlib
import json
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import Mock

import duckdb
from company_context import CompanyContextBuilder
from company_context_store import CompanyContextStore, LINK_VERSION, FYDP_PLATFORM_LINKAGE_VERSION


class CompanyColdFocusTests(unittest.TestCase):
    def test_focused_overview_preserves_shared_facts_without_full_traversal(self):
        builder = object.__new__(CompanyContextBuilder)
        builder._parent_action_names = ()
        builder._exact_parent_actions = False
        builder._source_manifest = Mock(return_value={"identity_definition": {"version": "v1"}, "files": {}})
        builder._focused_source_files = Mock(return_value=nullcontext())
        values = {
            "_site_identities": [{"cage": "52661", "vendor_name": "RTX", "city": "East Hartford"}],
            "_product_and_part_evidence": {
                "niin_financial_observations": [{"niin": "123456789", "description": "Part", "contract_ids": []}],
                "third_party_dla_route_summary": {"aggregate_version": "full-universe-v1"},
                "third_party_dla_procurement_routes": [],
            },
            "_capability_evidence": {"dla_items": [{"niin": "123456789", "description": "Part"}], "psc": [], "naics": [], "reported_subaward_descriptions": []},
            "_site_capability_evidence": [],
            "_top_awards": [{"award_key": "A", "contract_id": "PIID", "base_award_description": "Engine work"}],
            "_network_context": {"as_subcontractor_to": [{"prime_name": "Prime"}]},
            "_platform_exposure": [{"platform": "F-35"}],
            "_place_of_performance_activity": {"records": []},
            "_missile_program_trajectory": {},
            "_financial_summary": [{"measure_type": "prime_obligations", "net_value_usd": 100}],
            "_annual_activity": [{"fiscal_year": 2026, "measure_type": "prime_obligations", "net_value_usd": 100}],
            "_site_financials": [{"cage": "52661", "measure_type": "prime_obligations", "net_value_usd": 100}],
            "_location_footprint": {},
            "_future_demand_context": {"definition_version": FYDP_PLATFORM_LINKAGE_VERSION},
            "_customer_context": [{"customer": "Navy"}],
            "_opportunity_candidates": {},
            "_quality_summary": {"status": "reviewed"},
        }
        calls = {}
        for name, value in values.items():
            calls[name] = Mock(return_value=value)
            setattr(builder, name, calls[name])

        args = dict(scope_type="company_site", scope_id="52661", scope_name="RTX",
                    cages=["52661"], fiscal_years=[2026], site_definitions=[])
        focused = builder._build_context(**args, evidence_focus="conversation_overview")
        for name in ("_product_and_part_evidence", "_site_capability_evidence",
                     "_place_of_performance_activity", "_missile_program_trajectory",
                     "_location_footprint", "_opportunity_candidates"):
            calls[name].assert_not_called()
        full = builder._build_context(**args)
        for name in ("identity", "observed_financials", "annual_activity", "site_financials",
                     "capability_evidence", "platform_exposure", "customer_context",
                     "reported_subcontract_relationships", "top_awards", "quality"):
            self.assertEqual(focused[name], full[name], name)
        self.assertEqual(focused["evidence_index"]["record_count"],
                         full["evidence_index"]["record_count"])
        self.assertEqual(focused["platform_link_version"], LINK_VERSION)

        store = object.__new__(CompanyContextStore)
        store.get_raw = Mock(return_value=full)
        store._focused_site_raw = Mock(return_value=focused)
        short = store.get("company_site", "52661", "conversation_overview")
        detailed = store.get("company_site", "52661", "full_dossier")
        for name in ("observed_financials", "annual_activity", "capability_evidence",
                     "platform_exposure", "customer_context", "reported_subcontract_relationships",
                     "top_awards", "annual_financial_summary", "material_site_summary"):
            self.assertEqual(short[name], detailed[name], name)
        self.assertNotIn("product_and_part_evidence", short)
        self.assertIn("product_and_part_evidence", detailed)

    def test_verified_manifest_hashes_require_matching_marker_and_sizes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root / "data"
            data.mkdir()
            source = data / "transactions.parquet"
            source.write_bytes(b"verified source")
            digest = hashlib.sha256(source.read_bytes()).hexdigest()
            manifest = {"release_id": "release-test", "files": [{
                "local_path": "data/transactions.parquet", "size": source.stat().st_size,
                "sha256": digest,
            }]}
            (root / "runtime_manifest.json").write_text(json.dumps(manifest))
            fingerprint = hashlib.sha256(json.dumps(manifest, sort_keys=True,
                separators=(",", ":")).encode()).hexdigest()
            marker = root / ".verified-release.json"
            marker.write_text(json.dumps({"manifest_fingerprint": fingerprint}))
            builder = object.__new__(CompanyContextBuilder)
            builder.data_root = data.resolve()
            self.assertEqual(builder._verified_release_hashes(), {source.resolve(): digest})
            source.write_bytes(b"changed size")
            self.assertEqual(builder._verified_release_hashes(), {})

    def test_focused_source_slices_preserve_only_selected_site_and_year(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            builder = object.__new__(CompanyContextBuilder)
            builder.connection = duckdb.connect()
            builder.paths = {
                "transactions": root / "transactions.parquet",
                "network": root / "network.parquet",
            }
            original = dict(builder.paths)
            builder.connection.execute(
                "COPY (SELECT * FROM (VALUES "
                "('52661', 2026, 10), ('52661', 2025, 20), "
                "('15090', 2026, 30)) t(vendor_cage, year, spend_amount)) "
                "TO ? (FORMAT PARQUET)", [str(original["transactions"])]
            )
            builder.connection.execute(
                "COPY (SELECT * FROM (VALUES "
                "('52661', '15090', 2026), ('15090', '52661', 2026), "
                "('15090', '15090', 2026), ('52661', '15090', 2025)) "
                "t(sub_cage, prime_cage, year)) TO ? (FORMAT PARQUET)",
                [str(original["network"])]
            )
            with builder._focused_source_files(["52661"], [2026]):
                self.assertEqual(builder.connection.execute(
                    "SELECT SUM(spend_amount) FROM read_parquet(?)",
                    [str(builder.paths["transactions"])],
                ).fetchone()[0], 10)
                self.assertEqual(builder.connection.execute(
                    "SELECT COUNT(*) FROM read_parquet(?)",
                    [str(builder.paths["network"])],
                ).fetchone()[0], 2)
            self.assertEqual(builder.paths, original)


if __name__ == "__main__":
    unittest.main()
