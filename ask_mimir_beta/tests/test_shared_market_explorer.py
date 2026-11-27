import gzip
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from shared_market_explorer import SCOPE_GATES, SharedMarketExplorerStore
from lab_test_support import load_lab


class SharedMarketExplorerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        (root / "details").mkdir()
        (root / "pin.json").write_text(json.dumps({
            "market_release_id": "market-test", "main_release_id": "main-test",
        }))
        market = {
            "candidateId": "market-test", "mainRelease": "main-test",
            "direct": [
                {"id": "US", "name": "US", "level": "root", "parent": None, "sources": {}},
                {"id": "US_COUNTER_UAS", "name": "Counter-UAS", "level": "market", "parent": "US",
                 "sources": {"PRIME_AWARD": {"direct": {"records": 1, "byFiscalYear": {"2026": "-4.00"}}}}},
                {"id": "CUAS_DETECTION", "name": "Detection and tracking systems", "level": "subsegment",
                 "parent": "US_COUNTER_UAS", "sources": {"PRIME_AWARD": {"direct": {"records": 1, "byFiscalYear": {"2027": "12.50"}}}}},
            ],
        }
        technology = {
            "candidateId": "market-test", "mainRelease": "main-test",
            "nodes": [{"id": "radar.detection", "kind": "product", "label": "Radar detection systems",
                       "parent": None, "totals": {"PRIME_AWARD": {"2027": {"cents": "1250"}}},
                       "buyingMarkets": {"PRIME_AWARD": {"CUAS_DETECTION": {"2027": {"cents": "1250"}}}},
                       "examples": {"contracts": [{"name": "FA873015C0002", "cents": "1250"}]}}],
        }
        with gzip.open(root / "public.json.gz", "wt") as stream:
            json.dump(market, stream)
        with gzip.open(root / "technology-corpus.json.gz", "wt") as stream:
            json.dump(technology, stream)
        self.root = root

    def test_market_uses_signed_fiscal_totals_and_shared_scope(self):
        store = SharedMarketExplorerStore(self.root)
        self.assertEqual(store.resolve("What is the US C-UAS market?"), "US_COUNTER_UAS")
        self.assertEqual(store.resolve("Tell me about detection and tracking systems"), "CUAS_DETECTION")
        pack = store.get("US_COUNTER_UAS")
        self.assertEqual(store.get("US_C_UAS")["scope"]["segment_id"], "US_COUNTER_UAS")
        self.assertEqual(pack["source_totals"]["PRIME_AWARD"]["net_usd"], "8.50")
        self.assertEqual(pack["source_totals"]["PRIME_AWARD"]["by_fiscal_year_usd"],
                         {"2026": "-4.00", "2027": "12.50"})
        self.assertEqual(pack["related_technologies"][0]["id"], "radar.detection")

    def test_technology_segment_uses_same_corpus(self):
        store = SharedMarketExplorerStore(self.root)
        scope = store.resolve("Who supplies radar detection systems?")
        self.assertEqual(scope, "TECH:radar.detection")
        pack = store.get(scope)
        self.assertEqual(pack["annual_activity_usd"]["PRIME_AWARD"]["2027"], "12.5")
        self.assertEqual(pack["buying_markets"][0]["market_id"], "CUAS_DETECTION")
        self.assertEqual(pack["examples"]["contracts"][0]["mimir_url"],
                         "https://www.mimiradvisors.org/dashboard?view=AWARDS&award=FA873015C0002")

    def test_native_activity_uses_verified_scope_union_instead_of_child_sum(self):
        path = self.root / "public.json.gz"
        with gzip.open(path, "rt") as stream:
            market = json.load(stream)
        market["scopeMembership"] = {
            "bindingSha256": "a" * 64, "proofSha256": "b" * 64,
            "activityMode": "activity", "includeShared": True,
        }
        market["scopeActivity"] = {
            "schemaVersion": 1, "candidateId": "market-test", "mainRelease": "main-test",
            "bindingSha256": "a" * 64, "proofSha256": "b" * 64,
            "recordCountGrain": "distinct-source-record-across-period",
            "scopes": {"US": {"PRIME_AWARD": {
                "records": 2, "byFiscalYear": {"2026": "937.00", "2027": "0.00"},
                "netUsd": "937.00",
            }}, "US_COUNTER_UAS": {"PRIME_AWARD": {
                "records": 2, "byFiscalYear": {"2026": "937.00", "2027": "0.00"},
                "netUsd": "937.00",
            }}, "CUAS_DETECTION": {"PRIME_AWARD": {
                "records": 1, "byFiscalYear": {"2026": "500.00"}, "netUsd": "500.00",
            }}},
        }
        with gzip.open(path, "wt") as stream:
            json.dump(market, stream)
        proof_path = self.root / "scope-membership-proof.json.gz"
        with gzip.open(proof_path, "wt") as stream:
            json.dump({"status": "PASSED", "candidateId": "market-test", "mainRelease": "main-test",
                       "bindingSha256": "a" * 64, "gates": {gate: "PASSED" for gate in SCOPE_GATES}}, stream)
        proof_sha = hashlib.sha256(proof_path.read_bytes()).hexdigest()
        market["scopeMembership"]["proofSha256"] = proof_sha
        market["scopeActivity"]["proofSha256"] = proof_sha
        with gzip.open(path, "wt") as stream:
            json.dump(market, stream)
        pin = json.loads((self.root / "pin.json").read_text())
        pin["scope_membership_proof_sha256"] = proof_sha
        (self.root / "pin.json").write_text(json.dumps(pin))
        pack = SharedMarketExplorerStore(self.root).get("US_COUNTER_UAS")
        self.assertEqual(pack["source_totals"]["PRIME_AWARD"]["net_usd"], "937.00")
        self.assertEqual(pack["subsegments"][0]["source_totals"]["PRIME_AWARD"]["net_usd"], "500.00")

    def test_native_activity_rejects_unbound_policy(self):
        path = self.root / "public.json.gz"
        with gzip.open(path, "rt") as stream:
            market = json.load(stream)
        market["scopeActivity"] = {"schemaVersion": 1}
        with gzip.open(path, "wt") as stream:
            json.dump(market, stream)
        with self.assertRaisesRegex(ValueError, "published together"):
            SharedMarketExplorerStore(self.root)

    def test_mismatched_release_fails_closed(self):
        with gzip.open(self.root / "public.json.gz", "rt") as stream:
            market = json.load(stream)
        market["mainRelease"] = "other-main"
        with gzip.open(self.root / "public.json.gz", "wt") as stream:
            json.dump(market, stream)
        with self.assertRaisesRegex(ValueError, "do not match"):
            SharedMarketExplorerStore(self.root)

    def test_default_and_legacy_requests_select_shared_market(self):
        lab = load_lab()
        for source in (None, "current", "market_explorer"):
            values = {"messages": [{"role": "user", "content": "How has C-UAS changed?"}]}
            if source is not None:
                values["market_mapping_source"] = source
            with self.subTest(source=source):
                self.assertEqual(lab.AskRequest(**values).market_mapping_source, "market_explorer")


if __name__ == "__main__":
    unittest.main()
