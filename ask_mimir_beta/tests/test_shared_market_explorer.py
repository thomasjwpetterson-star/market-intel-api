import gzip
import json
import tempfile
import unittest
from pathlib import Path

from shared_market_explorer import SharedMarketExplorerStore


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

    def test_mismatched_release_fails_closed(self):
        with gzip.open(self.root / "public.json.gz", "rt") as stream:
            market = json.load(stream)
        market["mainRelease"] = "other-main"
        with gzip.open(self.root / "public.json.gz", "wt") as stream:
            json.dump(market, stream)
        with self.assertRaisesRegex(ValueError, "do not match"):
            SharedMarketExplorerStore(self.root)


if __name__ == "__main__":
    unittest.main()
