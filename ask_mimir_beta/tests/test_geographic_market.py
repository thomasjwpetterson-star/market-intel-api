import json
import tempfile
import unittest
from pathlib import Path

from geographic_market import StateIndustrialBaseStore


class StateIndustrialBasePrecomputedTests(unittest.TestCase):
    def test_precomputed_state_pack_is_loaded_and_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for filename in (
                "transactions.parquet",
                "network.parquet",
                "cage_locations.parquet",
            ):
                (root / filename).touch()
            precomputed = root / "state-markets"
            precomputed.mkdir()
            pack = {
                "context_type": "state_industrial_base",
                "scope": {"state_code": "TX", "state_name": "Texas"},
                "ranked_registered_facilities": [
                    {"cage": "11111"},
                    {"cage": "22222"},
                ],
                "leading_places_of_performance": [
                    {"city": "FORT WORTH"},
                    {"city": "DALLAS"},
                ],
                "coverage": {"registered_facilities": 2},
            }
            (precomputed / "tx.json").write_text(json.dumps(pack))

            result = StateIndustrialBaseStore(
                root, precomputed_dir=precomputed
            ).get("TX", limit=1)

            self.assertEqual(result["scope"]["state_name"], "Texas")
            self.assertEqual(len(result["ranked_registered_facilities"]), 1)
            self.assertEqual(len(result["leading_places_of_performance"]), 1)


if __name__ == "__main__":
    unittest.main()
