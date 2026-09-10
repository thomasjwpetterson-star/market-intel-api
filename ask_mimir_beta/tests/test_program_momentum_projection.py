import json
import tempfile
import unittest
from pathlib import Path

from program_momentum_store import (
    ProgramMomentumStore,
    is_program_momentum_language,
)


class ProgramMomentumProjectionTests(unittest.TestCase):
    def test_current_tactical_missile_language_uses_forward_workflow(self):
        for question in (
            "What's happening in the US tactical missile market at the moment?",
            "Give me the current outlook for US tactical missiles.",
            "Which interceptor programs are accelerating?",
        ):
            with self.subTest(question=question):
                self.assertTrue(is_program_momentum_language(question))
        self.assertFalse(is_program_momentum_language("Who supplies AMRAAM?"))

    def test_answer_projection_retains_bounded_budget_evidence(self):
        program = {
            "program_id": "EXAMPLE",
            "display_name": "Example missile",
            "rank": 1,
            "composite": {},
            "signal_lanes": {
                "production_events": {"events": [], "provisional_score": None}
            },
            "budget_evidence": [{"fiscal_year": year} for year in range(20)],
        }
        pack = {
            "programs": [program],
            "evidence_fingerprints": {},
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "momentum.json"
            path.write_text(json.dumps(pack))
            result = ProgramMomentumStore(path).get(market="missiles", limit=1)
        self.assertEqual(len(result["programs"][0]["budget_evidence"]), 16)


if __name__ == "__main__":
    unittest.main()
