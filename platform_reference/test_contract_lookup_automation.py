"""Release safety: real child resource guards and additive workflow rollback."""
from copy import deepcopy
import io
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from platform_reference.contract_lookup_automation import guarded, prefix_for, read_json, download, MIB
from platform_reference.contract_lookup_workflow import add_lookup_stage, remove_lookup_stage
from platform_reference.contract_lookup_release import validate_automated_lookup
from test_release_pointer_control import child_body, lookup_evidence


class LookupAutomationTests(unittest.TestCase):
    def test_wrong_or_traversing_release_is_rejected(self):
        for value in ("../current_manifest.json", "", "run/other", "a" * 129):
            with self.assertRaises(ValueError):
                prefix_for(value)

    def test_child_memory_and_deadline_stop_without_success(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for script, limit, seconds in (("x=bytearray(32*1024**2); import time; time.sleep(10)", 8*MIB, 5),
                                            ("import time; time.sleep(10)", 128*MIB, .1)):
                with self.assertRaisesRegex(RuntimeError, "stopped safely"):
                    guarded([sys.executable, "-c", script], root, root/"child.log",
                            rss_limit=limit, seconds=seconds, deadline=time.monotonic()+10,
                            disk_floor=0)

    def test_wrong_source_and_failed_load_or_memory_proof_reject_promotion(self):
        main = json.loads(child_body("main"))
        evidence = lookup_evidence(main)
        validate_automated_lookup(main, evidence)
        for mutate in (lambda p:p.update(source_asset_sha256="c"*64),
            lambda p:p["query_resources"].update(stop_guard="rss"),
            lambda p:p["offline_queries"].update(state="PERFORMANCE_GATE_FAILED"),
            lambda p:p["exact_parity"]["parts"][0].update(all_values_equal=False)):
            changed = deepcopy(evidence)
            mutate(changed)
            with self.assertRaises(ValueError):
                validate_automated_lookup(main, changed)

    def test_workflow_patch_and_rollback_preserve_independent_changes(self):
        definition = {"States": {"CheckUnifiedV7Exit": {"Choices": [{
            "Variable": "$.unified_v7_result.exit_code", "NumericEquals": 0,
            "Next": "CandidateRehearsalFinished"}]}, "BuildUnifiedV7Candidate": {
                "Parameters": {"TaskDefinition": "v7:8", "Overrides": {}, "NetworkConfiguration": {"private": True}},
                "ResultSelector": {"exit_code.$": "$.Containers[0].ExitCode"},
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "NotifyFailure"}]},
            "CandidateRehearsalFinished": {"Type": "Succeed"}}}
        updated = add_lookup_stage(definition, "lookup:1")
        self.assertEqual(updated["States"]["BuildContractLookup"]["TimeoutSeconds"], 5400)
        self.assertEqual(add_lookup_stage(updated, "lookup:1"), updated)
        updated["States"]["IndependentNewStage"] = {"Type": "Succeed"}
        rolled = remove_lookup_stage(updated, "lookup:1")
        self.assertIn("IndependentNewStage", rolled["States"])
        del rolled["States"]["IndependentNewStage"]
        self.assertEqual(rolled, definition)
        with self.assertRaises(ValueError):
            remove_lookup_stage(updated, "someone-elses:2")


if __name__ == "__main__":
    unittest.main()
