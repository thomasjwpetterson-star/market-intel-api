import unittest
import json
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from platform_context import PlatformContextStore, build_precomputed_platform_contexts

from platform_conversation_segments import (
    conversation_platform_topic,
    platform_segment_projection,
    platform_topic_options,
)


class PlatformConversationSegmentsTests(unittest.TestCase):
    def test_broad_question_answers_directly_and_focused_followup_selects_one(self):
        self.assertEqual(
            conversation_platform_topic("Tell me everything about AMRAAM"), "overview"
        )
        self.assertEqual(conversation_platform_topic("Tell me about AMRAAM"), "overview")
        self.assertEqual(conversation_platform_topic("Show me topic options for AMRAAM"), "menu")
        self.assertEqual(conversation_platform_topic("How much was spent on AMRAAM in FY2026?"), "awards")
        self.assertEqual(conversation_platform_topic("Does the FY2026 figure mean demand fell?", has_active_scope=True), "awards")
        self.assertEqual(conversation_platform_topic("Who makes AMRAAM?"), "prime")
        self.assertEqual(
            conversation_platform_topic("I am interested in the supply chain", has_active_scope=True),
            "supply_chain",
        )
        self.assertEqual(
            conversation_platform_topic("All of the above", has_active_scope=True), "overview"
        )
        self.assertEqual(
            conversation_platform_topic("Show me all suppliers for AMRAAM"), "overview"
        )

    def test_topic_menu_has_four_conversation_choices_and_a_deep_choice(self):
        options = platform_topic_options("AMRAAM")
        self.assertEqual([row["topic"] for row in options],
                         ["prime", "awards", "parts", "supply_chain", "all"])
        self.assertTrue(all(row["answer_mode"] == "conversation" for row in options[:-1]))
        self.assertEqual(options[-1]["answer_mode"], "deep_research")
        for row in options[:-1]:
            self.assertEqual(conversation_platform_topic(row["prompt"]), row["topic"])

    def test_supply_chain_view_keeps_selected_relationships_and_marks_other_lanes_unchecked(self):
        pack = {
            "scope": {"platform_id": "AMRAAM", "observation_window": "FY2021–FY2027"},
            "coverage": {"reported_supplier_sites": 247},
            "reported_supplier_sites": [{"cage": str(i), "supplier_name": f"Site {i}"}
                                        for i in range(30)],
            "direct_award_recipients": [{"cage": str(i)} for i in range(8)],
            "item_and_component_evidence": {"top_items": [{"niin": str(i)} for i in range(15)]},
            "top_prime_awards": [{"contract_id": "award"}],
            "evidence_index": [{"source": "USAspending"}],
        }
        segment = platform_segment_projection(pack, "supply_chain")
        self.assertEqual(segment["scope"], pack["scope"])
        self.assertEqual(segment["coverage"]["reported_supplier_sites"], 247)
        self.assertEqual(len(segment["reported_supplier_sites"]), 14)
        self.assertEqual(len(segment["direct_award_recipients"]), 5)
        self.assertEqual(len(segment["item_and_component_evidence"]["top_items"]), 8)
        self.assertNotIn("top_prime_awards", segment)
        self.assertIn("top_prime_awards", segment["evidence_scope"]["omitted_lanes"])
        self.assertEqual(len(pack["reported_supplier_sites"]), 30)

    def test_overview_keeps_bounded_awards_suppliers_and_parts(self):
        pack = {
            "scope": {"platform_id": "AMRAAM"},
            "coverage": {"reported_supplier_sites": 247},
            "direct_award_recipients": [{"cage": str(i)} for i in range(8)],
            "reported_supplier_sites": [{"cage": str(i)} for i in range(30)],
            "top_prime_awards": [{"contract_id": str(i)} for i in range(15)],
            "item_and_component_evidence": {"top_items": [{"niin": str(i)} for i in range(15)]},
            "reported_component_categories": [{"name": str(i)} for i in range(12)],
        }
        segment = platform_segment_projection(pack, "overview")
        self.assertEqual(len(segment["direct_award_recipients"]), 5)
        self.assertEqual(len(segment["reported_supplier_sites"]), 14)
        self.assertEqual(len(segment["top_prime_awards"]), 8)
        self.assertEqual(len(segment["item_and_component_evidence"]["top_items"]), 8)
        self.assertEqual(len(segment["reported_component_categories"]), 8)
        self.assertNotIn("item_and_component_evidence", segment["evidence_scope"]["omitted_lanes"])

    def test_release_builder_writes_topic_files_and_runtime_reads_one_directly(self):
        full = {
            "scope": {"platform_id": "AMRAAM", "display_name": "AMRAAM",
                      "included_platform_records": ["AMRAAM"]},
            "coverage": {"reported_supplier_sites": 2},
            "reported_supplier_sites": [{"cage": "1BWN1"}],
            "direct_award_recipients": [{"cage": "15090"}],
            "evidence_index": [{"source": "USAspending"}],
            "evidence_fingerprint": "source-fingerprint",
        }

        class FakeStore:
            def __init__(self, *_args, **_kwargs):
                self.connection = SimpleNamespace(close=lambda: None)
            def search(self, _name):
                return {"resolved_platform_id": "AMRAAM"}
            def get(self, _name):
                return full
            def answer_projection(self, _name, supplier_limit):
                self_suppliers = full["reported_supplier_sites"][:supplier_limit]
                return {**full, "reported_supplier_sites": self_suppliers}

        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            with patch("platform_context.PlatformContextStore", FakeStore):
                manifest = build_precomputed_platform_contexts(
                    output_dir, output_dir, ["AMRAAM"], release_id="release-1"
                )
            self.assertEqual(manifest["schema_version"], "platform-context-precompute-v2")
            entry = manifest["platforms"][0]
            self.assertEqual(set(entry["segments"]),
                             {"prime", "awards", "parts", "supply_chain", "overview"})
            segment_path = output_dir / entry["segments"]["supply_chain"]
            self.assertTrue(segment_path.exists())
            store = object.__new__(PlatformContextStore)
            store._precomputed_segment_paths = {("AMRAAM", "supply_chain"): segment_path}
            store.search = lambda _name: {"resolved_platform_id": "AMRAAM"}
            store._platform_members = lambda _name: ["AMRAAM"]
            store.answer_projection = lambda *_args, **_kwargs: self.fail("fell back to full dossier")
            direct = store.answer_segment_projection("AMRAAM", "supply_chain")
            self.assertEqual(direct["requested_segment"], "supply_chain")
            self.assertEqual(direct["reported_supplier_sites"][0]["cage"], "1BWN1")
            self.assertEqual(json.loads(segment_path.read_text())["scope"]["platform_id"], "AMRAAM")


if __name__ == "__main__":
    unittest.main()
