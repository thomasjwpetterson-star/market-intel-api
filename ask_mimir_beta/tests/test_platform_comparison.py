import unittest

from platform_comparison import build_platform_comparison, comparison_answer_projection


def platform_context(platform_id, suppliers):
    return {
        "scope": {"platform_id": platform_id},
        "financial_totals": {},
        "coverage": {},
        "reported_supplier_concentration": {},
        "direct_award_recipients": [],
        "reported_supplier_sites": suppliers,
        "reported_component_categories": [],
        "item_and_component_evidence": {},
        "top_prime_awards": [],
    }


class FakePlatformStore:
    def __init__(self, contexts):
        self.contexts = contexts

    def comparison_projection(self, platform_id):
        return self.contexts[platform_id]


class PlatformComparisonTests(unittest.TestCase):
    def test_exact_cage_overlap_keeps_each_platforms_evidence(self):
        shared_a = {
            "cage": "12345",
            "supplier_name": "Example Systems Inc",
            "city": "A",
            "state": "VA",
            "reported_descriptions": ["flight controls"],
            "mapped_platforms": ["UH-60", "CH-47", "AH-64"],
        }
        shared_b = {
            **shared_a,
            "reported_descriptions": ["actuation equipment"],
        }
        store = FakePlatformStore(
            {
                "UH-60": platform_context("UH-60", [shared_a]),
                "CH-47": platform_context("CH-47", [shared_b]),
            }
        )
        pack = build_platform_comparison(store, ["UH-60", "CH-47"])
        self.assertEqual(pack["platform_ids"], ["UH-60", "CH-47"])
        self.assertEqual(pack["overlap_counts"]["exact_cage_site_overlap"], 1)
        self.assertEqual(
            pack["material_exact_cage_site_overlap"][0]["cage"], "12345"
        )
        self.assertEqual(
            pack["material_exact_cage_site_overlap"][0]["platform_evidence"][
                "UH-60"
            ]["reported_descriptions"],
            ["flight controls"],
        )

    def test_cross_program_projection_does_not_repeat_full_supplier_tables(self):
        pack = {
            "platforms": [
                {
                    "reported_supplier_sites": [{"cage": "12345"}],
                    "reported_component_categories": [{"name": "controls"}],
                }
            ],
            "material_exact_cage_site_overlap": [{"cage": "12345"}],
            "material_reported_organization_overlap_at_different_sites": [],
            "material_cross_program_supplier_sites": [{"cage": "12345"}],
        }
        projected = comparison_answer_projection(
            pack, "comparison_cross_program_exposure"
        )
        self.assertEqual(projected["platforms"][0]["reported_supplier_sites"], [])
        self.assertEqual(projected["material_exact_cage_site_overlap"], [])
        self.assertEqual(
            projected["material_cross_program_supplier_sites"], [{"cage": "12345"}]
        )


if __name__ == "__main__":
    unittest.main()
