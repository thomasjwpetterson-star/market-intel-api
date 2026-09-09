import unittest

from product_intelligence import product_follow_up_intent, resolve_product_family


class ProductIntelligenceResolutionTests(unittest.TestCase):
    def test_leonardo_flight_recorder_family_resolves_from_company_and_product(self):
        self.assertEqual(
            resolve_product_family(
                "Find out everything about the Leonardo DRS flight recorder product line"
            ),
            "leonardo_drs_flight_recorders",
        )

    def test_each_distinctive_product_alias_resolves(self):
        for query in (
            "What is the outlook for DFIRS 2100?",
            "Assess the EAS3000 and ELB 3000 family",
            "Who supports the CPI-406?",
            "Tell me about the M-346 AJT CSMU",
        ):
            with self.subTest(query=query):
                self.assertEqual(
                    resolve_product_family(query), "leonardo_drs_flight_recorders"
                )

    def test_ambiguous_acronym_does_not_resolve_on_its_own(self):
        self.assertIsNone(resolve_product_family("Tell me about CSMU"))

    def test_product_diligence_follow_up_retains_scope(self):
        self.assertTrue(
            product_follow_up_intent(
                "What are the key acquisition risks and which sites matter?"
            )
        )


if __name__ == "__main__":
    unittest.main()
