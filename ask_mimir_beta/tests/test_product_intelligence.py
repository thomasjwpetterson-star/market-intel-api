import tempfile
import unittest
from pathlib import Path

import duckdb

from product_intelligence import (
    ProductIntelligenceStore,
    extract_product_subject,
    product_follow_up_intent,
    resolve_product_family,
    resolve_product_request,
)


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

    def test_explicit_unknown_product_line_gets_dynamic_scope(self):
        query = "Assess Acme XR-500 flight-control product line from an acquisition perspective"
        product_id = resolve_product_request(query)
        self.assertTrue(product_id.startswith("dynamic:"))
        self.assertEqual(extract_product_subject(query), "Acme XR-500 flight-control")

    def test_platform_or_company_question_does_not_become_dynamic_product(self):
        for query in (
            "Who supplies the F-16?",
            "Tell me about Honeywell's US defense business",
            "Find manufacturers of aircraft fuel controls",
        ):
            with self.subTest(query=query):
                self.assertIsNone(resolve_product_request(query))

    def test_dynamic_scope_survives_round_trip_and_builds_exact_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_minimal_sources(root)
            product_id = resolve_product_request("Research the Acme XR-500 product line")
            store = ProductIntelligenceStore(root, load_precomputed=False)
            pack = store.get(product_id)
            self.assertEqual(pack["scope"]["display_name"], "Acme XR-500")
            self.assertEqual(pack["scope"]["scope_status"], "request_defined_product_scope")
            self.assertEqual(len(pack["prime_awards"]), 1)

    @staticmethod
    def _write_minimal_sources(root: Path) -> None:
        connection = duckdb.connect()
        sources = {
            "opportunities.parquet": """
                SELECT 'O1' id, 'S1' sol_num, 'Acme XR-500 support' title,
                       'DoD' agency, 'Air Force' sub_agency, '2027-01-01' deadline,
                       NULL set_aside_type, '1' naics, '1' psc, 'VA' state,
                       'https://example.test' url, 'XR-500 support' description
            """,
            "contracts_rolled.parquet": """
                SELECT 'C1' contract_id, 'A1' award_key, 'ACME' vendor_name,
                       '12345' vendor_cage, 'XR-500 production' base_award_description,
                       'XR-500 production' latest_action_description,
                       'XR-500 production' description, NULL platform_family,
                       NULL platform_families, '1' psc, 'DoD' parent_agency,
                       'Air Force' sub_agency, 'CITY' city, 'VA' state, 'USA' country,
                       'CITY' place_of_performance_city, 'VA' place_of_performance_state,
                       'USA' place_of_performance_country, 10.0 total_spend,
                       DATE '2025-01-01' start_date, DATE '2025-02-01' last_action_date,
                       0.0 obligations_fy2021, 0.0 obligations_fy2022,
                       0.0 obligations_fy2023, 0.0 obligations_fy2024,
                       10.0 obligations_fy2025, 0.0 obligations_fy2026
            """,
            "network.parquet": """
                SELECT NULL prime_name, NULL sub_name, NULL prime_cage, NULL sub_cage,
                       NULL contract_id, NULL prime_award_description, NULL description,
                       NULL action_date, NULL AS year, NULL subaward_value,
                       NULL subaward_value_raw, NULL sub_city, NULL sub_state,
                       NULL sub_country, NULL platform_family, NULL psc WHERE false
            """,
            "nsn_cage_reference.parquet": """
                SELECT CAST(NULL AS VARCHAR) niin, CAST(NULL AS VARCHAR) nsn,
                       CAST(NULL AS VARCHAR) cage, CAST(NULL AS VARCHAR) vendor_name,
                       CAST(NULL AS VARCHAR) description,
                       CAST(NULL AS VARCHAR) part_number,
                       false is_active_authorized_source,
                       CAST(NULL AS VARCHAR) platform_families WHERE false
            """,
            "nsn_supplier_lookup.parquet": """
                SELECT CAST(NULL AS VARCHAR) niin, CAST(NULL AS VARCHAR) cage,
                       CAST(NULL AS VARCHAR) vendor, 0.0 total_revenue,
                       CAST(NULL AS VARCHAR) contract_id,
                       CAST(NULL AS INTEGER) AS year WHERE false
            """,
            "cage_locations.parquet": """
                SELECT '12345' cage_code, 'ACME' vendor_name, 'CITY' city,
                       'VA' state, 'exact' location_quality, 'SAM' entity_source
            """,
        }
        for filename, query in sources.items():
            escaped = str(root / filename).replace("'", "''")
            connection.execute(f"COPY ({query}) TO '{escaped}' (FORMAT PARQUET)")
        connection.close()


if __name__ == "__main__":
    unittest.main()
