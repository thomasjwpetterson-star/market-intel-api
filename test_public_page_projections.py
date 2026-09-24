import json
import unittest

import duckdb

import public_nsn_policy as policy
from public_nsn_policy import public_supplier_relationship, public_supplier_relationship_sql
from public_page_projections import build_public_nsn_profiles


class PublicPageProjectionTests(unittest.TestCase):
    def setUp(self):
        self.connection = duckdb.connect()
        self.addCleanup(self.connection.close)
        self.connection.execute("CREATE TABLE public_intelligence_manifest_next (entity_type VARCHAR, entity_id VARCHAR)")
        self.connection.execute("INSERT INTO public_intelligence_manifest_next VALUES ('nsn', '5310001860967'), ('nsn', '2910000013841')")
        self.connection.execute("""
            CREATE TABLE v_nsn_profile_lookup (
                niin VARCHAR, item_name VARCHAR, fsc_code VARCHAR,
                unit_of_issue VARCHAR, source_of_supply VARCHAR,
                acquisition_advice_code VARCHAR, shelf_life_code VARCHAR
            )
        """)
        self.connection.execute("""
            INSERT INTO v_nsn_profile_lookup VALUES
            ('001860967', 'WASHER,FLAT', '5310', 'EA', 'SMS', 'D', '0'),
            ('000013841', 'FILTER ELEMENT', '2910', 'EA', 'S9I', 'J', 'A')
        """)
        self.connection.execute("""
            CREATE TABLE v_nsn_cage_reference (
                niin VARCHAR, cage VARCHAR, vendor_name VARCHAR,
                part_number VARCHAR, is_active_authorized_source BOOLEAN,
                is_procurement_authorized BOOLEAN, supplier_status VARCHAR,
                rncc_codes VARCHAR, rnvc_codes VARCHAR, rnsc_codes VARCHAR,
                reference_source VARCHAR
            )
        """)
        for index in range(12):
            self.connection.execute(
                "INSERT INTO v_nsn_cage_reference VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    "001860967", f"C{index:04d}", f"SUPPLIER {index}", f"PART-{index:02d}",
                    index == 0, index == 1, "legacy wording must not leak",
                    "3" if index == 2 else ("1" if index == 3 else "5"),
                    "2" if index == 2 else "", "F" if index == 4 else "",
                    "OBSERVED_DLA_SALE" if index == 5 else "DLA_FLIS_PART_REFERENCE",
                ],
            )
        self.connection.execute("""
            CREATE TABLE v_nsn_supplier_lookup (
                niin VARCHAR, cage VARCHAR, contract_id VARCHAR,
                last_sold VARCHAR, sub_agency VARCHAR, parent_agency VARCHAR,
                vendor VARCHAR, total_revenue DOUBLE
            )
        """)
        for index in range(7):
            self.connection.execute(
                "INSERT INTO v_nsn_supplier_lookup VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ["001860967", f"C{index:04d}", f"AWARD-{index}", f"2026-09-{20-index:02d}", "DLA", "DOD", f"SUPPLIER {index}", 1000 + index],
            )
        self.connection.execute("CREATE TABLE v_nsn_summary (niin VARCHAR, platform_families VARCHAR, platform_count BIGINT)")
        self.connection.execute("INSERT INTO v_nsn_summary VALUES ('001860967', 'P1|P2|P3|P4|P5|P6|P7|P8', 8)")
        self.connection.execute("""
            CREATE TABLE v_nsn_supply_state (
                niin VARCHAR, supply_signal VARCHAR, total_stock DOUBLE,
                backorder_qty DOUBLE, annual_demand_quantity DOUBLE,
                reorder_point DOUBLE, reorder_point_gap DOUBLE,
                below_reorder_point BOOLEAN, forecast_3m_qty BIGINT,
                forecast_12m_qty BIGINT, forecast_stock_cover_months DOUBLE,
                source_retrieval_date DATE
            )
        """)
        self.connection.execute("""
            INSERT INTO v_nsn_supply_state VALUES
            ('001860967', 'BELOW_REORDER_POINT', 17, 4, 120, 30, 13, TRUE, 36, 144, 1.4167, DATE '2026-09-24')
        """)
        self.connection.execute("""
            CREATE TABLE v_nsn_price_summary (
                niin VARCHAR, latest_net_price DOUBLE, latest_price_date DATE,
                latest_unit_of_issue VARCHAR, trailing_12m_min_price DOUBLE,
                trailing_12m_median_price DOUBLE, trailing_12m_max_price DOUBLE,
                trailing_12m_observation_count BIGINT, source_retrieval_date DATE
            )
        """)
        self.connection.execute("""
            INSERT INTO v_nsn_price_summary VALUES
            ('001860967', 12.5, DATE '2026-08-31', 'EA', 9.5, 11.0, 14.75, 8, DATE '2026-09-24')
        """)
        self.connection.execute("""
            CREATE TABLE v_nsn_opportunity_detail (
                niin VARCHAR, solicitation_number VARCHAR,
                solicitation_line_number VARCHAR, response_deadline DATE,
                quantity DOUBLE, unit_of_issue VARCHAR,
                small_business_set_aside_indicator VARCHAR
            )
        """)
        self.connection.execute("""
            INSERT INTO v_nsn_opportunity_detail VALUES
            ('000013841', 'SPE7L526T5482', '0001', DATE '2026-09-28', 603, 'EA', 'N'),
            ('000013841', 'SPE7L526T5482', '0002', DATE '2026-09-28', 10, 'EA', 'N'),
            ('000013841', 'SOL-2', '0001', DATE '2026-09-29', 20, 'EA', 'Y'),
            ('000013841', 'SOL-3', '0001', DATE '2026-09-30', 30, 'EA', NULL),
            ('000013841', 'SOL-4', '0001', DATE '2026-10-01', 40, 'EA', 'N')
        """)

    def payload(self, entity_id):
        raw = self.connection.execute(
            "SELECT payload_json FROM public_nsn_profile_next WHERE entity_id = ?", [entity_id]
        ).fetchone()[0]
        return json.loads(raw)

    def test_enriched_nsn_has_exact_public_operational_fields_and_limits(self):
        build_public_nsn_profiles(self.connection)
        payload = self.payload("5310001860967")
        self.assertEqual(payload["niin"], "001860967")
        self.assertEqual((len(payload["part_numbers"]), payload["associated_part_number_count"], payload["part_numbers_hidden"]), (10, 12, 2))
        self.assertEqual((len(payload["supplier_sites"]), payload["associated_supplier_site_count"], payload["supplier_sites_hidden"]), (5, 12, 7))
        self.assertEqual((len(payload["platforms"]), payload["platforms_hidden"]), (6, 2))
        self.assertEqual((len(payload["recent_contracts"]), payload["observed_contract_count"], payload["contracts_hidden"]), (5, 7, 2))
        self.assertEqual(payload["supplier_sites"][0]["status"], "DLA-authorised source")
        self.assertEqual(payload["supplier_sites"][1]["status"], "DLA-authorised source · inactive CAGE")
        self.assertNotIn("legacy wording", json.dumps(payload["supplier_sites"]))
        self.assertEqual(payload["logistics_summary"], {
            "unit_of_issue": "EA", "managing_supply_activity": "SMS", "source_of_supply": "SMS",
            "acquisition_advice_code": "D", "shelf_life_code": "0",
        })
        self.assertEqual(payload["demand_supply_teaser"], {
            "supply_signal": "BELOW_REORDER_POINT", "total_stock": 17.0,
            "backorder_qty": 4.0, "annual_demand_quantity": 120.0,
            "reorder_point": 30.0, "reorder_point_gap": 13.0,
            "below_reorder_point": True, "forecast_3m_qty": 36,
            "forecast_12m_qty": 144, "forecast_stock_cover_months": 1.4167,
        })
        self.assertEqual(payload["observed_price_summary"], {
            "latest_net_price": 12.5, "latest_price_date": "2026-08-31",
            "latest_unit_of_issue": "EA", "trailing_12m_min_price": 9.5,
            "trailing_12m_median_price": 11.0, "trailing_12m_max_price": 14.75,
            "trailing_12m_observation_count": 8,
        })
        self.assertNotIn("source_retrieval_date", json.dumps(payload))

    def test_opportunity_niin_is_distinct_limited_and_reports_hidden_count(self):
        build_public_nsn_profiles(self.connection)
        summary = self.payload("2910000013841")["opportunity_summary"]
        self.assertEqual((summary["active_solicitation_count"], len(summary["active_solicitations"]), summary["solicitations_hidden"]), (4, 3, 1))
        self.assertEqual(summary["active_solicitations"][0], {
            "solicitation_number": "SPE7L526T5482", "response_deadline": "2026-09-28",
            "quantity": 603.0, "unit_of_issue": "EA", "small_business_set_aside_indicator": "N",
        })

    def test_shared_policy_matches_sql_and_request_time_import_contract(self):
        label_sql, rank_sql = public_supplier_relationship_sql(
            active="is_active", procurement="is_procurement", rncc="rncc", rnvc="rnvc", rnsc="rnsc", source="source"
        )
        cases = [
            (False, False, "3", "2", "", "", "Item-identifying manufacturer", 2),
            (False, False, "", "", "", "OBSERVED_DLA_SALE", "DLA award recipient", 5),
            (False, False, "", "", "", "", "Part/CAGE reference", 7),
        ]
        self.connection.execute("CREATE TABLE supplier_cases (is_active BOOLEAN, is_procurement BOOLEAN, rncc VARCHAR, rnvc VARCHAR, rnsc VARCHAR, source VARCHAR)")
        for case in cases:
            self.connection.execute("INSERT INTO supplier_cases VALUES (?, ?, ?, ?, ?, ?)", case[:6])
        sql_rows = self.connection.execute(f"SELECT {label_sql}, {rank_sql} FROM supplier_cases").fetchall()
        for case, sql_result in zip(cases, sql_rows):
            python_result = public_supplier_relationship({
                "is_active_authorized_source": case[0], "is_procurement_authorized": case[1],
                "rncc_codes": case[2], "rnvc_codes": case[3], "rnsc_codes": case[4], "source": case[5],
            })
            self.assertEqual(python_result, sql_result)
            self.assertEqual(sql_result, case[6:])
        self.assertEqual((policy.PUBLIC_NSN_PART_NUMBER_LIMIT, policy.PUBLIC_NSN_SUPPLIER_SITE_LIMIT, policy.PUBLIC_NSN_CONNECTED_PLATFORM_LIMIT, policy.PUBLIC_NSN_RECENT_CONTRACT_LIMIT, policy.PUBLIC_NSN_ACTIVE_SOLICITATION_LIMIT), (10, 5, 6, 5, 3))


if __name__ == "__main__":
    unittest.main()
