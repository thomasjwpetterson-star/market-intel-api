import json
import unittest

import duckdb

from public_page_projections import build_public_nsn_profiles


class PublicPageProjectionTests(unittest.TestCase):
    def test_nsn_projection_builds_one_ready_to_serve_payload(self):
        connection = duckdb.connect()
        connection.execute("""
            CREATE TABLE public_intelligence_manifest_next (
                entity_type VARCHAR,
                entity_id VARCHAR
            )
        """)
        connection.execute(
            "INSERT INTO public_intelligence_manifest_next VALUES ('nsn', '4920015575155')"
        )
        connection.execute("""
            CREATE TABLE v_nsn_profile_lookup (
                niin VARCHAR,
                item_name VARCHAR,
                fsc_code VARCHAR
            )
        """)
        connection.execute(
            "INSERT INTO v_nsn_profile_lookup VALUES ('015575155', 'PLUG VALVE,AIRCRAFT', '4920')"
        )
        connection.execute("""
            CREATE TABLE v_nsn_cage_reference (
                niin VARCHAR,
                cage VARCHAR,
                vendor_name VARCHAR,
                part_number VARCHAR,
                is_active_authorized_source BOOLEAN,
                is_procurement_authorized BOOLEAN,
                supplier_status VARCHAR
            )
        """)
        connection.execute("""
            INSERT INTO v_nsn_cage_reference VALUES
            ('015575155', '45934', 'ONTIC', '11-105', TRUE, TRUE, 'ACTIVE')
        """)
        connection.execute("""
            CREATE TABLE v_nsn_supplier_lookup (
                niin VARCHAR,
                cage VARCHAR,
                contract_id VARCHAR,
                last_sold VARCHAR,
                sub_agency VARCHAR,
                parent_agency VARCHAR,
                vendor VARCHAR,
                total_revenue DOUBLE
            )
        """)
        connection.execute("""
            INSERT INTO v_nsn_supplier_lookup VALUES
            ('015575155', '45934', 'FAKE-AWARD', '2026-09-01',
             'AIR FORCE', 'DEPARTMENT OF DEFENSE', 'ONTIC', 125000)
        """)
        connection.execute("""
            CREATE TABLE v_nsn_summary (
                niin VARCHAR,
                platform_families VARCHAR,
                platform_count BIGINT
            )
        """)
        connection.execute(
            "INSERT INTO v_nsn_summary VALUES ('015575155', 'A-4|AV-8B', 2)"
        )

        build_public_nsn_profiles(connection)

        entity_id, niin, raw_payload = connection.execute("""
            SELECT entity_id, niin, payload_json
            FROM public_nsn_profile_next
        """).fetchone()
        payload = json.loads(raw_payload)
        self.assertEqual(entity_id, "4920015575155")
        self.assertEqual(niin, "015575155")
        self.assertEqual(payload["item_name"], "PLUG VALVE,AIRCRAFT")
        self.assertEqual(payload["part_numbers"], ["11-105"])
        self.assertEqual(payload["approved_source"]["cage"], "45934")
        self.assertEqual(payload["platforms"], ["A-4", "AV-8B"])
        self.assertEqual(payload["observed_contract_count"], 1)
        self.assertEqual(payload["recent_contracts"][0]["contract_id"], "FAKE-AWARD")


if __name__ == "__main__":
    unittest.main()
