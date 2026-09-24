import tempfile
import unittest
from pathlib import Path

import duckdb

from item_context import ItemContextStore


BASE_COLUMNS = """
    '003214003'::VARCHAR AS niin,
    '6695003214003'::VARCHAR AS nsn,
    '6695'::VARCHAR AS fsc_code,
    'INDICATOR,SYMBOL INDICATING'::VARCHAR AS description,
    'EA'::VARCHAR AS unit_of_issue,
    'D'::VARCHAR AS acquisition_advice_code,
    100.0::DOUBLE AS govt_estimated_price,
    'SMS'::VARCHAR AS source_of_supply,
    NULL::VARCHAR AS demil_code,
    NULL::VARCHAR AS shelf_life_code
"""


class ItemContextLineageTests(unittest.TestCase):
    def _store(self, path: Path) -> ItemContextStore:
        store = ItemContextStore.__new__(ItemContextStore)
        store.connection = duckdb.connect()
        store.paths = {"reference": path}
        store.reference_columns = {
            str(row[0]).lower()
            for row in store.connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
            ).fetchall()
        }
        return store

    def test_reads_reported_end_item_context_when_present(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "reference.parquet"
            duckdb.sql(
                f"""
                COPY (
                    SELECT {BASE_COLUMNS},
                           '20814'::VARCHAR AS item_name_codes,
                           'INDICATOR,LIQUID QUANTITY'::VARCHAR
                               AS reported_end_item_context
                ) TO '{path}' (FORMAT PARQUET)
                """
            )
            store = self._store(path)
            profile = store._reference_profile("003214003")
            store.connection.close()

        self.assertEqual(profile["item_name_codes"], "20814")
        self.assertEqual(
            profile["reported_end_item_context"], "INDICATOR,LIQUID QUANTITY"
        )

    def test_remains_compatible_with_reference_file_without_lineage_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "reference.parquet"
            duckdb.sql(
                f"COPY (SELECT {BASE_COLUMNS}) TO '{path}' (FORMAT PARQUET)"
            )
            store = self._store(path)
            profile = store._reference_profile("003214003")
            store.connection.close()

        self.assertNotIn("item_name_codes", profile)
        self.assertNotIn("reported_end_item_context", profile)
        self.assertEqual(profile["description"], "INDICATOR,SYMBOL INDICATING")

    def test_reads_optional_operational_sidecars_without_changing_reference_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reference_path = root / "reference.parquet"
            supply_path = root / "nsn_supply_state_lookup.parquet"
            price_path = root / "nsn_price_summary_lookup.parquet"
            opportunity_summary_path = root / "nsn_opportunity_summary_lookup.parquet"
            opportunity_detail_path = root / "nsn_opportunity_detail.parquet"
            duckdb.sql(f"COPY (SELECT {BASE_COLUMNS}) TO '{reference_path}' (FORMAT PARQUET)")
            duckdb.sql(
                f"""
                COPY (SELECT '003214003'::VARCHAR AS niin,
                             'BACKORDERED'::VARCHAR AS supply_signal,
                             12::BIGINT AS forecast_3m_qty)
                TO '{supply_path}' (FORMAT PARQUET)
                """
            )
            duckdb.sql(
                f"""
                COPY (SELECT '003214003'::VARCHAR AS niin,
                             2::BIGINT AS active_solicitation_count,
                             DATE '2026-10-01' AS next_response_deadline,
                             'SPE7M226T6958'::VARCHAR AS next_solicitation_number)
                TO '{opportunity_summary_path}' (FORMAT PARQUET)
                """
            )
            duckdb.sql(
                f"""
                COPY (SELECT '003214003'::VARCHAR AS niin,
                             'SPE7M226T6958'::VARCHAR AS solicitation_number,
                             '0001'::VARCHAR AS solicitation_line_number,
                             DATE '2026-10-01' AS response_deadline)
                TO '{opportunity_detail_path}' (FORMAT PARQUET)
                """
            )
            duckdb.sql(
                f"""
                COPY (SELECT '003214003'::VARCHAR AS niin,
                             42.50::DOUBLE AS latest_net_price,
                             9::BIGINT AS price_observation_count)
                TO '{price_path}' (FORMAT PARQUET)
                """
            )
            store = self._store(reference_path)
            store.paths["supply_state"] = supply_path
            store.paths["price_summary"] = price_path
            store.paths["opportunity_summary"] = opportunity_summary_path
            store.paths["opportunity_detail"] = opportunity_detail_path
            supply = store._optional_sidecar("supply_state", "003214003")
            price = store._optional_sidecar("price_summary", "003214003")
            opportunity = store._optional_sidecar("opportunity_summary", "003214003")
            opportunity_rows = store._optional_sidecar_rows(
                "opportunity_detail", "003214003"
            )
            missing = store._optional_sidecar("supply_state", "999999999")
            store.connection.close()

        self.assertEqual(supply["supply_signal"], "BACKORDERED")
        self.assertEqual(supply["forecast_3m_qty"], 12)
        self.assertEqual(price["latest_net_price"], 42.5)
        self.assertEqual(price["price_observation_count"], 9)
        self.assertEqual(opportunity["active_solicitation_count"], 2)
        self.assertEqual(opportunity_rows[0]["solicitation_number"], "SPE7M226T6958")
        self.assertEqual(missing, {})


if __name__ == "__main__":
    unittest.main()
