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


if __name__ == "__main__":
    unittest.main()
