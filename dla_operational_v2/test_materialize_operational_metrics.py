from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from dla_operational_v2.materialize_operational_metrics import (
    connect,
    materialize,
    materialize_opportunities,
)


class OperationalMetricMaterializationTests(unittest.TestCase):
    def test_direct_nsn_solicitations_build_summary_and_detail(self) -> None:
        with tempfile.TemporaryDirectory() as raw_directory:
            root = Path(raw_directory)
            source = root / "solicitations"
            output = root / "output"
            temp = root / "temp"
            source.mkdir()
            output.mkdir()
            temp.mkdir()
            writer = duckdb.connect()
            writer.execute(
                f"""
                COPY (
                  SELECT * FROM (VALUES
                    ('5310001860967', 'SPE7M226T6958', '0001', '10/01/2026', '53', 'PG', 'T', 'N', '7018391512', '', '', '', '', ''),
                    ('5310001860967', 'SPE7M226T7000', '0001', '10/03/2026', '12', 'EA', 'T', 'Y', '7018391999', '', '', '', '', ''),
                    ('5310001860967', 'EXPIRED', '0001', '09/01/2026', '1', 'EA', 'T', 'N', 'OLD', '', '', '', '', ''),
                    ('INVALID', 'BAD', '0001', '10/03/2026', '1', 'EA', 'T', 'N', 'BAD', '', '', '', '', '')
                  ) t(nsn, solicitation_number, solicitation_line_number, return_by_date,
                      quantity, unit_of_issue, solicitation_type_indicator,
                      small_business_set_aside_indicator, purchase_request_number,
                      hazardous_material_id, material_requirements, source_of_supply_cage,
                      actual_mfg_source_cage, actual_mfg_source_name_address)
                ) TO '{source / "data.parquet"}' (FORMAT PARQUET)
                """
            )
            writer.close()
            connection = connect(temp, needs_s3=False)
            try:
                report = materialize_opportunities(
                    connection,
                    str(source),
                    output,
                    "2026-09-24-release",
                    "2026-09-24",
                )
            finally:
                connection.close()
            reader = duckdb.connect()
            summary = reader.execute(
                "SELECT * FROM read_parquet(?)",
                [str(output / "nsn_opportunity_summary_lookup.parquet")],
            ).fetchdf().to_dict(orient="records")
            detail_count = reader.execute(
                "SELECT COUNT(*) FROM read_parquet(?)",
                [str(output / "nsn_opportunity_detail.parquet")],
            ).fetchone()[0]
            reader.close()
            self.assertEqual(detail_count, 2)
            self.assertEqual(report["active_opportunity_niins"], 1)
            self.assertEqual(summary[0]["niin"], "001860967")
            self.assertEqual(summary[0]["active_solicitation_count"], 2)
            self.assertEqual(summary[0]["next_solicitation_number"], "SPE7M226T6958")

    def test_niin_totals_are_not_multiplied_by_repeated_condition_rows(self) -> None:
        with tempfile.TemporaryDirectory() as raw_directory:
            root = Path(raw_directory)
            source = root / "source"
            output = root / "output"
            temp = root / "temp"
            output.mkdir()
            temp.mkdir()
            for relative in (
                "forecast",
                "inventory_backorder/snapshot_date=2026-08-31",
                "inventory_reorder_point/snapshot_date=2026-08-31",
                "price_reason",
            ):
                (source / relative).mkdir(parents=True)

            writer = duckdb.connect()
            writer.execute(
                f"""
                COPY (
                  SELECT * FROM (VALUES
                    ('000000001', '000000001', 'NIIN', 12::BIGINT, DATE '2026-08-01', 'U', true, DATE '2026-09-24', 'r1', 'forecast.txt'),
                    ('000000001', '000000001', 'NIIN', 18::BIGINT, DATE '2026-09-01', 'U', true, DATE '2026-09-24', 'r1', 'forecast.txt'),
                    ('000000001', '000000001', 'NIIN', 100::BIGINT, DATE '2027-09-01', 'U', true, DATE '2026-09-24', 'r1', 'forecast.txt'),
                    ('000000002', '000000002', 'NIIN', 99::BIGINT, DATE '2026-08-01', '7', false, DATE '2026-09-24', 'r1', 'forecast.txt')
                  ) t(source_item_id, niin, identifier_type, forecast_qty, forecast_month,
                      security_classification, public_release_eligible, retrieval_date,
                      source_release, source_file)
                ) TO '{source / "forecast" / "data.parquet"}' (FORMAT PARQUET)
                """
            )
            writer.execute(
                f"""
                COPY (
                  SELECT * FROM (VALUES
                    ('1000000000001', '1000', '000000001', 20::DECIMAL(20,3), 'U', 'A', 5::DECIMAL(20,3), 120::DECIMAL(20,3), true, DATE '2026-09-24', 'r1', 'inventory.txt'),
                    ('1000000000001', '1000', '000000001', 20::DECIMAL(20,3), 'U', 'B', 5::DECIMAL(20,3), 120::DECIMAL(20,3), true, DATE '2026-09-24', 'r1', 'inventory.txt'),
                    ('1000000000002', '1000', '000000002', 50::DECIMAL(20,3), '7', 'A', 0::DECIMAL(20,3), 10::DECIMAL(20,3), false, DATE '2026-09-24', 'r1', 'inventory.txt')
                  ) t(nsn, fsc, niin, total_stock, security_classification, condition_code,
                      backorder_qty, annual_demand_quantity, public_release_eligible,
                      retrieval_date, source_release, source_file)
                ) TO '{source / "inventory_backorder" / "snapshot_date=2026-08-31" / "data.parquet"}' (FORMAT PARQUET)
                """
            )
            writer.execute(
                f"""
                COPY (
                  SELECT * FROM (VALUES
                    ('1000000000001', '1000', '000000001', 20::DECIMAL(20,3), 25::DECIMAL(20,3), 'U', 'A', true, DATE '2026-09-24', 'r1', 'reorder.txt'),
                    ('1000000000001', '1000', '000000001', 20::DECIMAL(20,3), 25::DECIMAL(20,3), 'U', 'B', true, DATE '2026-09-24', 'r1', 'reorder.txt')
                  ) t(nsn, fsc, niin, total_stock, reorder_point, security_classification,
                      condition_code, public_release_eligible, retrieval_date,
                      source_release, source_file)
                ) TO '{source / "inventory_reorder_point" / "snapshot_date=2026-08-31" / "data.parquet"}' (FORMAT PARQUET)
                """
            )
            writer.execute(
                f"""
                COPY (
                  SELECT * FROM (VALUES
                    ('1000', '000000001', '1000000000001', 'U', 'EA', 'ABCDE', 'C1', 2::DECIMAL(20,3), DATE '2026-08-01', 10::DECIMAL(20,4), 'P1', '1', 'R', 'A', 1::BIGINT, true, DATE '2026-09-24', 'r1', 'price.txt'),
                    ('1000', '000000001', '1000000000001', 'U', 'EA', 'FGHIJ', 'C2', 4::DECIMAL(20,3), DATE '2026-09-16', 14::DECIMAL(20,4), 'P2', '1', 'R', 'B', 1::BIGINT, true, DATE '2026-09-24', 'r1', 'price.txt')
                  ) t(fsc, niin, nsn, security_classification, unit_of_issue, cage_code,
                      contract_number, order_quantity, award_date, net_price,
                      purchase_order_number, purchase_order_item_number,
                      price_reviewer_code, price_reason_type, duplicate_count,
                      public_release_eligible, retrieval_date, source_release, source_file)
                ) TO '{source / "price_reason" / "data.parquet"}' (FORMAT PARQUET)
                """
            )
            writer.close()

            connection = connect(temp, needs_s3=False)
            try:
                manifest = materialize(
                    connection,
                    str(source),
                    output,
                    "2026-09-16-1038",
                    "2026-09-24",
                )
            finally:
                connection.close()

            reader = duckdb.connect()
            supply = reader.execute(
                "SELECT * FROM read_parquet(?)",
                [str(output / "nsn_supply_state_lookup.parquet")],
            ).fetchdf().to_dict(orient="records")
            price = reader.execute(
                "SELECT * FROM read_parquet(?)",
                [str(output / "nsn_price_summary_lookup.parquet")],
            ).fetchdf().to_dict(orient="records")
            reader.close()

            self.assertEqual(len(supply), 1)
            self.assertEqual(supply[0]["total_stock"], 20.0)
            self.assertEqual(supply[0]["backorder_qty"], 5.0)
            self.assertEqual(supply[0]["annual_demand_quantity"], 120.0)
            self.assertEqual(supply[0]["reorder_assessment_stock"], 20.0)
            self.assertEqual(supply[0]["reorder_point"], 25.0)
            self.assertEqual(supply[0]["reorder_point_gap"], 5.0)
            self.assertEqual(supply[0]["forecast_3m_qty"], 30)
            self.assertEqual(supply[0]["forecast_12m_qty"], 30)
            self.assertEqual(supply[0]["inventory_source_row_count"], 2)
            self.assertEqual(supply[0]["supply_signal"], "BACKORDERED")
            self.assertTrue(supply[0]["stock_sources_agree"])

            self.assertEqual(len(price), 1)
            self.assertEqual(float(price[0]["latest_net_price"]), 14.0)
            self.assertEqual(price[0]["price_observation_count"], 2)
            self.assertEqual(float(price[0]["trailing_12m_quantity_weighted_price"]), 12.666666666666666)
            self.assertEqual(
                manifest["metric_contract"]["inventory_aggregation"].split()[0],
                "MAX",
            )


if __name__ == "__main__":
    unittest.main()
