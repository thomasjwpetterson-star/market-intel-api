import tempfile
import unittest
from pathlib import Path

import duckdb

from company_context import CompanyContextBuilder


class CompanyIdentityPrecedenceTests(unittest.TestCase):
    def test_current_cage_directory_name_replaces_historical_profile_name(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            profiles = root / "profiles.parquet"
            geo = root / "geo.parquet"
            connection = duckdb.connect()
            connection.execute(
                """
                COPY (
                    SELECT
                        '26101'::VARCHAR AS cage_code,
                        'THE TRIUMPH GROUP OPERATIONS INC'::VARCHAR AS vendor_name,
                        'AWARD_AND_NETWORK'::VARCHAR AS profile_source,
                        2026::INTEGER AS last_active_year,
                        2026::INTEGER AS network_last_active_year
                ) TO ? (FORMAT PARQUET)
                """,
                [str(profiles)],
            )
            connection.execute(
                """
                COPY (
                    SELECT
                        '26101'::VARCHAR AS cage_code,
                        'AAR ALLEN SERVICES, INC'::VARCHAR AS vendor_name,
                        'WELLINGTON'::VARCHAR AS city,
                        'KS'::VARCHAR AS state,
                        'ZIP_MATCH'::VARCHAR AS location_quality
                ) TO ? (FORMAT PARQUET)
                """,
                [str(geo)],
            )
            builder = object.__new__(CompanyContextBuilder)
            builder.connection = connection
            builder.paths = {"profiles": profiles, "geo": geo}

            sites = builder._site_identities(["26101"], [])

            self.assertEqual(sites[0]["vendor_name"], "AAR ALLEN SERVICES, INC")
            connection.close()


if __name__ == "__main__":
    unittest.main()
