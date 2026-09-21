"""Rehearse a public-intelligence release without touching the active DuckDB snapshot."""

import argparse
import json
import math
import os
from pathlib import Path
import re
import tempfile
import time

import duckdb


SOURCE_VIEWS = {
    "v_summary": "summary.parquet",
    "v_profiles": "profiles.parquet",
    "v_geo": "geo.parquet",
    "v_contracts_rolled": "contracts_rolled.parquet",
    "v_opportunities": "opportunities.parquet",
    "v_nsn_supplier_lookup": "nsn_supplier_lookup.parquet",
    "v_nsn_profile_lookup": "nsn_profile_lookup.parquet",
    "v_nsn_cage_reference": "nsn_cage_reference.parquet",
    "v_transactions": "transactions.parquet",
    "v_contract_award_metadata": "contract_award_metadata.parquet",
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", type=Path, default=Path("local_data"))
    parser.add_argument("--cohort-size", type=int, default=200_000)
    parser.add_argument("--company-cap", type=int, default=45_000)
    parser.add_argument("--platform-cap", type=int, default=1_000)
    parser.add_argument("--award-cap", type=int, default=30_000)
    parser.add_argument("--solicitation-cap", type=int, default=4_000)
    parser.add_argument("--nsn-cap", type=int, default=120_000)
    return parser.parse_args()


def main():
    args = parse_args()
    source_dir = args.source_dir.resolve()
    required_files = [*SOURCE_VIEWS.values(), "network.parquet"]
    missing = [filename for filename in required_files if not (source_dir / filename).exists()]
    if missing:
        raise SystemExit(f"Missing source files: {', '.join(missing)}")

    with tempfile.TemporaryDirectory(prefix="mimir-public-release-") as output_dir:
        os.environ["LOCAL_CACHE_DIR"] = output_dir
        os.environ["PUBLIC_INTELLIGENCE_COHORT_SIZE"] = str(args.cohort_size)
        os.environ["PUBLIC_INTELLIGENCE_COMPANY_COHORT_SIZE"] = str(args.company_cap)
        os.environ["PUBLIC_INTELLIGENCE_PLATFORM_COHORT_SIZE"] = str(args.platform_cap)
        os.environ["PUBLIC_INTELLIGENCE_AWARD_COHORT_SIZE"] = str(args.award_cap)
        os.environ["PUBLIC_INTELLIGENCE_SOLICITATION_COHORT_SIZE"] = str(args.solicitation_cap)
        os.environ["PUBLIC_INTELLIGENCE_NSN_COHORT_SIZE"] = str(args.nsn_cap)
        os.environ.setdefault("OPENAI_API_KEY", "release-validation-not-used")

        import main as api

        connection = duckdb.connect()
        api.LOCAL_CACHE_DIR = Path(output_dir)
        api._apply_duck_pragmas(connection)
        for view_name, filename in SOURCE_VIEWS.items():
            source = str(source_dir / filename).replace("'", "''")
            connection.execute(
                f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{source}')"
            )
        network_source = str(source_dir / "network.parquet").replace("'", "''")
        connection.execute(f"""
            CREATE OR REPLACE VIEW v_subcontracts AS
            SELECT
                CAST(prime_cage AS VARCHAR) AS prime_cage,
                CAST(sub_cage AS VARCHAR) AS subcontractor_cage,
                CAST(contract_id AS VARCHAR) AS prime_award_id,
                CAST(action_date AS VARCHAR) AS subcontract_action_date,
                TRY_CAST(subaward_value AS DOUBLE) AS subcontract_value_usd
            FROM read_parquet('{network_source}')
        """)

        started_at = time.perf_counter()
        result = api.build_public_intelligence_release(connection)
        elapsed = time.perf_counter() - started_at
        release = result["release"]
        entries = result["entries"]

        assert release["total_entries"] == len(entries)
        assert release["total_entries"] <= release["requested_cohort_size"]
        assert len({(entry["entity_type"], entry["entity_id"]) for entry in entries}) == len(entries)
        assert len({entry["canonical_path"] for entry in entries}) == len(entries)
        assert all(re.fullmatch(r"\d{4}-\d{2}-\d{2}", entry["last_modified"]) for entry in entries)
        assert all(entry["sitemap_batch"] >= 1 for entry in entries)
        assert all(
            release["counts"].get(entity_type, 0) <= release["cohort_caps"].get(entity_type, 0)
            for entity_type in release["counts"]
        )
        assert all(
            release["counts"].get(entity_type, 0) <= release["quality_gate_matches"].get(entity_type, 0)
            for entity_type in release["counts"]
        )
        assert all(
            release["sitemap_batches"].get(entity_type, 0)
            == math.ceil(count / release["sitemap_batch_size"])
            for entity_type, count in release["counts"].items()
        )
        assert all(
            entry["display_name"].strip().upper() not in {"", "NAN", "NONE", "NULL"}
            for entry in entries
            if entry["entity_type"] == "nsn"
        )

        print(json.dumps({
            "validation": "passed",
            "elapsed_seconds": round(elapsed, 3),
            "release": release,
        }, indent=2, default=str))


if __name__ == "__main__":
    main()
