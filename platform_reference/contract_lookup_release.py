"""Attach a verified disk lookup to a schema-2 candidate, never to a live pointer.

The bounded external builder must run for each new analytical source. A missing
lookup is backwards compatible; a lookup from a different source is rejected.
"""
from copy import deepcopy

try:
    from .main_release_cache import asset_fingerprint, validate_manifest
except ImportError:  # Standalone promotion CLI uses this directory on sys.path.
    from main_release_cache import asset_fingerprint, validate_manifest


def validate_automated_lookup(manifest: dict, evidence: dict) -> None:
    """Require fresh source-bound parity and bounded offline handler/load proof."""
    entries = validate_manifest(manifest)
    lookup = entries.get("contracts_lookup.parquet")
    source_sha = asset_fingerprint(entries["contracts_rolled.parquet"])
    if not lookup or evidence.get("source_asset_sha256") != source_sha:
        raise ValueError("promotion requires a lookup bound to this Main source")
    rebuilt = with_contract_lookup(manifest, evidence.get("exact_parity", {}), manifest["release_id"])
    expected = next(row for row in rebuilt["files"] if row["local_path"] == "contracts_lookup.parquet")
    if expected != lookup:
        raise ValueError("lookup manifest differs from parity proof")
    timing = evidence.get("offline_queries", {})
    resources = evidence.get("query_resources", {})
    if (timing.get("state") != "PASSED" or timing.get("all_values_equal") is not True or
            timing.get("analytic_source_unchanged") is not True or
            timing.get("actual_profile_dto_parity", 0) < 1 or
            timing.get("mixed", {}).get("count", 0) < 1 or
            resources.get("exit_code") != 0 or resources.get("stop_guard") or
            resources.get("peak_process_rss_bytes", 2**63) > 1536 * 1024**2):
        raise ValueError("lookup offline handler/load or memory proof failed")


def with_contract_lookup(manifest: dict, proof: dict, release_id: str) -> dict:
    entries = validate_manifest(manifest)
    if proof.get("state") != "ALL_PARTS_EXACT_PARITY_PASSED":
        raise ValueError("lookup release requires complete exact parity")
    if proof.get("release_id") != manifest["release_id"]:
        raise ValueError("lookup proof belongs to another Main release")
    source = entries["contracts_rolled.parquet"]
    parts = proof.get("parts", [])
    originals = source.get("parts", [source])
    if len(parts) != len(originals):
        raise ValueError("lookup proof omits source partitions")
    for index, (result, original) in enumerate(zip(parts, originals)):
        if result.get("index") != index or result.get("source") != original:
            raise ValueError("lookup source partition order differs")
        if not result.get("all_values_equal") or not result.get("duplicate_id_selection_order_preserved"):
            raise ValueError("lookup field or selection parity failed")
        if result.get("exit_code") != 0 or result.get("stop_guard"):
            raise ValueError("lookup resource guard stopped a partition")
        if result.get("rows_compared", 0) <= 0:
            raise ValueError("lookup partition is empty")
    candidate = deepcopy(manifest)
    candidate.update(release_id=release_id, release_status="CANDIDATE_ONLY", customer_facing=False)
    candidate["files"] = [row for row in candidate["files"] if row["local_path"] != "contracts_lookup.parquet"]
    candidate["files"].append({"local_path": "contracts_lookup.parquet",
        "platform_reference_release_id": manifest["platform_reference_release_id"],
        "source_asset_sha256": asset_fingerprint(source),
        "parts": [result["candidate"] for result in parts]})
    candidate["contract_lookup_validation"] = {
        "source_release_id": manifest["release_id"],
        "rows_compared": sum(result["rows_compared"] for result in parts),
        "all_values_equal": True, "duplicate_id_selection_order_preserved": True,
        "analytical_files_unchanged": True,
    }
    validate_manifest(candidate)
    return candidate
