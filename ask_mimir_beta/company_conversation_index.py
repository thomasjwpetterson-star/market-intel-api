"""Release-built, CAGE-sorted transaction slice for cold company conversations."""

from __future__ import annotations

import json
import os
import re
import shutil
from pathlib import Path
from typing import Any

import duckdb

from company_context import file_sha256, verified_runtime_file_hashes
from reviewed_platform_links import LINK_VERSION, recovered_platform_sql


INDEX_VERSION = "company-conversation-transactions-v1"
INDEX_FILENAME = "company_conversation_transactions.parquet"
META_FILENAME = "company_conversation_transactions.json"


def usable_transaction_index(data_root: Path) -> Path | None:
    """Use an index only when it belongs to the bootstrapped source release."""
    data_root = data_root.resolve()
    raw_path = data_root / "transactions.parquet"
    index_path = data_root / INDEX_FILENAME
    meta_path = data_root / META_FILENAME
    try:
        metadata: dict[str, Any] = json.loads(meta_path.read_text())
        release_hashes = verified_runtime_file_hashes(data_root)
        if (
            metadata.get("index_version") == INDEX_VERSION
            and metadata.get("platform_link_version") == LINK_VERSION
            and metadata.get("source_sha256") == release_hashes.get(raw_path)
            and release_hashes.get(index_path)
            and release_hashes.get(meta_path)
            and index_path.is_file()
        ):
            return index_path
    except (OSError, ValueError, json.JSONDecodeError):
        pass
    return None


def build_transaction_index(
    data_root: Path, *, memory_limit: str = "1GB", temp_directory: Path | None = None
) -> dict[str, Any]:
    """Sort the narrow evidence columns once during release creation."""
    if os.getenv("RENDER_SERVICE_ID"):
        raise RuntimeError(
            "Build the company Conversation index in dedicated ETL, not on a Render serving instance"
        )
    data_root = data_root.resolve()
    raw_path = data_root / "transactions.parquet"
    if not raw_path.is_file():
        raise FileNotFoundError(raw_path)
    if not re.fullmatch(r"\d+(?:MB|GB)", memory_limit.upper()):
        raise ValueError("memory_limit must use MB or GB")
    source_hash = file_sha256(raw_path)
    output = data_root / INDEX_FILENAME
    temporary = output.with_suffix(".parquet.tmp")
    temporary.unlink(missing_ok=True)
    scratch = temp_directory or (data_root.parent / "company-conversation-index-scratch")
    scratch.mkdir(parents=True, exist_ok=True)
    required_free = max(256 * 1024**2, 4 * raw_path.stat().st_size)
    if shutil.disk_usage(scratch).free < required_free:
        raise RuntimeError(
            "Not enough scratch space to build the company Conversation index"
        )
    connection = duckdb.connect()
    try:
        connection.execute("SET preserve_insertion_order = false")
        connection.execute("SET threads = 2")
        connection.execute(f"SET memory_limit = '{memory_limit.upper()}'")
        connection.execute("SET temp_directory = ?", [str(scratch)])
        source = str(raw_path).replace("'", "''")
        target = str(temporary).replace("'", "''")
        connection.execute(f"""
            COPY (
                SELECT vendor_cage, source_system, award_key, transaction_key,
                       contract_id, action_date, sub_agency, parent_agency,
                       base_award_description, description, spend_amount, psc,
                       year, niin, nsn,
                       {recovered_platform_sql('raw', 'transactions')} AS platform_family
                FROM read_parquet('{source}') raw
                WHERE source_system IN ('USA_SPENDING', 'DLA')
                ORDER BY vendor_cage, year
            ) TO '{target}' (
                FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 16384
            )
        """)
        temporary.replace(output)
    finally:
        connection.close()
        temporary.unlink(missing_ok=True)
    if file_sha256(raw_path) != source_hash:
        output.unlink(missing_ok=True)
        raise RuntimeError("Transaction source changed during Conversation index build")
    metadata = {
        "index_version": INDEX_VERSION,
        "platform_link_version": LINK_VERSION,
        "source_sha256": source_hash,
        "source_size_bytes": raw_path.stat().st_size,
        "index_size_bytes": output.stat().st_size,
    }
    meta_path = data_root / META_FILENAME
    temporary_meta = meta_path.with_suffix(".json.tmp")
    temporary_meta.write_text(json.dumps(metadata, sort_keys=True) + "\n")
    temporary_meta.replace(meta_path)
    return metadata
