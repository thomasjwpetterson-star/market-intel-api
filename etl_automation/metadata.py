"""Metadata stamped onto every generated Parquet object."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import pyarrow.parquet as pq


def parquet_object_metadata(path: str | Path) -> Dict[str, str]:
    """Return compact, S3-safe validation metadata for a Parquet file."""
    parquet = pq.ParquetFile(str(path))
    schema_payload = json.dumps(
        [(field.name, str(field.type), field.nullable) for field in parquet.schema_arrow],
        separators=(",", ":"),
    ).encode("utf-8")
    metadata = {
        "row-count": str(parquet.metadata.num_rows),
        "schema-sha256": hashlib.sha256(schema_payload).hexdigest(),
        "generated-at": datetime.now(timezone.utc).isoformat(),
    }
    run_id = os.getenv("ETL_AUTOMATION_RUN_ID", "").strip()
    if run_id:
        metadata["etl-run-id"] = run_id
    return metadata

