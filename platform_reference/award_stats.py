"""Count contract rows from the active Main release without scanning their data."""

from pathlib import Path

import pyarrow.parquet as pq


def count_contract_rows(cache_dir: Path) -> int:
    single_or_parts = cache_dir / "contracts_rolled.parquet"
    legacy_parts = cache_dir / "contracts_rolled"
    if single_or_parts.is_file():
        paths = [single_or_parts]
    elif single_or_parts.is_dir():
        paths = sorted(single_or_parts.rglob("*.parquet"))
    elif legacy_parts.is_dir():
        paths = sorted(legacy_parts.rglob("*.parquet"))
    else:
        raise FileNotFoundError("active Main release has no contracts_rolled Parquet cache")
    if not paths:
        raise FileNotFoundError("active Main release has an empty contracts_rolled Parquet cache")
    total = sum(int(pq.ParquetFile(str(path)).metadata.num_rows) for path in paths)
    if total <= 0:
        raise ValueError("active Main release has no contract rows")
    return total
