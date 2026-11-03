"""Register an optional disk-only award lookup; retain the analytical view."""
from pathlib import Path
import logging
import shutil


def register_contract_lookup(connection, cache_dir: Path, *, enabled: bool) -> bool:
    connection.execute("DROP VIEW IF EXISTS v_contracts_lookup")
    if cache_dir.exists():
        disk = shutil.disk_usage(cache_dir)
        logging.getLogger("mimir-api").info(
            "Contract lookup storage: filesystem_free_bytes=%s filesystem_total_bytes=%s",
            disk.free, disk.total,
        )
    path = cache_dir / "contracts_lookup.parquet"
    if not enabled or not path.exists():
        return False
    files = sorted(path.glob("*.parquet")) if path.is_dir() else [path]
    if not files:
        raise ValueError("contract lookup asset is empty")
    # Paths are a value parameter; view identifiers are fixed application constants.
    relation = connection.read_parquet([str(p.resolve()) for p in files])
    relation.create_view("v_contracts_lookup")
    source = connection.execute("DESCRIBE v_contracts_rolled").fetchall()
    lookup = connection.execute("DESCRIBE v_contracts_lookup").fetchall()
    if source != lookup:
        connection.execute("DROP VIEW v_contracts_lookup")
        raise ValueError("contract lookup schema differs from the analytical source")
    return True
