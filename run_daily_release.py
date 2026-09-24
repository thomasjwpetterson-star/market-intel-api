"""Run source ETL and public serving materialisation as isolated processes."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parent


def main() -> None:
    # Process isolation releases the ETL's large pandas frames before DuckDB
    # starts materialising the public page corpus.
    subprocess.run([sys.executable, str(ROOT / "run_etl.py")], check=True)
    subprocess.run(
        [sys.executable, str(ROOT / "materialize_public_intelligence_release.py")],
        check=True,
    )


if __name__ == "__main__":
    main()
