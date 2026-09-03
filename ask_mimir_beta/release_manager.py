"""Validate and atomically promote a frozen Ask Mimir metric release."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, Tuple

from materialize import file_sha256


REQUIRED_OUTPUTS = {
    "primitives": "derived_metric_primitives.parquet",
    "components": "derived_metric_components.parquet",
}


def validate_staging(staging_dir: Path, evidence_file: Path) -> Dict[str, Any]:
    manifest_path = staging_dir / "manifest.json"
    if not manifest_path.exists():
        raise ValueError(f"manifest is missing: {manifest_path}")
    manifest = json.loads(manifest_path.read_text())
    if not manifest.get("release_id"):
        raise ValueError("manifest release_id is missing")
    if file_sha256(evidence_file) != manifest["source"]["sha256"]:
        raise ValueError("evidence ledger hash does not match the manifest")
    for output_name, filename in REQUIRED_OUTPUTS.items():
        output_path = staging_dir / filename
        if not output_path.exists():
            raise ValueError(f"release output is missing: {output_path}")
        expected = manifest["outputs"][output_name]["sha256"]
        if file_sha256(output_path) != expected:
            raise ValueError(f"{output_name} hash does not match the manifest")
    return manifest


def promote_release(staging_dir: Path, release_root: Path, evidence_file: Path) -> Path:
    staging_dir = staging_dir.resolve()
    release_root = release_root.resolve()
    evidence_file = evidence_file.resolve()
    manifest = validate_staging(staging_dir, evidence_file)
    release_id = str(manifest["release_id"])
    releases_dir = release_root / "releases"
    final_dir = releases_dir / release_id
    if final_dir.exists():
        raise ValueError(f"immutable release already exists: {final_dir}")

    releases_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(tempfile.mkdtemp(prefix=f".{release_id}-", dir=releases_dir))
    try:
        for filename in [*REQUIRED_OUTPUTS.values(), "sample_metric_observations.json"]:
            source = staging_dir / filename
            if source.exists():
                shutil.copy2(source, temp_dir / filename)

        evidence_dir = temp_dir / "evidence"
        evidence_dir.mkdir()
        frozen_evidence = evidence_dir / "transactions.parquet"
        try:
            os.link(evidence_file, frozen_evidence)
        except OSError:
            os.symlink(evidence_file, frozen_evidence)

        manifest["source"]["path"] = "evidence/transactions.parquet"
        for output_name, filename in REQUIRED_OUTPUTS.items():
            manifest["outputs"][output_name]["path"] = filename
        manifest["outputs"]["sample_observations"] = "sample_metric_observations.json"
        (temp_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        os.replace(temp_dir, final_dir)
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise

    pointer = {
        "release_id": release_id,
        "release_path": f"releases/{release_id}",
        "manifest_sha256": file_sha256(final_dir / "manifest.json"),
    }
    pointer_temp = release_root / ".active_release.json.tmp"
    pointer_temp.write_text(json.dumps(pointer, indent=2) + "\n")
    os.replace(pointer_temp, release_root / "active_release.json")
    return final_dir


def resolve_active_release(release_root: Path) -> Tuple[Path, Path]:
    release_root = release_root.resolve()
    pointer = json.loads((release_root / "active_release.json").read_text())
    release_dir = (release_root / pointer["release_path"]).resolve()
    manifest_path = release_dir / "manifest.json"
    if file_sha256(manifest_path) != pointer["manifest_sha256"]:
        raise ValueError("active release manifest does not match the atomic pointer")
    manifest = json.loads(manifest_path.read_text())
    if manifest["release_id"] != pointer["release_id"]:
        raise ValueError("active release ID does not match its manifest")
    evidence_file = (release_dir / manifest["source"]["path"]).resolve()
    if file_sha256(evidence_file) != manifest["source"]["sha256"]:
        raise ValueError("active evidence ledger does not match its release manifest")
    for output_name, filename in REQUIRED_OUTPUTS.items():
        output_path = release_dir / filename
        if file_sha256(output_path) != manifest["outputs"][output_name]["sha256"]:
            raise ValueError(f"active {output_name} file does not match its release manifest")
    return release_dir, evidence_file


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--staging-dir", required=True, type=Path)
    parser.add_argument("--release-root", required=True, type=Path)
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    promoted = promote_release(args.staging_dir, args.release_root, args.evidence)
    print(json.dumps({"promoted_release": str(promoted)}, indent=2))


if __name__ == "__main__":
    main()
