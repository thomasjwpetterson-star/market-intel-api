"""Compare an isolated current-process control with the automated ETL shadow."""

from __future__ import annotations

import json
import math
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable

import duckdb

from .paths import manual_cache_prefix, staging_cache_prefix
from .release import GENERATED_ARTIFACTS


DEEP_COMPARE_FILES = {
    "summary.parquet",
    "network.parquet",
    "profiles.parquet",
    "transactions.parquet",
    "contracts_rolled.parquet",
    "nsn_summary.parquet",
    "nsn_profile_lookup.parquet",
    "nsn_supplier_lookup.parquet",
    "products.parquet",
    "nsn_cage_reference.parquet",
    "opportunities.parquet",
}

KEY_COLUMNS: dict[str, tuple[str, ...]] = {
    "summary.parquet": ("cage_code", "year"),
    "network.parquet": ("source_report_id", "source_dedup_key", "award_key"),
    "profiles.parquet": ("cage_code", "profile_source"),
    "transactions.parquet": ("transaction_key", "award_key", "cage_code"),
    "contracts_rolled.parquet": ("contract_id", "award_key"),
    "nsn_summary.parquet": ("niin", "year"),
    "nsn_profile_lookup.parquet": ("niin",),
    "nsn_supplier_lookup.parquet": ("niin", "cage_code"),
    "products.parquet": ("niin", "cage", "part_number"),
    "nsn_cage_reference.parquet": ("niin", "cage", "part_number"),
    "opportunities.parquet": ("id", "sol_num"),
}

NUMERIC_TYPES = (
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "FLOAT",
    "DOUBLE",
    "DECIMAL",
)


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def _head(s3: Any, bucket: str, key: str) -> Dict[str, Any]:
    remote = s3.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
    metadata = {
        str(name).lower(): str(value)
        for name, value in (remote.get("Metadata") or {}).items()
    }
    return {
        "key": key,
        "version_id": str(remote.get("VersionId") or ""),
        "size": int(remote.get("ContentLength") or 0),
        "etag": str(remote.get("ETag") or "").strip('"'),
        "sha256": metadata.get("sha256"),
        "row_count": int(metadata["row-count"]) if metadata.get("row-count") else None,
        "schema_sha256": metadata.get("schema-sha256"),
        "etl_run_id": metadata.get("etl-run-id"),
        "last_modified": remote["LastModified"].astimezone(timezone.utc).isoformat(),
    }


def _download_version(
    s3: Any,
    bucket: str,
    remote: Dict[str, Any],
    destination: Path,
) -> None:
    s3.download_file(
        bucket,
        remote["key"],
        str(destination),
        ExtraArgs={"VersionId": remote["version_id"]},
    )


def _schema(connection: duckdb.DuckDBPyConnection, path: Path) -> list[dict[str, str]]:
    rows = connection.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{_sql_path(path)}')"
    ).fetchall()
    return [
        {"name": str(row[0]), "type": str(row[1]), "nullable": str(row[2])}
        for row in rows
    ]


def _deep_metrics(path: Path, filename: str) -> Dict[str, Any]:
    connection = duckdb.connect()
    try:
        schema = _schema(connection, path)
        names = {column["name"] for column in schema}
        expressions = ["COUNT(*) AS row_count"]
        metric_names = ["row_count"]
        for column in schema:
            column_type = column["type"].upper()
            if not column_type.startswith(NUMERIC_TYPES):
                continue
            quoted = _quoted(column["name"])
            safe_name = column["name"].replace(".", "_")
            for suffix, expression in (
                ("non_null", f"COUNT({quoted})"),
                ("sum", f"SUM(TRY_CAST({quoted} AS DOUBLE))"),
                ("min", f"MIN(TRY_CAST({quoted} AS DOUBLE))"),
                ("max", f"MAX(TRY_CAST({quoted} AS DOUBLE))"),
            ):
                metric_names.append(f"numeric.{safe_name}.{suffix}")
                expressions.append(f"{expression}")
        for key in KEY_COLUMNS.get(filename, ()):
            if key not in names:
                continue
            quoted = _quoted(key)
            metric_names.extend(
                (f"key.{key}.non_null", f"key.{key}.distinct")
            )
            expressions.extend((f"COUNT({quoted})", f"COUNT(DISTINCT {quoted})"))
        values = connection.execute(
            f"SELECT {', '.join(expressions)} FROM read_parquet('{_sql_path(path)}')"
        ).fetchone()
        return {
            "schema": schema,
            "metrics": {
                name: value
                for name, value in zip(metric_names, values)
            },
        }
    finally:
        connection.close()


def _different(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left != right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return not math.isclose(float(left), float(right), rel_tol=1e-9, abs_tol=1e-6)
    return left != right


def _metric_deltas(
    control: Dict[str, Any],
    shadow: Dict[str, Any],
) -> list[Dict[str, Any]]:
    deltas = []
    metric_names = sorted(set(control).union(shadow))
    for name in metric_names:
        left = control.get(name)
        right = shadow.get(name)
        if not _different(left, right):
            continue
        delta = None
        delta_percent = None
        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            delta = right - left
            if left:
                delta_percent = (delta / abs(left)) * 100.0
        deltas.append(
            {
                "metric": name,
                "manual": left,
                "automated": right,
                "delta": delta,
                "delta_percent": delta_percent,
            }
        )
    return deltas


def compare_releases(
    s3: Any,
    bucket: str,
    run_id: str,
    deep_files: Iterable[str] = DEEP_COMPARE_FILES,
) -> Dict[str, Any]:
    deep_files = set(deep_files)
    manual_prefix = manual_cache_prefix(run_id)
    automated_prefix = staging_cache_prefix(run_id)
    report: Dict[str, Any] = {
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "manual_prefix": manual_prefix,
        "automated_prefix": automated_prefix,
        "artifacts": [],
    }

    workdir = Path(tempfile.mkdtemp(prefix=f"mimir-compare-{run_id[:12]}-"))
    try:
        for artifact in GENERATED_ARTIFACTS:
            filename = artifact.filename
            manual = _head(s3, bucket, f"{manual_prefix}{filename}")
            automated = _head(s3, bucket, f"{automated_prefix}{filename}")
            item: Dict[str, Any] = {
                "filename": filename,
                "manual": manual,
                "automated": automated,
                "schema_metadata_match": (
                    manual["schema_sha256"] == automated["schema_sha256"]
                ),
                "row_count_match": manual["row_count"] == automated["row_count"],
                "content_hash_match": manual["sha256"] == automated["sha256"],
                "deep_compared": filename in deep_files,
                "metric_deltas": [],
                "generation_lag_seconds": (
                    datetime.fromisoformat(
                        automated["last_modified"].replace("Z", "+00:00")
                    )
                    - datetime.fromisoformat(
                        manual["last_modified"].replace("Z", "+00:00")
                    )
                ).total_seconds(),
            }
            if filename in deep_files:
                manual_path = workdir / f"manual-{filename}"
                automated_path = workdir / f"automated-{filename}"
                _download_version(s3, bucket, manual, manual_path)
                _download_version(s3, bucket, automated, automated_path)
                manual_deep = _deep_metrics(manual_path, filename)
                automated_deep = _deep_metrics(automated_path, filename)
                item["schema_match"] = manual_deep["schema"] == automated_deep["schema"]
                item["metric_deltas"] = _metric_deltas(
                    manual_deep["metrics"],
                    automated_deep["metrics"],
                )
                manual_path.unlink(missing_ok=True)
                automated_path.unlink(missing_ok=True)
            else:
                item["schema_match"] = item["schema_metadata_match"]

            if not item["schema_match"]:
                item["status"] = "SCHEMA_BREAK"
            elif item["metric_deltas"] or not item["row_count_match"]:
                item["status"] = "DATA_DELTA"
            elif not item["content_hash_match"]:
                item["status"] = "CONTENT_ONLY"
            else:
                item["status"] = "MATCH"
            report["artifacts"].append(item)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    status_counts: Dict[str, int] = {}
    for item in report["artifacts"]:
        status_counts[item["status"]] = status_counts.get(item["status"], 0) + 1
    report["summary"] = {
        "artifact_count": len(report["artifacts"]),
        "status_counts": status_counts,
        "requires_review": bool(
            status_counts.get("SCHEMA_BREAK") or status_counts.get("DATA_DELTA")
        ),
    }
    report_key = f"mimir/comparisons/{run_id}/comparison_report.json"
    s3.put_object(
        Bucket=bucket,
        Key=report_key,
        Body=(json.dumps(report, indent=2, default=str) + "\n").encode("utf-8"),
        ContentType="application/json",
    )
    report["report_key"] = report_key
    return report
