"""Safely validate and repair the Athena serving-view type boundary.

This utility is intentionally narrow.  It preserves the SQL currently stored in
Glue and changes only the unstable ``unit_price_status`` projection in
``global_spend_transactions``.  Before applying anything to the live names it
creates and checks a complete shadow of the affected serving chain.
"""

from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import time
from typing import Any

import boto3


DATABASE = "market_intel_gold"
SOURCE_VIEW = "global_spend_transactions"
MASTER_VIEW = "dashboard_master_view"
SUMMARY_VIEW = "dashboard_summary_v2"
SHADOW_SUFFIX = "_repaircheck_20260923"
EXPECTED_OLD_TYPE = "varchar(50)"
REPAIRED_TYPE = "varchar(64)"
SOURCE_CANDIDATE = "global_spend_transactions_attribution_candidate"
MASTER_CANDIDATE = "dashboard_master_view_attribution_candidate"


def decode_original_sql(table: dict[str, Any]) -> str:
    """Decode Athena's Presto-view envelope from a Glue table response."""
    original = table.get("ViewOriginalText", "")
    match = re.search(r"Presto View:\s*([A-Za-z0-9+/=]+)", original)
    if not match:
        raise ValueError(f"{table.get('Name')} is not a decodable Athena view")
    payload = json.loads(base64.b64decode(match.group(1)))
    return str(payload["originalSql"])


def schema(table: dict[str, Any]) -> list[tuple[str, str]]:
    columns = table.get("StorageDescriptor", {}).get("Columns", [])
    return [(str(column["Name"]), str(column["Type"])) for column in columns]


def repair_source_sql(sql: str) -> str:
    """Widen exactly the outer unit-price-status projection."""
    pattern = r"(?m)^(\s*,\s*)p\.unit_price_status\s*$"
    replacement = (
        r"\1CAST(p.unit_price_status AS VARCHAR(64)) AS unit_price_status"
    )
    repaired, count = re.subn(pattern, replacement, sql)
    if count != 1:
        raise ValueError(
            "Expected exactly one outer p.unit_price_status projection; "
            f"found {count}"
        )
    return repaired


def select_sql_from_ddl(path: Path) -> str:
    """Load the SELECT body from a checked-in CREATE OR REPLACE VIEW file."""
    ddl = path.read_text()
    pattern = re.compile(
        r'^\s*CREATE\s+OR\s+REPLACE\s+VIEW\s+[^\s]+\s+AS\s+',
        re.IGNORECASE,
    )
    sql, count = pattern.subn("", ddl, count=1)
    if count != 1:
        raise ValueError(f"Not a CREATE OR REPLACE VIEW file: {path}")
    return sql.rstrip().removesuffix(";").rstrip()


def replace_relation(sql: str, source: str, target: str) -> str:
    """Replace one exact, optionally quoted Athena relation name."""
    patterns = (
        (f'"{DATABASE}"."{source}"', f'"{DATABASE}"."{target}"'),
        (f"{DATABASE}.{source}", f"{DATABASE}.{target}"),
        (f'"{source}"', f'"{target}"'),
        (source, target),
    )
    for old, new in patterns:
        if old in sql:
            replaced = sql.replace(old, new)
            if replaced.count(target) != sql.count(target) + sql.count(old):
                raise ValueError(f"Ambiguous relation replacement for {source}")
            return replaced
    raise ValueError(f"Relation {source} not found in view SQL")


def schema_delta(
    before: list[tuple[str, str]], after: list[tuple[str, str]]
) -> list[dict[str, str | None]]:
    before_map = dict(before)
    after_map = dict(after)
    names = dict.fromkeys([name for name, _ in before] + [name for name, _ in after])
    return [
        {"column": name, "before": before_map.get(name), "after": after_map.get(name)}
        for name in names
        if before_map.get(name) != after_map.get(name)
    ]


def assert_preserves_schemas(
    actual: list[tuple[str, str]],
    required_schemas: list[list[tuple[str, str]]],
    widened: dict[str, str] | None = None,
) -> None:
    """Require a proposed merged schema to retain every prior public column."""
    actual_map = dict(actual)
    expected_overrides = widened or {}
    errors = []
    for required in required_schemas:
        for name, old_type in required:
            expected_type = expected_overrides.get(name, old_type)
            actual_type = actual_map.get(name)
            if actual_type != expected_type:
                errors.append(
                    {"column": name, "expected": expected_type, "actual": actual_type}
                )
    if errors:
        raise RuntimeError(f"Merged schema does not preserve prior contracts: {errors}")


class Repair:
    def __init__(
        self,
        bucket: str,
        region: str,
        change_id: str,
        source_ddl: Path,
        master_ddl: Path,
    ) -> None:
        self.bucket = bucket
        self.region = region
        self.change_id = change_id
        self.proposed_source_sql = select_sql_from_ddl(source_ddl)
        self.proposed_master_sql = select_sql_from_ddl(master_ddl)
        self.glue = boto3.client("glue", region_name=region)
        self.athena = boto3.client("athena", region_name=region)
        self.s3 = boto3.client("s3", region_name=region)
        self.snapshot_prefix = f"mimir/change-control/{change_id}/athena-view-repair"

    def table(self, name: str) -> dict[str, Any]:
        return self.glue.get_table(DatabaseName=DATABASE, Name=name)["Table"]

    def all_views(self) -> dict[str, dict[str, Any]]:
        paginator = self.glue.get_paginator("get_tables")
        result: dict[str, dict[str, Any]] = {}
        for page in paginator.paginate(DatabaseName=DATABASE):
            for table in page["TableList"]:
                if table.get("TableType") == "VIRTUAL_VIEW":
                    result[str(table["Name"])] = table
        return result

    def direct_dependents(
        self, views: dict[str, dict[str, Any]], source: str
    ) -> list[str]:
        pattern = re.compile(
            rf"(?i)(?<![A-Za-z0-9_]){re.escape(source)}(?![A-Za-z0-9_])"
        )
        return sorted(
            name
            for name, table in views.items()
            if name != source and pattern.search(decode_original_sql(table))
        )

    def snapshot(self, names: list[str]) -> None:
        manifest: dict[str, Any] = {
            "change_id": self.change_id,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "database": DATABASE,
            "views": {},
        }
        for name in names:
            table = self.table(name)
            sql = decode_original_sql(table)
            table_json = json.dumps(table, default=str, indent=2).encode()
            self.s3.put_object(
                Bucket=self.bucket,
                Key=f"{self.snapshot_prefix}/{name}.glue.json",
                Body=table_json,
                ContentType="application/json",
            )
            self.s3.put_object(
                Bucket=self.bucket,
                Key=f"{self.snapshot_prefix}/{name}.sql",
                Body=sql.encode(),
                ContentType="text/plain",
            )
            manifest["views"][name] = {"schema": schema(table)}
        self.s3.put_object(
            Bucket=self.bucket,
            Key=f"{self.snapshot_prefix}/manifest.json",
            Body=json.dumps(manifest, indent=2).encode(),
            ContentType="application/json",
        )

    def query(self, sql: str, label: str, timeout_seconds: int = 900) -> str:
        response = self.athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": DATABASE},
            ResultConfiguration={
                "OutputLocation": f"s3://{self.bucket}/temp_etl/view_repair/"
            },
            ClientRequestToken=(
                f"mimir-{self.change_id}-{label}".replace("_", "-")[:128]
            ),
        )
        query_id = response["QueryExecutionId"]
        deadline = time.monotonic() + timeout_seconds
        while True:
            execution = self.athena.get_query_execution(QueryExecutionId=query_id)[
                "QueryExecution"
            ]
            status = execution["Status"]
            state = status["State"]
            if state == "SUCCEEDED":
                return query_id
            if state in {"FAILED", "CANCELLED"}:
                raise RuntimeError(
                    f"Athena {label} {state}: "
                    f"{status.get('StateChangeReason', 'unknown failure')}"
                )
            if time.monotonic() >= deadline:
                self.athena.stop_query_execution(QueryExecutionId=query_id)
                raise TimeoutError(f"Athena {label} timed out: {query_id}")
            time.sleep(2)

    def create_view(self, name: str, sql: str, label: str) -> str:
        return self.query(
            f'CREATE OR REPLACE VIEW "{DATABASE}"."{name}" AS\n{sql}', label
        )

    def validate_shadows(self) -> dict[str, Any]:
        views = self.all_views()
        source_dependents = self.direct_dependents(views, SOURCE_VIEW)
        if source_dependents != [MASTER_VIEW]:
            raise RuntimeError(
                "Unexpected direct production dependencies for "
                f"{SOURCE_VIEW}: {source_dependents}"
            )
        master_dependents = self.direct_dependents(views, MASTER_VIEW)
        if SUMMARY_VIEW not in master_dependents:
            raise RuntimeError(f"{SUMMARY_VIEW} no longer depends on {MASTER_VIEW}")

        snapshot_names = [
            SOURCE_VIEW,
            SOURCE_CANDIDATE,
            MASTER_VIEW,
            MASTER_CANDIDATE,
        ] + master_dependents
        self.snapshot(snapshot_names)

        source_shadow = SOURCE_VIEW + SHADOW_SUFFIX
        master_shadow = MASTER_VIEW + SHADOW_SUFFIX
        shadow_master_sql = replace_relation(
            self.proposed_master_sql, SOURCE_VIEW, source_shadow
        )

        query_ids: dict[str, str] = {}
        query_ids[source_shadow] = self.create_view(
            source_shadow, self.proposed_source_sql, "create-source-shadow"
        )
        query_ids[master_shadow] = self.create_view(
            master_shadow, shadow_master_sql, "create-master-shadow"
        )

        shadow_source_schema = schema(self.table(source_shadow))
        shadow_master_schema = schema(self.table(master_shadow))
        source_delta = schema_delta(schema(views[SOURCE_VIEW]), shadow_source_schema)
        master_delta = schema_delta(schema(views[MASTER_VIEW]), shadow_master_schema)
        assert_preserves_schemas(
            shadow_source_schema,
            [schema(views[SOURCE_VIEW]), schema(views[SOURCE_CANDIDATE])],
            {"unit_price_status": REPAIRED_TYPE},
        )
        assert_preserves_schemas(
            shadow_master_schema,
            [schema(views[MASTER_VIEW]), schema(views[MASTER_CANDIDATE])],
            {"unit_price_status": REPAIRED_TYPE},
        )

        downstream_results: dict[str, Any] = {}
        for name in master_dependents:
            shadow_name = name + SHADOW_SUFFIX
            original = decode_original_sql(views[name])
            shadow_sql = replace_relation(original, MASTER_VIEW, master_shadow)
            query_ids[shadow_name] = self.create_view(
                shadow_name, shadow_sql, f"create-{name[:60]}-shadow"
            )
            delta = schema_delta(schema(views[name]), schema(self.table(shadow_name)))
            if delta:
                raise RuntimeError(f"Unexpected downstream schema change for {name}: {delta}")
            downstream_results[name] = {"shadow": shadow_name, "schema_delta": delta}

        for name in (source_shadow, master_shadow, SUMMARY_VIEW + SHADOW_SUFFIX):
            query_ids[f"smoke:{name}"] = self.query(
                f'SELECT 1 FROM "{DATABASE}"."{name}" LIMIT 1',
                f"smoke-{name[:70]}",
            )

        return {
            "snapshot": f"s3://{self.bucket}/{self.snapshot_prefix}/manifest.json",
            "source_dependents": source_dependents,
            "master_dependents": master_dependents,
            "source_schema_delta": source_delta,
            "master_schema_delta": master_delta,
            "downstream": downstream_results,
            "query_ids": query_ids,
        }

    def apply(self) -> dict[str, Any]:
        validation = self.validate_shadows()
        source = self.table(SOURCE_VIEW)
        source_candidate = self.table(SOURCE_CANDIDATE)
        master = self.table(MASTER_VIEW)
        master_candidate = self.table(MASTER_CANDIDATE)

        query_ids = {
            SOURCE_VIEW: self.create_view(
                SOURCE_VIEW, self.proposed_source_sql, "apply-source"
            ),
            MASTER_VIEW: self.create_view(
                MASTER_VIEW, self.proposed_master_sql, "apply-master"
            ),
        }
        live_source_schema = schema(self.table(SOURCE_VIEW))
        live_master_schema = schema(self.table(MASTER_VIEW))
        live_source_delta = schema_delta(schema(source), live_source_schema)
        live_master_delta = schema_delta(schema(master), live_master_schema)
        assert_preserves_schemas(
            live_source_schema,
            [schema(source), schema(source_candidate)],
            {"unit_price_status": REPAIRED_TYPE},
        )
        assert_preserves_schemas(
            live_master_schema,
            [schema(master), schema(master_candidate)],
            {"unit_price_status": REPAIRED_TYPE},
        )

        live_checks = [SOURCE_VIEW, MASTER_VIEW, SUMMARY_VIEW]
        live_checks.extend(validation["master_dependents"])
        for name in dict.fromkeys(live_checks):
            limit = 1 if name in {SOURCE_VIEW, MASTER_VIEW, SUMMARY_VIEW} else 0
            query_ids[f"smoke:{name}"] = self.query(
                f'SELECT 1 FROM "{DATABASE}"."{name}" LIMIT {limit}',
                f"live-smoke-{name[:70]}",
            )

        return {
            "validation": validation,
            "live_source_schema_delta": live_source_delta,
            "live_master_schema_delta": live_master_delta,
            "live_query_ids": query_ids,
        }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--change-id", required=True)
    parser.add_argument(
        "--source-ddl", type=Path, default=Path("athena_01_global_spend_transactions.sql")
    )
    parser.add_argument(
        "--master-ddl", type=Path, default=Path("athena_02_dashboard_master_view.sql")
    )
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    repair = Repair(
        args.bucket,
        args.region,
        args.change_id,
        args.source_ddl,
        args.master_ddl,
    )
    result = repair.apply() if args.apply else repair.validate_shadows()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
