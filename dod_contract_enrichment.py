"""Contract-level lookup for official DoD announcement enrichment."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List

import duckdb


def normalize_contract_id(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def lookup_contract_announcements(
    parquet_path: Path,
    contract_id: str,
    *,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    normalized = normalize_contract_id(contract_id)
    if not normalized or not parquet_path.exists():
        return []

    bounded_limit = min(max(int(limit), 1), 25)
    connection = duckdb.connect()
    try:
        cursor = connection.execute(
            """
            SELECT announcement_id,
                   CAST(announcement_date AS VARCHAR) AS announcement_date,
                   service,
                   entry_type,
                   recipient_text,
                   primary_contract_id,
                   announced_value_usd,
                   obligated_at_announcement_usd,
                   work_locations,
                   completion_text,
                   competition_text,
                   contracting_activity,
                   description,
                   source_title,
                   source_url,
                   source_published_at
            FROM read_parquet(?) a
            WHERE REGEXP_REPLACE(
                      UPPER(COALESCE(a.primary_contract_id, '')),
                      '[^A-Z0-9]', '', 'g'
                  ) = ?
               OR EXISTS (
                    SELECT 1
                    FROM UNNEST(a.contract_ids) AS ids(contract_id)
                    WHERE REGEXP_REPLACE(
                              UPPER(COALESCE(ids.contract_id, '')),
                              '[^A-Z0-9]', '', 'g'
                          ) = ?
               )
            ORDER BY announcement_date DESC, entry_index DESC
            LIMIT ?
            """,
            [str(parquet_path), normalized, normalized, bounded_limit],
        )
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        connection.close()


def lookup_scope_announcements(
    parquet_path: Path,
    transactions_path: Path,
    *,
    cage: str | None = None,
    company_name: str | None = None,
    platform: str | None = None,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """Return announcements linked to USAspending contracts in a resolved scope."""

    if not parquet_path.exists() or not transactions_path.exists():
        return []

    filters: List[str] = []
    params: List[Any] = [str(transactions_path)]
    clean_cage = str(cage or "").strip().upper()
    clean_name = str(company_name or "").strip().upper()
    clean_platform = str(platform or "").strip().upper()

    if clean_cage and clean_cage != "AGGREGATE":
        filters.append("UPPER(TRIM(COALESCE(t.vendor_cage, ''))) = ?")
        params.append(clean_cage)
    elif clean_name:
        filters.append("POSITION(? IN UPPER(COALESCE(t.vendor_name, ''))) > 0")
        params.append(clean_name)

    if clean_platform:
        filters.append("UPPER(TRIM(COALESCE(t.platform_family, ''))) = ?")
        params.append(clean_platform)

    if not filters:
        return []

    bounded_limit = min(max(int(limit), 1), 25)
    params.extend([str(parquet_path), bounded_limit])
    connection = duckdb.connect()
    try:
        cursor = connection.execute(
            f"""
            WITH scope_contracts AS (
                SELECT DISTINCT REGEXP_REPLACE(
                           UPPER(COALESCE(CAST(t.contract_id AS VARCHAR), '')),
                           '[^A-Z0-9]', '', 'g'
                       ) AS normalized_contract_id
                FROM read_parquet(?) t
                WHERE {' AND '.join(filters)}
                  AND COALESCE(CAST(t.contract_id AS VARCHAR), '') <> ''
            )
            SELECT announcement_id,
                   CAST(announcement_date AS VARCHAR) AS announcement_date,
                   service,
                   entry_type,
                   recipient_text,
                   primary_contract_id,
                   announced_value_usd,
                   obligated_at_announcement_usd,
                   work_locations,
                   completion_text,
                   competition_text,
                   contracting_activity,
                   description,
                   source_title,
                   source_url,
                   source_published_at
            FROM read_parquet(?) a
            WHERE REGEXP_REPLACE(
                      UPPER(COALESCE(a.primary_contract_id, '')),
                      '[^A-Z0-9]', '', 'g'
                  ) IN (SELECT normalized_contract_id FROM scope_contracts)
               OR EXISTS (
                    SELECT 1
                    FROM UNNEST(a.contract_ids) AS ids(contract_id)
                    WHERE REGEXP_REPLACE(
                              UPPER(COALESCE(ids.contract_id, '')),
                              '[^A-Z0-9]', '', 'g'
                          ) IN (
                              SELECT normalized_contract_id FROM scope_contracts
                          )
               )
            ORDER BY announcement_date DESC, entry_index DESC
            LIMIT ?
            """,
            params,
        )
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        connection.close()
