"""Deadline predicates for DuckDB's active solicitation feeds."""

EASTERN_TODAY_SQL = "CAST(CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York' AS DATE)"


def active_solicitation_deadline_sql(column: str = "deadline") -> str:
    """Compare offset-aware timestamps precisely and keep date-only precision.

    The column expression is supplied by the internal query, never a request.
    A calendar date without a published timezone stays eligible through that
    date in the U.S. Eastern calendar used by the opportunity listing.
    """
    value = f"TRIM(CAST({column} AS VARCHAR))"
    return f"""CASE
        WHEN REGEXP_MATCHES({value}, '[T ][0-9]{{2}}:[0-9]{{2}}.*([Zz]|[+-][0-9]{{2}}(:?[0-9]{{2}})?)$')
            THEN TRY_CAST({column} AS TIMESTAMPTZ) > CURRENT_TIMESTAMP
        WHEN REGEXP_FULL_MATCH({value}, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}([T ][0-9]{{2}}:[0-9]{{2}}(:[0-9]{{2}}([.][0-9]+)?)?)?')
            THEN TRY_CAST(SUBSTR({value}, 1, 10) AS DATE) >= {EASTERN_TODAY_SQL}
        ELSE FALSE
    END"""
