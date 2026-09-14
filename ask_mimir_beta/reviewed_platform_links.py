"""Reviewed, deterministic award-to-platform recovery shared across workflows."""

LINK_VERSION = "reviewed-platform-links-20260915-v4"
CANONICAL_PLATFORM_CORRECTIONS = {
    "COLUMBIA CLASS SSN": "COLUMBIA CLASS SSBN",
    "M109A7 HOWITZER": "M109 PALADIN",
}
REVIEWED_EXACT_RECORD_PATTERNS = {
    "M109 PALADIN": (
        r"M109A7/M992A3 VEHICLE PRODUCTION|"
        r"M109A7 (?:FOV|FAMILY OF VEHICLES) PRODUCTION|"
        r"PALADIN INTEGRATED MANAGEMENT \(PIM\) LOW RATE INITIAL PRODUCTION"
    ),
    "LCAC": (
        r"SHIP[- ]TO[- ]SHORE CONNECTOR|"
        r"(^|[^A-Z0-9])LCAC[ /-]*(?:SSC|1[0-9][0-9]|100[ ]+CLASS)"
        r"([^A-Z0-9]|$)|(^|[^A-Z0-9])SSC[ /-]*LCAC([^A-Z0-9]|$)"
    ),
}


def recovered_platform_sql(alias: str, lane: str) -> str:
    """Fill missing mappings and normalize explicitly reviewed label errors.

    SQL literals below are reviewed source constants, never user input.
    DLA procurement is deliberately excluded from award-description recovery.
    """
    fields = ("prime_award_description", "description") if lane == "network" else ("base_award_description", "action_description", "description")
    description = "UPPER(" + " || ' ' || ".join(f"COALESCE({alias}.{field}, '')" for field in fields) + ")"
    cases = []
    for platform, pattern in REVIEWED_EXACT_RECORD_PATTERNS.items():
        escaped = pattern.replace("'", "''")
        source = "" if lane == "network" else f"{alias}.source_system = 'USA_SPENDING' AND "
        cases.append(f"WHEN {source}REGEXP_MATCHES({description}, '{escaped}') THEN '{platform}'")
    return (
        "CASE " + " ".join(
            f"WHEN UPPER(TRIM({alias}.platform_family)) = '{source}' THEN '{target}'"
            for source, target in CANONICAL_PLATFORM_CORRECTIONS.items()
        ) + f" WHEN UPPER(TRIM(COALESCE({alias}.platform_family, ''))) NOT IN "
        "('', 'UNMAPPED', 'REVIEW NEEDED') "
        f"THEN {alias}.platform_family " + " ".join(cases) + f" ELSE {alias}.platform_family END"
    )
