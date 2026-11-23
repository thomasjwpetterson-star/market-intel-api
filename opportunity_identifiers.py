"""Preserve opportunity classification codes as identifiers across old caches."""
import math
import re


def normalize_opportunity_naics(value):
    """Remove a numeric-storage suffix without padding or inventing a code.

    SAM defines NAICS as a string of at most six digits. Unknown nonempty
    values remain available for review; missing codes are never inferred.
    """
    if value is None or isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip()
    if text == '<NA>':
        return None
    match = re.fullmatch(r'([0-9]{1,6})[.]0+', text)
    return match.group(1) if match else text


def opportunity_naics_sql(column='naics'):
    """DuckDB expression for an internally named identifier column."""
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', column):
        raise ValueError('Expected an internal SQL column name')
    value = f'TRIM(CAST({column} AS VARCHAR))'
    return (f"CASE WHEN REGEXP_FULL_MATCH({value}, '[0-9]{{1,6}}[.]0+') "
            f"THEN REGEXP_REPLACE({value}, '[.]0+$', '') ELSE {value} END")


def validate_opportunity_naics_codes(values):
    """Stop a refreshed opportunity cache publishing noncanonical identifiers.

    Missing codes stay missing. A fractional, scientific or other ambiguous
    source value needs source review rather than a guessed replacement.
    """
    invalid = 0
    for value in values:
        if value is None or str(value) in ('', '<NA>'):
            continue
        if not isinstance(value, str) or not re.fullmatch(r'[0-9]{1,6}', value):
            invalid += 1
    if invalid:
        raise ValueError(f'Opportunity cache has {invalid} noncanonical NAICS identifiers; source review required')


def opportunity_view_sql(file_path):
    """Keep native rows and every other field while adapting legacy DOUBLEs."""
    if not isinstance(file_path, str) or not file_path:
        raise ValueError('Expected an internal opportunity cache path')
    path = file_path.replace("'", "''")
    return ('CREATE OR REPLACE VIEW v_opportunities AS SELECT * REPLACE ('
            + opportunity_naics_sql() + " AS naics) FROM read_parquet('" + path + "')")
