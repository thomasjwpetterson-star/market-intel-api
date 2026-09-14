"""Keep public-source text from becoming a formula in downloaded CSV files."""

from typing import Any


def spreadsheet_cell(value: Any) -> Any:
    # Preserve actual numeric measures, including negative obligations. Only
    # text fields need escaping; quoting CSV alone does not disable formulas.
    if isinstance(value, str) and value.lstrip().startswith(("=", "+", "-", "@")):
        return "'" + value
    return value
