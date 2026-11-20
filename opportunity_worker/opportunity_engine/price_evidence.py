"""Reject non-procurement valuations in frozen and future price evidence."""
def non_procurement_document_class(attachment_sha256, attachment_name, source_row_text=""):
    """Document meaning takes precedence over valid numeric row arithmetic.

    The exact reviewed hash remains excluded if renamed. Filename matches flag
    similar property documents for review, without claiming a verified value
    basis for a different source. A property clause in an executed line alone
    is not enough to reject a genuine purchase.
    """
    import re

    if attachment_sha256 == (
        "84075394258b3f88d553c39b5950b58e045113c016aaaf4f270b5005f268a967"
    ):
        return "GOVERNMENT_FURNISHED_PROPERTY_INVENTORY"
    name = re.sub(r"[^A-Z0-9]+", " ", str(attachment_name or "").upper()).strip()
    row = re.sub(r"[^A-Z0-9]+", " ", str(source_row_text or "").upper()).strip()
    if re.search(r"\bGOVERNMENT FURNISHED PROPERTY\b|\bGFP\b", name):
        return "NON_PROCUREMENT_DOCUMENT_REQUIRES_REVIEW"
    if re.search(r"\b(INVENTORY|ASSET REGISTER|PROPERTY REGISTER)\b", name) and re.search(
        r"\b(IN STOCK|IN REPAIR|UNIT COST|EXTENDED COST|SERIAL NBR|BATCH VALUATION)\b", row
    ):
        return "NON_PROCUREMENT_DOCUMENT_REQUIRES_REVIEW"
    return None


def procurement_price_sql(alias="",dialect="athena"):
    """Filter before pagination/join; old frozen copies can predate eligibility fixes."""
    import re
    if alias and not re.fullmatch(r"[a-z][a-z0-9_]*",alias):
        raise ValueError("Invalid internal SQL alias")
    if dialect not in ("athena","duckdb"):raise ValueError("Invalid SQL dialect")
    regex="regexp_matches" if dialect=="duckdb" else "regexp_like"
    p=alias+"."if alias else ""
    def c(name):return p+name
    filename="upper(coalesce("+c("attachment_s3_uri")+",''))"
    row="upper(coalesce("+c("attachment_source_row_text")+",''))"
    known="84075394258b3f88d553c39b5950b58e045113c016aaaf4f270b5005f268a967"
    property_pattern=r"GOVERNMENT[^A-Z0-9]*FURNISHED[^A-Z0-9]*PROPERTY|(^|[^A-Z0-9])GFP([^A-Z0-9]|$)"
    inventory_pattern=r"INVENTORY|ASSET[^A-Z0-9]*REGISTER|PROPERTY[^A-Z0-9]*REGISTER"
    stock_pattern=r"IN[^A-Z0-9]*STOCK|IN[^A-Z0-9]*REPAIR|UNIT[^A-Z0-9]*COST|EXTENDED[^A-Z0-9]*COST|SERIAL[^A-Z0-9]*NBR|BATCH[^A-Z0-9]*VALUATION"
    return " AND ".join([
        "coalesce("+c("include_in_mimir_range")+",false)",
        "coalesce("+c("promotion_eligible_without_review")+",false)",
        "coalesce("+c("quality_status")+",'') <> 'EXCLUDED_NON_PROCUREMENT_DOCUMENT'",
        "coalesce("+c("attachment_sha256")+",'') NOT IN ('','"+known+"')",
        "NOT "+regex+"("+filename+",'"+property_pattern+"')",
        "NOT ("+regex+"("+filename+",'"+inventory_pattern+"') AND "+regex+"("+row+",'"+stock_pattern+"'))",
        "try_cast("+c("unit_price_usd")+" AS double)>0",
        "try_cast("+c("quantity")+" AS double)>0",
    ])
