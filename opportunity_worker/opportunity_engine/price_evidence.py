"""Reject non-procurement valuations in frozen and future price evidence."""
def non_procurement_document_class(attachment_sha256, attachment_name, source_row_text=""):
    """Document meaning takes precedence over valid numeric row arithmetic.

    The exact reviewed hash remains excluded if renamed. Filename matches flag
    similar property documents for review, without claiming a verified value
    basis for a different source. A property clause in an executed line alone
    is not enough to reject a genuine purchase.
    """
    import re

    reviewed = {
        "84075394258b3f88d553c39b5950b58e045113c016aaaf4f270b5005f268a967":
            "GOVERNMENT_FURNISHED_PROPERTY_INVENTORY",
        "aa8abebbd6e303032eb32a1a63568275147577139783e2b2558cb212c7454169":
            "PREAWARD_PLANNING_ESTIMATE_REFERENCE",
        "6f2e0fc4bb73993ba7c0f0cd226a38bd85bc9dca33a67c55efe27de1999b14bf":
            "VALUE_BASIS_UNESTABLISHED_REFERENCE",
    }
    if attachment_sha256 in reviewed:
        return reviewed[attachment_sha256]
    name = re.sub(r"[^A-Z0-9]+", " ", str(attachment_name or "").upper()).strip()
    row = re.sub(r"[^A-Z0-9]+", " ", str(source_row_text or "").upper()).strip()
    if re.search(r"\bGOVERNMENT FURNISHED PROPERTY\b|\bGFP\b", name):
        return "NON_PROCUREMENT_DOCUMENT_REQUIRES_REVIEW"
    if re.search(r"\b(INVENTORY|ASSET REGISTER|PROPERTY REGISTER)\b", name) and re.search(
        r"\b(IN STOCK|IN REPAIR|UNIT COST|EXTENDED COST|SERIAL NBR|BATCH VALUATION)\b", row
    ):
        return "NON_PROCUREMENT_DOCUMENT_REQUIRES_REVIEW"
    if re.search(r"\b(PRE ?SOLICITATION|PLANNING ESTIMATE|IGCE)\b", name):
        return "PREAWARD_DOCUMENT_VALUE_REQUIRES_REVIEW"
    return None


def document_value_basis(document_class):
    """Economic meaning is separate from the unchanged extraction price_basis."""
    if document_class == "GOVERNMENT_FURNISHED_PROPERTY_INVENTORY":
        return "GOVERNMENT_PROPERTY_ACCOUNTING_UNIT_COST"
    if document_class == "PREAWARD_PLANNING_ESTIMATE_REFERENCE":
        return "SOURCE_PLANNING_ESTIMATE_UNIT_PRICE"
    if document_class:
        return "SOURCE_VALUE_BASIS_UNESTABLISHED"
    return "OBSERVED_SOURCE_LINE_PRICE_UNREVIEWED"


def retain_document_clin(document_class):
    """An estimate can have real schedule CLINs; inventory item codes cannot."""
    return document_class in (None, "PREAWARD_PLANNING_ESTIMATE_REFERENCE", "PREAWARD_DOCUMENT_VALUE_REQUIRES_REVIEW")


def document_quality_status(document_class):
    if document_class in ("PREAWARD_PLANNING_ESTIMATE_REFERENCE", "VALUE_BASIS_UNESTABLISHED_REFERENCE", "PREAWARD_DOCUMENT_VALUE_REQUIRES_REVIEW"):
        return "REFERENCE_ONLY_SOURCE_VALUE"
    if document_class:
        return "EXCLUDED_NON_PROCUREMENT_DOCUMENT"
    return "AUTO_ADMITTED_BY_COHORT_RULE"


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
    known=("84075394258b3f88d553c39b5950b58e045113c016aaaf4f270b5005f268a967",
           "aa8abebbd6e303032eb32a1a63568275147577139783e2b2558cb212c7454169",
           "6f2e0fc4bb73993ba7c0f0cd226a38bd85bc9dca33a67c55efe27de1999b14bf")
    property_pattern=r"GOVERNMENT[^A-Z0-9]*FURNISHED[^A-Z0-9]*PROPERTY|(^|[^A-Z0-9])GFP([^A-Z0-9]|$)"
    inventory_pattern=r"INVENTORY|ASSET[^A-Z0-9]*REGISTER|PROPERTY[^A-Z0-9]*REGISTER"
    stock_pattern=r"IN[^A-Z0-9]*STOCK|IN[^A-Z0-9]*REPAIR|UNIT[^A-Z0-9]*COST|EXTENDED[^A-Z0-9]*COST|SERIAL[^A-Z0-9]*NBR|BATCH[^A-Z0-9]*VALUATION"
    return " AND ".join([
        "coalesce("+c("include_in_mimir_range")+",false)",
        "coalesce("+c("promotion_eligible_without_review")+",false)",
        "coalesce("+c("quality_status")+",'') <> 'EXCLUDED_NON_PROCUREMENT_DOCUMENT'",
        "coalesce("+c("attachment_sha256")+",'') NOT IN ('','"+"','".join(known)+"')",
        "NOT "+regex+"("+filename+",'"+property_pattern+"')",
        "NOT ("+regex+"("+filename+",'"+inventory_pattern+"') AND "+regex+"("+row+",'"+stock_pattern+"'))",
        "NOT "+regex+"("+filename+",'PRE[^A-Z0-9]*SOLICITATION|PLANNING[^A-Z0-9]*ESTIMATE|(^|[^A-Z0-9])IGCE([^A-Z0-9]|$)')",
        "try_cast("+c("unit_price_usd")+" AS double)>0",
        "try_cast("+c("quantity")+" AS double)>0",
    ])
