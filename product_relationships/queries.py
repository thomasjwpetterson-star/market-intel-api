"""Read only. Fixed source tables and strictly validated identifiers."""
import re

REFERENCES = 'market_intel_gold.ref_flis_source_relationships'
PARTS = 'market_intel_silver.view_dla_contract_history_part_references'

def cage(value):
    if not isinstance(value, str) or not re.fullmatch('[A-Z0-9]{5}', value):
        raise ValueError('Invalid CAGE')
    return value

def niin(value):
    if not isinstance(value, str) or not re.fullmatch(r'(?:\d{9}|\d{13})', value):
        raise ValueError('Use a nine-digit NIIN or thirteen-digit NSN')
    return value[-9:]

def pagination(limit, offset):
    if type(limit) is not int or type(offset) is not int or not 1 <= limit <= 100 or not 0 <= offset <= 100000:
        raise ValueError('Invalid pagination')
    return f' OFFSET {offset} LIMIT {limit}'

def scope_query(selected):
    selected = cage(selected)
    return f"SELECT DISTINCT niin FROM {REFERENCES} WHERE cage_code='{selected}'"

def context_ctes(selected, seller=None):
    selected = cage(selected)
    seller = cage(seller) if seller else None
    item_filter = '' if seller else f"AND p.niin IN ({scope_query(selected)})"
    part_filter = f"AND p.cage='{seller}'" if seller else f"AND p.cage<>'{selected}'"
    # Preserve punctuation in part numbers. No fuzzy joins or company-name grouping.
    active = "c.cage_status='A' AND c.rnvc NOT IN ('8','9')"
    design = f"{active} AND c.rncc='3' AND c.rnvc='2'"
    reference_row = "CAST(ROW(c.cage_code,c.part_number,c.rncc,c.rnvc,c.rnsc,c.cage_status,cast(c.ingestion_date AS varchar),c.source_file) AS ROW(cage varchar,part_number varchar,rncc varchar,rnvc varchar,rnsc varchar,cage_status varchar,snapshot_date varchar,source_file varchar))"
    return f"""WITH seller_parts AS (
 SELECT p.niin,p.cage seller_cage,upper(trim(p.part_number)) part_number,
 max(p.nsn) nsn,max(p.item_name) item_name,min(p.first_observed_date) first_observed,max(p.last_observed_date) last_observed
 FROM {PARTS} p
 WHERE nullif(trim(p.part_number),'') IS NOT NULL AND regexp_like(p.cage,'^[A-Z0-9]{{5}}$')
 AND p.cage NOT IN ('19207','81349','80205','96906','57685','81348','88044') {part_filter} {item_filter}
 GROUP BY p.niin,p.cage,upper(trim(p.part_number))
), catalog AS (
 SELECT c.niin,c.cage_code,c.part_number,c.normalized_part_number,c.rncc,c.rnvc,c.rnsc,c.cage_status,c.ingestion_date,c.source_file
 FROM {REFERENCES} c
), matches AS (
 SELECT p.niin,p.seller_cage,max(p.nsn) nsn,max(p.item_name) item_name,min(p.first_observed) first_observed,max(p.last_observed) last_observed,count(DISTINCT p.part_number) seller_part_references,
 coalesce(bool_or(c.cage_code='{selected}' AND {active}),false) selected_catalog_match,
 coalesce(bool_or(c.cage_code=p.seller_cage AND {design}),false) seller_design_reference,
 count(DISTINCT CASE WHEN c.cage_code NOT IN ('{selected}',p.seller_cage) AND {design} THEN c.cage_code END) other_design_sources,
 json_format(cast(array_distinct(max_by(p.part_number,p.part_number,8)) AS JSON)) seller_parts_json,
 json_format(cast(array_distinct(max_by({reference_row},concat(CASE WHEN c.cage_code='{selected}' THEN '2' ELSE '1' END,coalesce(cast(c.ingestion_date AS varchar),''),coalesce(c.cage_code,'')),12)) AS JSON)) reference_evidence_json
 FROM seller_parts p LEFT JOIN catalog c ON c.niin=p.niin AND c.normalized_part_number=p.part_number
 GROUP BY p.niin,p.seller_cage
), products AS (
 SELECT m.*,
 CASE WHEN m.selected_catalog_match AND (m.other_design_sources>0 OR m.seller_design_reference) THEN 'overlapping_catalog_sources'
 WHEN m.seller_design_reference THEN 'seller_design_reference'
 WHEN m.selected_catalog_match THEN 'selected_catalog_match'
 WHEN m.other_design_sources=1 THEN 'other_source_reference'
 WHEN m.other_design_sources>1 THEN 'multiple_other_sources'
 ELSE 'unattributed' END attribution
 FROM matches m
)
"""

def market_query(selected, registry_table, limit=25, offset=0):
    page = pagination(limit, offset)
    if not re.fullmatch(r'market_intel_platform_candidate\.oi_[a-z0-9_]+', registry_table):
        raise ValueError('Invalid registry binding')
    example = "CAST(ROW(p.niin,p.nsn,p.item_name,p.attribution,p.seller_parts_json,p.reference_evidence_json,cast(p.last_observed AS varchar)) AS ROW(niin varchar,nsn varchar,item_name varchar,attribution varchar,seller_parts_json varchar,reference_evidence_json varchar,last_observed varchar))"
    return context_ctes(selected) + f""", peers AS (
 SELECT p.seller_cage,max(r.company_name) seller_name,count(*) observed_items,
 count_if(p.attribution='selected_catalog_match') selected_catalog_items,
 count_if(p.attribution='other_source_reference') other_source_items,
 count_if(p.attribution IN ('overlapping_catalog_sources','multiple_other_sources')) ambiguous_items,
 count_if(p.attribution='seller_design_reference') seller_design_items,
 count_if(p.attribution='unattributed') unattributed_items,
 max(p.last_observed) last_observed,
 json_format(cast(max_by({example},concat(CASE WHEN p.attribution='unattributed' THEN '0' ELSE '1' END,cast(p.last_observed AS varchar),p.niin),5) AS JSON)) examples_json
 FROM products p LEFT JOIN {registry_table} r ON r.cage_code=p.seller_cage GROUP BY p.seller_cage
), counted AS (SELECT peers.*,count(*) OVER () sellers,sum(observed_items) OVER () seller_item_relationships,
 sum(selected_catalog_items) OVER () total_selected_catalog_items,sum(other_source_items) OVER () total_other_source_items,
 sum(ambiguous_items) OVER () total_ambiguous_items,sum(seller_design_items) OVER () total_seller_design_items,
 sum(unattributed_items) OVER () total_unattributed_items FROM peers)
SELECT counted.*,(SELECT count(*) FROM ({scope_query(selected)}) scope) scope_items FROM counted ORDER BY observed_items DESC,seller_cage{page}
"""

def portfolio_query(selected, seller, limit=25, offset=0):
    page = pagination(limit, offset)
    return context_ctes(selected, seller) + f""", counted AS (
 SELECT products.*,count(*) OVER () total_observed_items,
 count_if(attribution='selected_catalog_match') OVER () total_selected_catalog_items,
 count_if(attribution='other_source_reference') OVER () total_other_source_items,
 count_if(attribution IN ('overlapping_catalog_sources','multiple_other_sources')) OVER () total_ambiguous_items,
 count_if(attribution='seller_design_reference') OVER () total_seller_design_items,
 count_if(attribution='unattributed') OVER () total_unattributed_items FROM products)
SELECT * FROM counted ORDER BY last_observed DESC,niin{page}
"""

def references_query(item, limit=50, offset=0):
    item = niin(item)
    page = pagination(limit, offset)
    return f"""SELECT DISTINCT niin,cage_code,part_number,normalized_part_number,rncc,rnvc,rnsc,cage_status,rnaac,sadc,
 source_relationship,is_procurement_authorized,is_active_authorized_source,cast(ingestion_date AS varchar) snapshot_date,source_file
 FROM {REFERENCES} WHERE niin='{item}' ORDER BY cage_code,part_number,rncc,rnvc,rnsc,source_file{page}"""
