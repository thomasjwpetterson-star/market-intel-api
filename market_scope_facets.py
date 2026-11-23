"""Read-only normalization of the existing canonical decisions for carry checks.

This does not classify records. Values come from the accepted native ledger and
its existing subject/hold registries. Subject and hold adapters must resolve the
full allocation extent upstream; a native-only or parent-only wildcard is not
accepted here. New engine associations use the same normalized inventory.
"""
from market_scope_membership import KEY, _table

SCALAR_FACETS = ('market_node','platform_bucket','platform_applicability',
                 'product_technology','technology_subject','work_type','work_role','application_domain')
CANONICAL_COLUMNS = frozenset((*SCALAR_FACETS,'platform_ids'))


def canonical_facets_sql(facts, subjects, holds, memberships, *, fact_columns, edge_columns):
    for table in (facts, subjects, holds, memberships): _table(table)
    if not frozenset((*KEY,*CANONICAL_COLUMNS)) <= frozenset(fact_columns):
        raise ValueError('Canonical mapping columns must be retained in full')
    if not frozenset((*KEY,'facet_id','relationship')) <= frozenset(edge_columns):
        raise ValueError('Subject and hold edges require exact resolved allocation extents')
    keys=','.join(KEY)
    parts=[]
    for field in SCALAR_FACETS:
        condition=f"{field} IS NOT NULL AND trim({field})<>''"
        if field=='market_node': condition+=" AND market_node<>'US'"
        if field=='platform_bucket': condition+=" AND platform_bucket<>'UNRESOLVED'"
        parts.append(f"SELECT {keys},'mapping' AS kind,'{field}:'||{field} AS facet_id,'canonical' AS relationship FROM {facts} WHERE {condition}")
    parts.append(f"SELECT {','.join('f.'+k for k in KEY)},'mapping' AS kind,'platform:'||p.platform_id AS facet_id,'canonical' AS relationship "
                 f"FROM {facts} f CROSS JOIN UNNEST(f.platform_ids) AS p(platform_id) WHERE p.platform_id IS NOT NULL AND trim(p.platform_id)<>''")
    for table,kind in [(subjects,'subject'),(holds,'hold')]:
        parts.append(f"SELECT {keys},'{kind}' AS kind,facet_id,relationship FROM {table}")
    parts.append(f"SELECT {keys},'association' AS kind,target_dimension||':'||target_id AS facet_id,relationship FROM {memberships}")
    return 'SELECT DISTINCT * FROM ('+' UNION ALL '.join(parts)+') canonical_facets'
