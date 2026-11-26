"""Programme dashboard queries share the reviewed action scope and native subawards."""
from programme_parts import buyer_name


def action_scope(name, years=None, agency=None, domain=None, psc=None, reference=None):
    programme = str(name or '').strip()
    if not programme:
        raise ValueError('Choose a programme.')
    years = sorted(set(years or []))
    if any(type(year) is not int or not 1900 <= year <= 2200 for year in years):
        raise ValueError('Use valid fiscal years.')
    payload = dict(program=programme, years=years, agency=agency, domain=domain)
    entity = reference.resolve(programme) if reference else None
    if entity:
        _, where, params = reference.predicate(payload)
        label = entity['canonical_label']
    else:
        # Unregistered native labels remain searchable. Reviewed clears/holds
        # cannot reappear through this fallback merely under an old spelling.
        where, params = 'UPPER(TRIM(platform_family)) = ?', [programme.upper()]
        if reference:
            import json
            where += " AND COALESCE(award_key,'') NOT IN (SELECT json_extract_string(value,'$') FROM json_each(?))"
            params.append(json.dumps(sorted(row['award_key'] for row in reference.rows)))
        if years:
            where += ' AND year IN (' + ','.join('?' for _ in years) + ')'
            params.extend(years)
        if agency:
            where += ' AND (UPPER(TRIM(sub_agency))=? OR UPPER(TRIM(parent_agency))=?)'
            params.extend([buyer_name(agency)] * 2)
        if domain:
            where += ' AND UPPER(TRIM(market_segment))=?'
            params.append(domain.strip().upper())
        label = programme
    if psc:
        where += " AND REGEXP_REPLACE(UPPER(TRIM(CAST(psc AS VARCHAR))),'[.]0+$','')=?"
        params.append(str(psc).split(' - ')[0].strip().upper())
    return label, where, params


AWARD_KEY = "COALESCE(NULLIF(award_key,''),CONCAT(COALESCE(source_system,'UNKNOWN'),'|',contract_id,'|',COALESCE(vendor_cage,'')))"
SITE_KEY = "COALESCE(NULLIF(UPPER(TRIM(vendor_cage)),''),'NAME:'||UPPER(TRIM(vendor_name)))"


def subcontracts_scope(name, years=None, agency=None, domain=None, psc=None,
                      prime_vendor=None, sub_vendor=None):
    clauses = ['UPPER(TRIM(n.platform_family))=?']
    params = [name.strip().upper()]
    if years:
        ys = sorted(set(years))
        if any(type(y) is not int or not 1900 <= y <= 2200 for y in ys):
            raise ValueError('Use valid fiscal years.')
        clauses.append('n.year IN (' + ','.join('?' for _ in ys) + ')')
        params.extend(ys)
    for column, value in [('market_segment', domain), ('psc', str(psc).split(' - ')[0] if psc else None),
                          ('prime_name', prime_vendor), ('sub_name', sub_vendor)]:
        if value:
            expression = 'UPPER(TRIM(CAST(n.' + column + ' AS VARCHAR)))'
            if column == 'psc':
                expression = "REGEXP_REPLACE(" + expression + ",'[.]0+$','')"
            clauses.append(expression + '=?')
            params.append(value.strip().upper())
    if agency:
        # Scope to the recorded buyer on the agency-qualified parent award,
        # without multiplying reports or inventing inherited v7 membership.
        clauses.append('''EXISTS (SELECT 1 FROM v_transactions t
            WHERE t.award_key=n.award_key
              AND (UPPER(TRIM(t.sub_agency))=? OR UPPER(TRIM(t.parent_agency))=?))''')
        params.extend([buyer_name(agency)] * 2)
    return ' AND '.join(clauses), params
