"""Optional native-slice market activity reader; never enabled by a request flag.

The trusted server proxy supplies a descriptor from a verified Market manifest.
Its enriched fact relation must preserve the canonical financial inventory and
use the SAME Main's party headers. It is not the legacy rolled-up detail table.
The publisher must pass the financial, prior-tag/subject/hold, DLA eligibility,
metadata and stock-link gates before emitting this descriptor.
"""
from dataclasses import dataclass
import json
import re

from market_scope_membership import Binding, NativeMarketSelection, FACT_COLUMNS, MEMBERSHIP_COLUMNS, _digest
from market_scope_facets import CANONICAL_COLUMNS
from market_scope_contract import validate_output_binding

DB = 'market_intel_platform_candidate'
VIEWS = {'contracts': 'PRIME_AWARD', 'contract-actions': 'PRIME_AWARD',
         'customers': 'PRIME_AWARD', 'contractors': 'PRIME_AWARD',
         'subcontracts': 'FIRST_TIER_REPORT', 'subcontractors': 'FIRST_TIER_REPORT',
         'parts': 'DLA_AWARD', 'award-nsns': 'PRIME_AWARD'}
METADATA = ('contract_id', 'parent_award_key', 'company', 'cage', 'uei', 'prime_company',
            'description', 'customer', 'nsn', 'report_year')
DETAIL_COLUMNS = frozenset((*FACT_COLUMNS, *METADATA, *CANONICAL_COLUMNS, 'modification_number', 'action_date'))
HASH = re.compile(r'[a-f0-9]{64}\Z')
AWARD = re.compile(r'[A-Z0-9_-]{1,128}\Z')


@dataclass(frozen=True)
class ScopeQuery:
    sql: str
    table_id: str
    selection_json: str
    metrics_sql: str
    totals_sql: str

    @property
    def selection(self):
        # Return a fresh object; caller mutation cannot change a minted cursor.
        return json.loads(self.selection_json)

    @property
    def context_sha256(self):
        return _digest([self.selection, self.sql])


def compile_scope_query(payload):
    # Snapshot nested descriptor and request containers before validation.
    p = json.loads(json.dumps(payload))
    d = p['scopeMembership']
    if not isinstance(d, dict) or d.get('schemaVersion') != 1:
        raise ValueError('Invalid native market membership descriptor')
    for field, pattern in [('mainRelease', r'mimir-main-platform-v7-[a-z0-9-]+'),
                           ('candidateId', r'cohesive-market-[a-f0-9]+')]:
        if (d.get(field) != p.get(field) or not isinstance(d.get(field), str)
                or not re.fullmatch(pattern, d[field])):
            raise ValueError('Membership descriptor belongs to another release')
    binding = Binding(**d['binding'])
    if d.get('bindingSha256') != binding.sha256 or not HASH.fullmatch(d.get('proofSha256', '')):
        raise ValueError('Missing immutable membership proof binding')
    suffixes = []
    for field, prefix in [('factTable', 'market_scope_financial_facts_'),
                          ('membershipTable', 'market_scope_memberships_'),
                          ('stockTable', 'market_native_stock_links_')]:
        match = re.fullmatch(prefix + r'([a-f0-9]{20})', d.get(field, ''))
        if not match:
            raise ValueError('Invalid native membership relation')
        suffixes.append(match[1])
    if len(set(suffixes)) != 1 or p.get('table') != 'market_professional_detail_' + suffixes[0]:
        raise ValueError('Market relations belong to different projections')
    validate_output_binding(d,p['table'])
    if (not isinstance(d.get('factColumns'), list) or not DETAIL_COLUMNS <= set(d['factColumns'])
            or not isinstance(d.get('membershipColumns'), list)
            or not MEMBERSHIP_COLUMNS <= set(d['membershipColumns'])):
        raise ValueError('Native metadata or membership schema is incomplete')
    source = VIEWS.get(p.get('view'))
    if not source or p.get('sources', [source]) != [source]:
        raise ValueError('This detail view requires its exact single source measure')
    mode = p.get('activityMode', 'activity')
    shared = p.get('includeShared', True)
    if mode not in ('primary', 'activity') or type(shared) is not bool:
        raise ValueError('Invalid market activity policy')
    if p['view'] != 'contract-actions' and p.get('award') is not None:
        raise ValueError('Award filter is supported only by native action detail')
    award = p.get('award') if p['view'] == 'contract-actions' else None
    if award is not None and (not isinstance(award, str) or not AWARD.fullmatch(award)):
        raise ValueError('Invalid native award selection')
    if p['view'] == 'contract-actions' and award is None:
        raise ValueError('Native action view requires an award')
    selector = NativeMarketSelection(binding, DB + '.' + d['factTable'], DB + '.' + d['membershipTable'],
                                    frozenset(d['factColumns']), frozenset(d['membershipColumns']))
    selected = selector.selected_sql(p['nodes'], p['years'], [source],
        platforms=p.get('platforms', []), activity=mode == 'activity', include_shared=shared)
    if award:
        selected = f"SELECT * FROM ({selected}) eligible WHERE source_record_id='{award}'"
    selection = {'mainRelease': d['mainRelease'], 'candidateId': d['candidateId'],
        'bindingSha256': binding.sha256, 'proofSha256': d['proofSha256'],
        'outputBindingSha256':d['outputBindingSha256'],'policySha256':d['policySha256'],
        'market': p['market'], 'view': p['view'], 'years': sorted(set(p['years'])),
        'nodes': sorted(set(p['nodes'])), 'platforms': sorted(set(p.get('platforms', []))),
        'sources': [source], 'activityMode': mode, 'includeShared': shared, 'award': award}
    # Group AFTER membership union. Never sum distinct amounts or join money to
    # multiple memberships/NSNs. Native actions retain the full source/year key.
    metadata = ','.join(f'arbitrary({field}) AS {field}' for field in METADATA)
    platforms = 'json_format(CAST(array_sort(array_distinct(flatten(array_agg(platform_ids)))) AS JSON)) AS platforms'
    if p['view'] == 'contract-actions':
        sql = f"""WITH selected AS ({selected}), actions AS (
          SELECT source_kind,source_record_id,fiscal_year,native_record_id,
            native_record_id||'/'||CAST(fiscal_year AS varchar) AS id,
            sum(net_usd) AS net,{metadata},
            arbitrary(modification_number) AS modification_number,
            arbitrary(CAST(action_date AS varchar)) AS action_date,{platforms}
          FROM selected GROUP BY source_kind,source_record_id,fiscal_year,native_record_id
        ), stock AS (
          SELECT source_kind,source_record_id,fiscal_year,native_record_id,
            json_format(CAST(array_sort(array_distinct(array_agg(nsn))) AS JSON)) AS stock_nsns
          FROM {DB}.{d['stockTable']}
          WHERE source_kind='{source}' AND source_record_id='{award}'
            AND fiscal_year IN ({','.join(map(str, selection['years']))})
          GROUP BY source_kind,source_record_id,fiscal_year,native_record_id
        ) SELECT actions.*,stock.stock_nsns FROM actions LEFT JOIN stock
          USING (source_kind,source_record_id,fiscal_year,native_record_id)"""
    else:
        sql = f"""WITH selected AS ({selected}), records AS (
          SELECT source_record_id AS id,sum(net_usd) AS net,{metadata},{platforms}
          FROM selected GROUP BY source_record_id
        )"""
        if p['view'] == 'customers':
            sql += " SELECT coalesce(nullif(trim(customer),''),'Awarding customer not reported') AS id,sum(net) AS net,count(*) AS records FROM records GROUP BY 1"
        elif p['view'] in ('contractors', 'subcontractors'):
            sql += """ SELECT coalesce(CASE WHEN regexp_like(cage,'^[A-Z0-9]{5}$') THEN 'CAGE:'||cage END,
              CASE WHEN regexp_like(uei,'^[A-Z0-9]{12}$') THEN 'UEI:'||uei END,'RECORD:'||id) AS id,
              max(company) AS name,max(cage) AS cage,max(uei) AS uei,sum(net) AS net,count(*) AS records
              FROM records GROUP BY 1"""
        else:
            sql += ' SELECT * FROM records'
            if p['view'] == 'award-nsns':
                # Keep the award's selected amount; NSN multiplicity never
                # multiplies money or filters out its other signed actions.
                sql += f""" WHERE id IN (SELECT f.source_record_id FROM selected f
                  WHERE regexp_like(replace(coalesce(f.nsn,''),'-',''),'^[0-9]{{13}}$')
                  OR regexp_like(coalesce(f.description,''),'(?i)\\bNSN\\b[\\s:)]*[0-9]{{4}}[- ]?[0-9]{{2}}[- ]?[0-9]{{3}}[- ]?[0-9]{{4}}(?![0-9])')
                  OR EXISTS (SELECT 1 FROM {DB}.{d['stockTable']} s
                    WHERE s.source_kind=f.source_kind AND s.source_record_id=f.source_record_id
                      AND s.fiscal_year=f.fiscal_year AND s.native_record_id=f.native_record_id
                      AND regexp_like(s.nsn,'^[0-9]{{13}}$')))"""
    # Outer ordering avoids confusing an input column with an aggregate alias.
    sql = 'SELECT * FROM (' + sql + ') market_rows ORDER BY abs(net) DESC,id ASC'
    measures = """count(*) AS financial_slices,
      count(DISTINCT source_record_id) AS awards,
      count(DISTINCT ROW(source_record_id,fiscal_year,native_record_id)) AS native_records,
      sum(net_usd) AS signed_usd,sum(abs(net_usd)) AS absolute_usd"""
    metrics = f'SELECT source_kind,fiscal_year,{measures} FROM ({selected}) selected GROUP BY source_kind,fiscal_year'
    # Headline award/native counts are calculated from the selected union;
    # they must never be obtained by adding annual or relationship-bucket counts.
    totals = f'SELECT source_kind,{measures} FROM ({selected}) selected GROUP BY source_kind'
    return ScopeQuery(sql, suffixes[0], json.dumps(selection, sort_keys=True, separators=(',', ':')), metrics, totals)
