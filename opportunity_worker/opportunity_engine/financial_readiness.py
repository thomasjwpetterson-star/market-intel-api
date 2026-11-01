"""Read-side quarantine until subcontract opportunity amounts are reconciled.

The main platform's promoted company-network finance is a separate authority.
This does not cap, rewrite or repair source reports, nor change prime/item data.
"""
import hashlib
import json
import re
from pathlib import Path

LANES = ('recurring_subcontract_work','new_prime_historical_supplier_flow')
VALUE_BASIS = 'production_description_assisted_adjusted_reported_subcontract_value'
SOURCE_VIEW = 'market_intel_gold.ref_company_network'
POLICY = 'subcontract-opportunity-finance-quarantine-v1'
POLICY_HASH = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
REASON = 'Historical subcontract amounts have not yet been reconciled to the adjusted source records. They are withheld from opportunity values and value-based ranking.'
DISPLAY_FIELDS = ('value_low','value_median','value_high','commercial_value','company_addressable_value',
                  'historical_package_low','historical_package_median','historical_package_high',
                  'next_12_month_low','next_12_month_median','next_12_month_high',
                  'next_36_month_low','next_36_month_median','next_36_month_high',
                  'previous_value_median','current_value_median')


def validated(manifest):
    policy=manifest.get('subcontract_opportunity_finance') or {}
    columns={x['Name'] for x in manifest.get('tables',{}).get('company_subcontract_facts',{}).get('columns',[])}
    return bool(policy.get('state')=='validated' and policy.get('value_basis')==VALUE_BASIS
                and policy.get('source_view')==SOURCE_VIEW and policy.get('validation_query_id')
                and {'value_basis','included_in_adjusted_total','source_report_identity'}<=columns)


def risk_sql(manifest, alias='o'):
    if not re.fullmatch('[a-z_]+',alias):raise ValueError('Invalid financial SQL alias')
    return 'false' if validated(manifest) else alias+".lane IN ('"+"','".join(LANES)+"')"


def status(manifest):
    return dict(policy=POLICY,policy_hash=POLICY_HASH,subcontract_opportunity_values='validated' if validated(manifest) else 'not_validated',
                affected_lanes=list(LANES),interpretation='This status concerns opportunity-engine subcontract benchmarks. Independently reconciled main-platform finance, prime obligations, item pricing and agency plans remain separate.')


def guard(row, manifest, *, source_report=False):
    result=dict(row)
    affected = result.get('lane') in LANES or source_report or result.get('source_kind') in ('subaward_report','subcontract_report') or result.get('amount_basis')=='reported_subcontract_package_amount'
    if not affected:return result
    result.update(financial_validation='validated' if validated(manifest) else 'not_validated',financial_validation_policy=POLICY,financial_validation_policy_hash=POLICY_HASH)
    if validated(manifest):return result
    fields=DISPLAY_FIELDS+(('reported_amount','amount','reported_subcontract_amount') if source_report or result.get('source_kind') in ('subaward_report','subcontract_report') or result.get('amount_basis')=='reported_subcontract_package_amount' else ())
    raw={key:result[key] for key in fields if key in result and result[key] is not None}
    if raw:result['unvalidated_financial_evidence_json']=json.dumps(dict(amounts=raw,source_value_basis=result.get('value_basis') or result.get('amount_basis'),interpretation='Unreconciled source figures retained for audit only; not a validated contract size, opportunity value or supplier revenue'),default=str)
    for key in fields:
        if key in result:result[key]=None
    result['financial_validation_reason']=REASON
    if result.get('lane') in LANES:
        result['value_basis']='withheld_pending_subcontract_source_reconciliation'
        result['commercial_value_kind']='unvalidated_subcontract_benchmark'
    return result
