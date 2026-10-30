"""Versioned procurement reasons; demand maturity is separate from company access.
Pure read model: does not create awards, notices, approvals or company workshares.
"""
import datetime, hashlib, html, json, re
from pathlib import Path
ROOT=Path(__file__).resolve().parent
RULES=json.loads((ROOT/'procurement_rules.json').read_text())
BY_ID={r['id']:r for r in RULES['rules']}
BY_LANE={lane:r for r in RULES['rules'] for lane in r['lanes']}
VERSION=RULES['version']
LOGIC_HASH=hashlib.sha256(Path(__file__).read_bytes()+(ROOT/'procurement_rules.json').read_bytes()).hexdigest()
PLANS={'explicit_work_package_plan','research_and_facilities_plan','all_service_procurement_plan'}
# Source passages for review, never standalone new opportunities.
SCREENS={
 'PR07':re.compile(r'\b(?:seeking|invites?|looking for|soliciting)\s+(?:qualified\s+|potential\s+|industry\s+|additional\s+)?(?:teaming partners|team members|partners|consortium members)\b|\b(?:teaming|consortium)\s+(?:opportunit(?:y|ies)|invitation|call for members)\b',re.I),
 'PR09':re.compile(r'\b(?:will|plans? to|planned|future efforts? (?:may|will)|proposed)\b[^.!?\n]{0,180}\b(?:upgrad\w*|refresh\w*|replac\w*|moderni[sz]\w*|retir\w*|obsolesc\w*|diminishing manufacturing sources)\b',re.I),
 'PR06':re.compile(r'\b(?:transition to (?:production|acquisition|procurement)|follow.on (?:production|procurement|acquisition)|production transition)\b',re.I),
 'PR14':re.compile(r'\b(?:production (?:ramp|rate increase)|increase[^.!?\n]{0,50}production|advance procurement|long.lead (?:procurement|purchas\w*)|capacity expansion)\b',re.I),
 'PR13':re.compile(r'\b(?:sources sought|seeking (?:new|additional|alternative) (?:manufacturing )?sources|new.source qualification (?:programme|program|opportunity))\b',re.I)}

def day(v):
 try:return datetime.date.fromisoformat(str(v)[:10])
 except (TypeError,ValueError):return None

def decode(v):
 if isinstance(v,dict):return v
 try:return json.loads(v or '{}')
 except (TypeError,ValueError):return {}

def text(v):return re.sub(r'\s+',' ',html.unescape(re.sub(r'<[^>]+>',' ',str(v or '')))).strip()

MECHANISMS={
 'PR16':re.compile(r'\b(?:SBIR|STTR|small business innovation research|small business technology transfer)\b',re.I),
 'PR17':re.compile(r'\b(?:broad agency announcement|commercial solutions opening|other transaction authority|consortium solicitation|prize challenge)\b',re.I),
 'PR18':re.compile(r'\b(?:on.ramp|off.ramp|task.order competition|IDIQ|multiple.award vehicle)\b',re.I),
 'PR19':re.compile(r'\b(?:foreign military sales|FMS case|foreign military financing)\b',re.I),
 'PR21':re.compile(r'\b(?:re.solicit(?:ation|ing)?|no acceptable (?:bids|offers)|no award was made|procurement failed)\b',re.I)}

def evaluate(o,as_of):
 today=day(as_of)
 if not today:raise ValueError('An explicit valid as-of date is required')
 lane=str(o.get('lane') or '');r=BY_LANE.get(lane)
 eid=o.get('source_evidence_id');source=bool(eid)
 raw=text(o.get('scope_text'));start=day(o.get('window_start'));end=day(o.get('window_end'))
 state=' '.join(str(o.get(k) or '') for k in ('reconciled_state','procurement_state')).lower()
 terminal=any(x in state for x in ('cancelled','awarded_or_ordered','source_confirmed_award'))
 elapsed=bool(end and end<today)
 assessment=o.get('source_assessment') or {}
 hints=[]
 for key,pattern in SCREENS.items():
  m=pattern.search(raw)
  if not m:continue
  # Negation and a comparison with an old programme are not invitations.
  before=raw[max(0,m.start()-35):m.start()]
  if re.search(r'\b(?:not|no|without)\s+(?:a\s+|currently\s+)?$',before,re.I):continue
  if key=='PR13' and lane!='published_procurement':continue
  hints.append(dict(rule_id=key,label=BY_ID[key]['label'],status='source_passage_for_review',quote=raw[max(0,m.start()-65):min(len(raw),m.end()+160)],evidence_id=eid))
 invitation=bool(assessment.get('qualification_invitation'))
 # Do not convert non-notice historic text into a new-source opening.
 if not assessment and lane=='published_procurement':invitation=any(h['rule_id']=='PR13' for h in hints) or str(o.get('notice_type') or '').strip().lower()=='sources sought'
 posted=day(o.get('notice_posted_date'))
 fresh=bool(posted and 0<=(today-posted).days<=90)
 invitation_current=invitation and (bool(end and end>=today) or (end is None and fresh))
 # Buyers often call prospective direct contractors "industry partners".
 # That phrase alone is not evidence of a consortium or a teaming agreement.
 teaming=any(h['rule_id']=='PR07' and re.search(r'\b(?:teaming|consortium|team members)\b',h['quote'],re.I) for h in hints)
 teaming_current=teaming and lane=='published_procurement' and bool(end and end>=today)
 inputs=decode(o.get('value_inputs_json'))
 template=lane=='new_prime_historical_supplier_flow' and bool(inputs.get('new_prime_evidence') and inputs.get('prior_awards',0)>0 and inputs.get('source_reports',0)>0 and inputs.get('support_digest') and day(inputs.get('first_award_date')) and day(inputs['first_award_date'])<=today)
 evidence_ids=list(dict.fromkeys(x for x in [eid,inputs.get('new_prime_evidence')]+[x.get('report_evidence') for x in inputs.get('preview',[])]+[x.get('parent_evidence') for x in inputs.get('preview',[])] if x))
 unknown=[]
 if not source:unknown.append('Native demand evidence is missing')
 if not o.get('customer_name'):unknown.append('Responsible customer is not established')
 if not start and not end:unknown.append('Engagement/demand timing is not established')
 if lane in PLANS:unknown.extend(['Plan-to-solicitation/award outcome remains to be resolved','External delivery and company entry route require confirmation'])
 if 'supplier' in str(lane):unknown.extend(['Package may already be placed','Technical access and the buying site require confirmation'])
 if template:unknown.append('Prior outsourcing supports potential pull, not a forecast supplier RFQ')
 if lane=='source_item_forecasts':unknown.append('Inventory, incoming supply and conversion into procurement remain checks')
 if lane=='scoped_recurrence':unknown.append('Observed cadence has not been validated as an award forecast')
 if invitation_current and end is None:unknown.append('Recent agency source-interest notice; response deadline is not supplied. Confirm the office still accepts capability information.')
 stage='evidence_gap';route='unestablished';primary=r['id'] if r else None
 if terminal or elapsed:stage='resolved_or_elapsed'
 elif not source or not o.get('customer_name'):pass
 elif invitation_current:stage='qualification_opening';route='explicit_source_invitation';primary='PR13'
 elif teaming_current:stage='teaming_opening';route='explicit_teaming_invitation';primary='PR07'
 elif lane.startswith('published') and end:stage='published_requirement';route='published_response_route'
 elif lane=='source_item_forecasts':stage='source_forecast';route='item_access_check'
 elif lane=='explicit_work_package_plan':stage='emerging_demand';route='planned_package_access_check'
 elif lane in ('research_and_facilities_plan','all_service_procurement_plan'):stage='planning_context';route='external_task_not_established'
 elif template:stage='emerging_demand';route='prime_supply_chain_validation'
 elif lane=='funded_prime_supplier_packages':stage='historical_watch';route='no_new_sourcing_event'
 elif lane=='scoped_recurrence':stage='repeat_demand_inference';route='future_route_unconfirmed'
 elif lane=='new_prime_historical_supplier_flow':unknown.append('New-parent/prior-outsourcing lineage is incomplete')
 elif lane.startswith('published'):unknown.append('Current response window is not verified')
 if invitation and not invitation_current:unknown.append('Qualification invitation timing is not verified as current')
 if hints and stage not in ('teaming_opening','qualification_opening'):unknown.append('Additional source passages are review tags, not validated buying signals')
 rule=BY_ID.get(primary,{})
 basis={'source_name':o.get('source_name'),'evidence_ids':evidence_ids,'source_url':o.get('source_url'),'source_locator':o.get('source_locator'),'source_record_hash':o.get('record_hash')}
 demand_value='programme_funding_context' if lane in ('research_and_facilities_plan','all_service_procurement_plan') else 'historical_comparator' if lane in ('scoped_recurrence','new_prime_historical_supplier_flow','funded_prime_supplier_packages') else 'source_quantity_price_scenario' if lane in ('published_item_procurement','source_item_forecasts') else 'source_planned_package' if lane=='explicit_work_package_plan' else 'notice_or_exact_item_price_evidence'
 out=dict(version=VERSION,logic_sha256=LOGIC_HASH,as_of=str(today),rule_id=primary,reason_title=rule.get('label','Unclassified evidence'),stage=stage,participation_route=route,requires_access_confirmation=True,source_basis=basis,source_passages=hints,unresolved=unknown,negative_evidence=[dict(reason='Source records cancellation/award or retained demand window has elapsed',evidence_id=eid)] if terminal or elapsed else [],value_interpretation=demand_value,company_addressable_value=None,next_action=rule.get('next_action','Review missing evidence before recommending this work.'),source_period=dict(start=str(start) if start else None,end=str(end) if end else None,basis=o.get('date_basis')),solicitation_date_estimate=None,same_scope_resolution_required=lane in PLANS or 'supplier' in str(lane),template_lineage_present=template)
 mechanisms=[]
 if o.get('source_name')=='sbir_topics':mechanisms.append(dict(rule_id='PR16',label=BY_ID['PR16']['label'],evidence_id=eid,source_wording='Structured SBIR/STTR topic source'))
 for code,pattern in MECHANISMS.items():
  match=pattern.search(text(o.get('title'))+' '+raw)
  if match:mechanisms.append(dict(rule_id=code,label=BY_ID[code]['label'],evidence_id=eid,source_wording=match.group(0)))
 out['procurement_mechanisms']=mechanisms
 if lane.startswith('published') and mechanisms and stage not in ('resolved_or_elapsed','evidence_gap'):
  code=mechanisms[0]['rule_id']
  out['rule_id']=code;out['reason_title']=BY_ID[code]['label'];out['next_action']=BY_ID[code]['next_action']
  if code=='PR16':out['unresolved'].append('SBIR/STTR applicant, phase, ownership/size and research-data rights conditions require notice-specific verification; past topic activity is not eligibility')
  if code=='PR18':out['unresolved'].append('Direct task-order access depends on vehicle holder/on-ramp or permitted teaming status')
  if code=='PR19':out['unresolved'].append('FMS approval or programme ceiling does not establish a signed/funded supplier package')
 out['response_window_verified']=bool(end and end>=today and not terminal)
 if invitation_current and end is None:out['next_action']='Contact the office in the agency notice to confirm whether capability information is still being accepted, then establish the qualification scope, technical-data access, cost and timetable.'
 out['decision_hash']=hashlib.sha256(json.dumps(out,sort_keys=True,default=str).encode()).hexdigest()
 return out

def attach(data,as_of):
 for o in data:o['demand_model']=evaluate(o,as_of)
 return data

def catalogue():return dict(RULES,logic_sha256=LOGIC_HASH)
