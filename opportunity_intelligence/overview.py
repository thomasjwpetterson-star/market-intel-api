"""Company-wide counts and conservative totals; never add historical benchmarks."""
import json,re
from decimal import Decimal,InvalidOperation

POLICY='priced-demand-and-materiality-v1'
PRICED_KINDS=('item_reference_estimate','historical_unit_price_estimate')

def summary(counts,priced,release):
 result={kind:dict(count=int(counts.get(kind+'_count') or 0),larger_count=int(counts.get(kind+'_larger_count') or 0),priced_source_count=0,priced_requirement_count=0,priced_demand_total=None,value_components=[]) for kind in ('work','items')}
 groups={}
 for row in priced:
  if row.get('commercial_value_kind') not in PRICED_KINDS:continue
  try:value=Decimal(str(row.get('commercial_value')))
  except (InvalidOperation,TypeError,ValueError):continue
  if not value.is_finite() or value<=0:continue
  category='items' if str(row.get('item_demand')).lower()=='true' else 'work'
  def decode(v):
   try:return json.loads(v or '{}')
   except (ValueError,TypeError):return {}
  notice=decode(row.get('notice_item_pricing_json'));inputs=decode(row.get('value_inputs_json'))
  niin=str(row.get('item_reference_niin') or (notice[0].get('nsn') if isinstance(notice,list) and len(notice)==1 else '') or (inputs.get('niin') or inputs.get('nsn') if isinstance(inputs,dict) else '') or '')
  niin=re.sub(r'\D','',niin)[-9:]
  buyer=re.sub('[^A-Z0-9]','',str(row.get('customer_name') or '').upper())
  # Overlapping notice families are already collapsed by the shared feed query.
  # Repeated same-buyer/item requirements contribute only the largest scenario.
  # Unresolved buyer identities stay separate rather than grouping unknowns.
  key=(category,buyer,niin) if len(niin)==9 and buyer else (category,row['episode_id'])
  result[category]['priced_source_count']+=1
  current=groups.get(key)
  member={k:row.get(k) for k in ('episode_id','record_hash','title','customer_name','source_url','source_evidence_id','commercial_value','commercial_value_kind','notice_item_pricing_json','value_inputs_json','item_reference_niin')}
  if current is None:groups[key]=dict(value=value,row=member,members=[member],niin=niin)
  else:
   current['members'].append(member)
   if value>current['value']:current.update(value=value,row=member)
 for key,group in groups.items():
  result[key[0]]['value_components'].append(dict(**group['row'],included_value=float(group['value'].quantize(Decimal('.01'))),niin=group['niin'] or None,related_requirements=group['members'],aggregation='Largest priced scenario for this buyer/item; related notices are not added' if len(group['members'])>1 else 'One priced requirement'))
 for category in result.values():
  components=category['value_components'];components.sort(key=lambda x:(-x['included_value'],x['episode_id']))
  category['priced_requirement_count']=len(components)
  if components:category['priced_demand_total']=float(sum(Decimal(str(x['included_value'])) for x in components))
 scale=float(counts.get('observed_prime_net_obligations') or 0)
 return dict(release=release,policy=POLICY,categories=result,materiality_floor=1000000 if scale>=100000000 else 0,materiality_basis='Default $1m focus for sites with at least $100m lifetime observed prime obligations; editable, not annual revenue or a capability limit.',value_explanation='Priced demand uses stated quantities and exact-item reference or historical unit prices. Repeated requirements for the same buyer and item contribute their largest scenario once. Historical contract comparisons, programme totals and overlapping forecasts are excluded. Government reference dates may be unknown; prices are not current quotes. Unpriced opportunities are not assumed to be worth zero. These are demand estimates, not expected company revenue.')
