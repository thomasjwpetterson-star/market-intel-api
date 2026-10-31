"""Company-wide counts and conservative totals; never add historical benchmarks."""
import json,re
from decimal import Decimal,InvalidOperation

POLICY='priced-demand-business-coverage-v4'
PRICED_KINDS=('item_reference_estimate','historical_unit_price_estimate')

def summary(counts,priced,release,business=None):
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
  request=inputs.get('purchase_request_number') if isinstance(inputs,dict) else None
  key=(category,'purchase_request',buyer,str(request),niin) if request and len(niin)==9 and buyer else (category,buyer,niin) if len(niin)==9 and buyer else (category,row['episode_id'])
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
 combined={}
 for category in result.values():
  for component in category['value_components']:
   buyer=re.sub('[^A-Z0-9]','',str(component.get('customer_name') or '').upper());niin=component.get('niin')
   raw=decode(component.get('value_inputs_json'));request=raw.get('purchase_request_number') if isinstance(raw,dict) else None
   key=('purchase_request',buyer,str(request),niin) if request and buyer and niin else ('item',buyer,niin) if buyer and niin else ('episode',component['episode_id'])
   previous=combined.get(key)
   if previous is None:combined[key]={**component,'related_requirements':list(component['related_requirements'])}
   else:
    related=previous['related_requirements']+component['related_requirements']
    winner=component if component['included_value']>previous['included_value'] else previous
    combined[key]={**winner,'related_requirements':related,'aggregation':'Largest priced scenario for this buyer/item across contract and item feeds; related records are not added'}
 components=sorted(combined.values(),key=lambda x:(-x['included_value'],x['episode_id']))
 overall=dict(count=sum(x['count'] for x in result.values()),priced_source_count=sum(x['priced_source_count'] for x in result.values()),priced_requirement_count=len(components),priced_demand_total=float(sum(Decimal(str(x['included_value'])) for x in components)) if components else None,value_components=components)
 scale=float(counts.get('observed_prime_net_obligations') or 0)
 forecast=forecast_summary(priced)
 coverage=coverage_summary(business or {},overall,forecast)
 try:annual_scale=max(float((business or {}).get('annual_net_obligations') or 0),0)
 except (ValueError,TypeError):annual_scale=0
 focus=max(1000000,annual_scale*0.01) if annual_scale>=100000000 else 1000000 if scale>=100000000 else 0
 return dict(release=release,policy=POLICY,business_coverage=coverage,forecast=forecast,categories=result,overall=overall,materiality_floor=focus,materiality_basis='Default priority focus scales to 1% of observed annual obligations for sites above $100m annually, with a $1m floor. A $1m focus also applies where lifetime observed obligations exceed $100m. Editable; smaller prospects remain available.',value_explanation='The headline combines priced contract and item requirements across the complete company feed. It uses stated quantities and exact-item reference or historical unit prices. Repeated requirements for the same buyer and item contribute their largest scenario once, including overlaps between contract and item feeds. Historical contract comparisons, programme totals and forecasts are excluded. Government reference dates may be unknown; prices are not current quotes. Unpriced opportunities are not assumed to be worth zero. These are demand estimates, not expected company revenue.')


def forecast_summary(rows):
 """Forecast scenarios stay separate from published requirements and revenue."""
 by_item={}
 for row in rows:
  if row.get('commercial_value_kind')!='forecast_scenario' or row.get('valuation_status')!='catalogue_EA_assumed_forecast_unit_not_stated':continue
  try:amount=Decimal(str(row['commercial_value']))
  except (InvalidOperation,TypeError,ValueError):continue
  if not amount.is_finite() or amount<=0:continue
  niin=str(row.get('item_reference_niin') or row['episode_id'])
  previous=by_item.get(niin)
  if previous is None or amount>previous[0]:by_item[niin]=(amount,row)
 years={};components=[]
 for niin,(amount,row) in by_item.items():
  try:annual=json.loads(row.get('annual_scenarios_json') or '[]')
  except (TypeError,ValueError):annual=[]
  for year in annual:
   value=year.get('market_price_scenario') if year.get('market_price_scenario') is not None else year.get('government_reference_scenario')
   if value is None:continue
   key=str(year['calendar_year']);years[key]=years.get(key,Decimal(0))+Decimal(str(value))
  components.append({**row,'included_value':float(amount),'niin':niin,'aggregation':'One item forecast across its full stated future period; not added to published requests'})
 components.sort(key=lambda x:(-x['included_value'],x['episode_id']))
 return dict(estimated_demand_total=float(sum((v[0] for v in by_item.values()),Decimal(0))) if by_item else None,priced_items=len(by_item),dated_market_price_items=sum(bool(r.get('market_price_median')) for _,r in by_item.values()),first_month=min((str(r.get('window_start')) for _,r in by_item.values() if r.get('window_start')),default=None),last_month=max((str(r.get('window_end')) for _,r in by_item.values() if r.get('window_end')),default=None),annual_values=[dict(calendar_year=y,estimated_demand=float(v)) for y,v in sorted(years.items())],value_components=components,interpretation='DLA forecast quantities priced using recent observed same-item/unit medians, otherwise government references. Catalogue EA is assumed because the forecast source omits units; non-EA conversions and conflicts are excluded. Forecasts can change and may overlap published requirements. These figures are separate from published demand and expected company revenue. Stock and reorder snapshots are context, not a deduction from the source forecast.')


def coverage_summary(business,published,forecast):
 """Expose coverage shortfalls; never manufacture opportunities to fill a target."""
 def num(key):
  try:
   v=float(business.get(key))
   return v if v==v and abs(v)!=float('inf') else None
  except (ValueError,TypeError):return None
 annual=num('annual_net_obligations');target=3*max(annual,0) if annual is not None else None
 current=published.get('priced_demand_total');future=forecast.get('estimated_demand_total')
 upper=(current or 0)+(future or 0) if current is not None or future is not None else None
 status='not_measurable' if not target or upper is None else 'below_benchmark_even_before_overlap' if upper<target else 'requires_overlap_and_access_review'
 context=dict(business)
 if 'interpretation' in context:context['catalogue_sales_interpretation']=context.pop('interpretation')
 return dict(**context,target_multiple=3,coverage_benchmark=target,coverage_status=status,
  dod_coverage_benchmark=3*max(num('annual_dod_net_obligations') or 0,0) if num('annual_dod_net_obligations') is not None else None,
  published_demand_to_annual_ratio=current/annual if annual and annual>0 and current is not None else None,
  full_horizon_forecast_to_annual_ratio=future/annual if annual and annual>0 and future is not None else None,
  interpretation='Three times trailing-year net federal obligations is a coverage benchmark, not a forecast or promised pipeline. Published requirements and full-horizon forecasts remain separate because they may overlap. Unpriced work is unknown, not zero. Reseller product-market sales are historical context and are not added to obligations, manufacturer revenue or future pipeline.')
