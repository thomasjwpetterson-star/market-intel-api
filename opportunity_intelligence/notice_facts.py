"""Conservative, quoted single-item quantity facts from the retained notice text."""
import json,re
from decimal import Decimal,InvalidOperation,ROUND_HALF_UP
from opportunity_worker.opportunity_engine.notice_quantity import VERSION,quantity_fact as sam_quantity_fact


def quantity_fact(row):
 """A modern U-type DIBBS quantity is an IDC estimate, not a firm order.

The source has changed the meaning of U historically; do not reinterpret
pre-FY2020 identifiers. Preserve exact fields and the method's public source.
 """
 if row.get('source_name')!='dla_solicitations':return sam_quantity_fact(row)
 identifier=str(row.get('native_identifier') or '').upper()
 if not re.fullmatch(r'(?:SPE|SPR)[A-Z0-9]{3}(?:2[0-9]|[3-9][0-9])U[A-Z0-9]{4}',identifier):return {}
 try:
  inputs=json.loads(row.get('value_inputs_json') or '{}')
  if not isinstance(inputs,dict):return {}
  quantity=Decimal(str(inputs.get('quantity')))
 except (InvalidOperation,ValueError,TypeError):return {}
 if not isinstance(inputs,dict) or not quantity.is_finite() or not 0<quantity<=1000000000 or quantity!=quantity.to_integral_value():return {}
 unit=str(inputs.get('request_unit') or '').strip().upper();nsn=str(inputs.get('nsn') or '')
 if not re.fullmatch(r'[A-Z]{2,3}',unit) or not re.fullmatch(r'[0-9]{13}',nsn):return {}
 return dict(notice_quantity=int(quantity),notice_quantity_nsn=nsn,notice_quantity_unit=unit,notice_quantity_is_estimate=True,notice_quantity_basis='Source-estimated IDC quantity; not a guaranteed order quantity. Check the solicitation minimum and ordering period.',notice_quantity_evidence='Structured DIBBS fields: solicitation '+identifier+'; quantity '+str(int(quantity))+'; unit '+unit,notice_quantity_policy='modern-dla-aidc-quantity-v1',notice_quantity_source_evidence_id=row.get('source_evidence_id'),notice_quantity_method_url='https://www.dla.mil/Portals/104/Documents/J7Acquisition/MasterSolicitation4ASAcqRev-102_September_29_2025.pdf')

def price_scenario_fact(row):
 """Multiply only a quoted single-item quantity and same-item, same-unit price."""
 if str(row.get('psc') or '').startswith(('J','N','Y','Z')) or row.get('required_delivery_mode') in ('maintenance_repair','equipment_installation','facility_construction','onsite_services','research_development') or re.search(r'\b(?:repair\w*|overhaul\w*|remanufactur\w*)\b',str(row.get('title') or ''),re.I):return {}
 if not row.get('notice_quantity') or not row.get('notice_quantity_nsn'):return {}
 try:items=json.loads(row.get('notice_item_pricing_json') or '[]')
 except (ValueError,TypeError):return {}
 if not isinstance(items,list) or len(items)!=1 or not isinstance(items[0],dict):return {}
 item=items[0]
 if item.get('nsn')!=row['notice_quantity_nsn'] or item.get('catalogue_unit')!=row.get('notice_quantity_unit'):return {}
 scenarios=[]
 for field,basis in [('government_reference_unit_price','Government reference'),('supplier_average_unit_price','Single supplier historical average')]:
  try:price=Decimal(str(item.get(field)));quantity=Decimal(str(row['notice_quantity']))
  except (InvalidOperation,ValueError,TypeError):continue
  if not price.is_finite() or price<=0 or not quantity.is_finite() or quantity<=0:continue
  total=(price*quantity).quantize(Decimal('.01'),rounding=ROUND_HALF_UP)
  try:cap=Decimal(str(row.get('notice_aggregate_ceiling_usd') or item.get('source_aggregate_ceiling_usd') or 0))
  except (InvalidOperation,ValueError,TypeError):continue
  if not cap.is_finite() or (cap>0 and total>cap):continue
  scenarios.append(dict(nsn=item['nsn'],quantity=int(quantity),unit=item['catalogue_unit'],unit_price=float(price),gross_reference_scenario=float(total),price_basis=basis,quantity_quote=row.get('notice_quantity_evidence'),price_evidence=item.get('catalogue_evidence'),interpretation='Quoted source quantity multiplied by the exact item reference price. Government reference date is not retained. Excludes engineering and nonrecurring costs; configuration, current pricing, qualification and company workshare remain unconfirmed. Not a current quote, award value or company revenue.'))
 return {'notice_item_lot_scenarios_json':json.dumps(scenarios)} if scenarios else {}
