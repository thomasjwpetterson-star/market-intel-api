"""Conservative, quoted single-item quantity facts from the retained notice text."""
import json
from decimal import Decimal,InvalidOperation,ROUND_HALF_UP
from opportunity_worker.opportunity_engine.notice_quantity import VERSION,quantity_fact

def price_scenario_fact(row):
 """Multiply only a quoted single-item quantity and same-item, same-unit price."""
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
  scenarios.append(dict(nsn=item['nsn'],quantity=int(quantity),unit=item['catalogue_unit'],unit_price=float(price),gross_reference_scenario=float(total),price_basis=basis,quantity_quote=row.get('notice_quantity_evidence'),price_evidence=item.get('catalogue_evidence'),interpretation='Quoted source quantity multiplied by the exact item reference price. Government reference date is not retained. Excludes engineering and nonrecurring costs; configuration, current pricing, qualification and company workshare remain unconfirmed. Not a current quote, award value or company revenue.'))
 return {'notice_item_lot_scenarios_json':json.dumps(scenarios)} if scenarios else {}
