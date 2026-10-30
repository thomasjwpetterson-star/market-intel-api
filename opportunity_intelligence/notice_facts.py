"""Conservative, quoted single-item quantity facts from the retained notice text."""
import html,re,json
from decimal import Decimal,InvalidOperation,ROUND_HALF_UP
VERSION='explicit-single-item-quantity-v1'
_PATTERN=re.compile(r'\b(Estimated\s+(?:Spares\s+Requirement|Quantity)|Quantity\s+(?:Required|Requested))\s*[:=]\s*(\d[\d,]*)\s*(EA|EACH|UNITS?|SETS?|KITS?)\b',re.I)

def quantity_fact(row):
 if row.get('source_name')!='sam_notices':return {}
 text=re.sub(r'\s+',' ',re.sub(r'<[^>]+>',' ',html.unescape(str(row.get('scope_text') or '')))).strip()
 items={re.sub(r'\D','',x) for x in re.findall(r'(?<!\d)\d{4}[- ]?\d{2}[- ]?\d{3}[- ]?\d{4}(?!\d)',str(row.get('title') or '')+' '+text)}
 if len(items)!=1:return {}
 matches=list(_PATTERN.finditer(text))
 if not matches:return {}
 # Do not turn multiple schedules, ranges or options into a single lot size.
 values={(int(m.group(2).replace(',','')),m.group(3).upper()) for m in matches}
 if len(values)!=1:return {}
 count,unit=next(iter(values));quote=matches[0].group(0)
 if not 0<count<=1000000000:return {}
 if any(re.search(r'\b(?:minimum|maximum|option|optionally|range|annual|monthly)\b',text[max(0,m.start()-28):m.start()],re.I) for m in matches):return {}
 if any(re.search(r'\b(?:per|each)\s+(?:month|year|annum)\b|\b(?:monthly|annually)\b',text[m.end():m.end()+30],re.I) for m in matches):return {}
 units={'EACH':'EA','UNIT':'EA','UNITS':'EA','SETS':'SET','KITS':'KIT'}
 estimated=any(m.group(1).lower().startswith('estimated') for m in matches)
 return dict(notice_quantity=count,notice_quantity_nsn=next(iter(items)),notice_quantity_unit=units.get(unit,unit),notice_quantity_basis='Source-estimated requirement; not an order' if estimated else 'Quantity stated for this single-item requirement; not an award',notice_quantity_evidence=quote,notice_quantity_policy=VERSION,notice_quantity_source_evidence_id=row.get('source_evidence_id'))

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
