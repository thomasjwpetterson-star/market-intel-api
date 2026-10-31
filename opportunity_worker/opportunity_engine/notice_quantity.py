"""Shared, quoted single-item quantity extraction; no price or eligibility inference."""
import html,re
VERSION='explicit-single-item-quantity-v2'
_PATTERN=re.compile(r'\b(Estimated\s+(?:Spares\s+Requirement|Quantity)|Quantity\s+(?:Required|Requested))\s*[:=]\s*(\d[\d,]*)\s*(EA|EACH|UNITS?|SETS?|KITS?)\b',re.I)

def quantity_fact(row):
 if row.get('source_name')!='sam_notices':return {}
 text=re.sub(r'\s+',' ',re.sub(r'<[^>]+>',' ',html.unescape(str(row.get('scope_text') or '')))).strip()
 items={re.sub(r'\D','',x) for x in re.findall(r'(?<!\d)\d{4}[- ]?\d{2}[- ]?\d{3}[- ]?\d{4}(?!\d)',str(row.get('title') or '')+' '+text)}
 if len(items)!=1:return {}
 matches=list(_PATTERN.finditer(text))
 if not matches:
  # DLA's structured single-line prose is a statement of quantity, including
  # optional IDC estimates; a contract ceiling or minimum is not a quantity.
  if not re.search(r'\bProposed procurement for NSN\s+\d{13}\b',text,re.I):return {}
  lines=list(re.finditer(r'\bLine\s+(\d{4})\s+Qty\s+(\d[\d,]*)\s+UI\s+([A-Z]{2,3})\b',text,re.I))
  distinct={(m.group(1),int(m.group(2).replace(',','')),m.group(3).upper()) for m in lines}
  if len(distinct)!=1:return {}
  line,count,unit=next(iter(distinct))
  if not 0<count<=1000000000:return {}
  ceiling=re.search(r'aggregate total of orders.{0,120}?reach\s+\$?([0-9,]+(?:\.[0-9]{1,2})?)',text,re.I)
  cap=float(ceiling.group(1).replace(',','')) if ceiling else None
  estimated=bool(re.search(r'Automated IDC|Indefinite Delivery Contract',text,re.I))
  return dict(notice_quantity_is_estimate=estimated,notice_aggregate_ceiling_usd=cap,notice_aggregate_ceiling_evidence=ceiling.group(0) if ceiling else None,notice_quantity=count,notice_quantity_nsn=next(iter(items)),notice_quantity_unit=unit,notice_quantity_basis='Source-estimated IDC line quantity; not a guaranteed total or aggregate ceiling' if estimated else 'Quantity stated for this DLA line; not an award',notice_quantity_evidence=lines[0].group(0),notice_quantity_policy=VERSION,notice_quantity_source_evidence_id=row.get('source_evidence_id'))

 # Do not turn multiple schedules, ranges or options into a single lot size.
 values={(int(m.group(2).replace(',','')),m.group(3).upper()) for m in matches}
 if len(values)!=1:return {}
 count,unit=next(iter(values));quote=matches[0].group(0)
 if not 0<count<=1000000000:return {}
 if any(re.search(r'\b(?:minimum|maximum|option|optionally|range|annual|monthly)\b',text[max(0,m.start()-28):m.start()],re.I) for m in matches):return {}
 if any(re.search(r'\b(?:per|each)\s+(?:month|year|annum)\b|\b(?:monthly|annually)\b',text[m.end():m.end()+30],re.I) for m in matches):return {}
 units={'EACH':'EA','UNIT':'EA','UNITS':'EA','SETS':'SET','KITS':'KIT'}
 estimated=any(m.group(1).lower().startswith('estimated') for m in matches)
 return dict(notice_quantity_is_estimate=estimated,notice_quantity=count,notice_quantity_nsn=next(iter(items)),notice_quantity_unit=units.get(unit,unit),notice_quantity_basis='Source-estimated requirement; not an order' if estimated else 'Quantity stated for this single-item requirement; not an award',notice_quantity_evidence=quote,notice_quantity_policy=VERSION,notice_quantity_source_evidence_id=row.get('source_evidence_id'))

