"""Shared, quoted single-item quantity extraction; no price or eligibility inference."""
import html,re
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

