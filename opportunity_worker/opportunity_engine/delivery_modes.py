"""Evidence constraints for the work being bought, separate from noun similarity.

PSC categories are a negative/compatibility constraint, never a relevance match.
The statement itself must still identify the required product/work family.
"""
import html,re
PSC_REFERENCE='https://origin-www.acquisition.gov/sites/default/files/manual/PSC%20Manual%20April%202025.pdf'
PSC_MODES={'Y':'facility_construction','Z':'facility_construction','J':'maintenance_repair','N':'equipment_installation'}
_REPAIR=re.compile(r'\b(?:repair\w*|overhaul\w*|remanufactur\w*|rebuild\w*)\b',re.I)
_INSTALL=re.compile(r'\b(?:install(?:ation|ing|ed)?|renovat\w*|construct\w*|refurbish\w*|alteration)\b',re.I)
_FACILITY=re.compile(r'\b(?:toilet partitions?|bathrooms?|restrooms?|roofs?|roofing|paving|pavement|masonry|carpet|buildings?|conference center)\b',re.I)
_ONSITE=re.compile(r'\bon[- ]site\b[^.;]{0,100}\b(?:repair\w*|replacement|install\w*|maintenance|inspection|services?)\b|\b(?:replacement|installation)\s+services\b',re.I)
_RESEARCH=re.compile(r'\b(?:research|prototyp\w*|experimental)\b|\bR\s*&\s*D\b',re.I)
_PRODUCT=re.compile(r'\b(?:assembl\w*|pumps?|valves?|actuators?|generators?|toilet|wire rope|circuit cards?|bearings?|motors?|sensors?|filters?|connectors?|switches?|housings?)\b',re.I)

def text(value):return re.sub(r'\s+',' ',re.sub('<[^>]+>',' ',html.unescape(str(value or '')))).strip()
def _code_mode(psc):return PSC_MODES.get(str(psc or '').strip().upper()[:1])

def required_mode(title,body='',psc='',structured=False):
 title=text(title).replace('_',' ');body=text(body) if structured else ''
 if _RESEARCH.search(title):return 'research_development','The title identifies research or prototype development; existing-item qualification is separate.'
 if structured and _code_mode(psc)=='facility_construction':return 'facility_construction','The requirement is coded as PSC '+str(psc)+' (construction or maintenance/alteration of structures/facilities).'
 if _FACILITY.search(title) and (_INSTALL.search(title) or re.search(r'\breplacement\b',title,re.I)):
  return 'facility_construction','The title identifies a building/facility construction, installation or replacement task.'
 m=_ONSITE.search(title) or _ONSITE.search(body)
 if m:return 'onsite_services','The requirement explicitly calls for '+m.group(0)+'.'
 if _REPAIR.search(title):return 'maintenance_repair','The title explicitly requires repair, overhaul or rebuilding.'
 if _INSTALL.search(title) and not re.search(r'\b(?:kits?|manuals?|tools?|hardware)\b',title,re.I):return 'equipment_installation','The title explicitly requires installation/construction work rather than only an equipment delivery.'
 if structured and _code_mode(psc):return _code_mode(psc),'The requirement is coded as PSC '+str(psc)+'; compatibility still requires specific work evidence.'
 if structured and re.fullmatch(r'\d{4}',str(psc or '').strip()):return 'component_supply','The requirement has a tangible-product PSC; the source scope still determines manufacturing and technical access.'
 return 'not_established','The retained title and applicable requirement coding do not establish a delivery mode.'

def observed_mode(work,psc=''):
 work=text(work)
 if _ONSITE.search(work):return 'onsite_services'
 if _RESEARCH.search(work):return 'research_development'
 if (_FACILITY.search(work) and _INSTALL.search(work)) or _code_mode(psc)=='facility_construction':return 'facility_construction'
 if _REPAIR.search(work) or _code_mode(psc)=='maintenance_repair':return 'maintenance_repair'
 if (_INSTALL.search(work) and not re.search(r'\b(?:kits?|manuals?|tools?|hardware)\b',work,re.I)) or _code_mode(psc)=='equipment_installation':return 'equipment_installation'
 if re.fullmatch(r'\d{4}',str(psc or '').strip()) or _PRODUCT.search(work):return 'component_supply'
 return 'not_established'

def mode_fit(work,required,work_psc=''):
 observed=observed_mode(work,work_psc)
 allowed={'facility_construction':{'facility_construction'},'onsite_services':{'onsite_services'},'maintenance_repair':{'maintenance_repair','onsite_services'},'equipment_installation':{'equipment_installation','onsite_services','facility_construction'}}
 if required in allowed and observed not in allowed[required]:
  labels={'facility_construction':'facility construction or alteration','onsite_services':'onsite services','maintenance_repair':'equipment repair or overhaul','equipment_installation':'equipment installation','component_supply':'component supply','not_established':'an unspecified type of work','research_development':'research and development'}
  return False,observed,'The requirement calls for '+labels[required]+'. The selected history shows '+labels.get(observed,observed)+', which does not establish that service delivery capability.'
 if required=='component_supply' and observed in ('maintenance_repair','equipment_installation','facility_construction','onsite_services'):
  return False,observed,'The selected history describes service work; it does not independently establish new manufacturing or supply capability for this requirement.'
 return True,observed,'No delivery-mode conflict was identified. This does not establish source qualification, eligibility or capacity.'
