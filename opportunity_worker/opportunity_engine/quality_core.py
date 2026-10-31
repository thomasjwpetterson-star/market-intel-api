"""Deterministic scope/access and notice-coherence checks, with quoted evidence."""
import datetime,hashlib,html,json,re
from pathlib import Path
from .company_enrichment import STOP,words
from .delivery_modes import required_mode,mode_fit

VERSION='scope-and-source-v6'
NOISE=set('form fit function consideration extension extensions extend extended modification modifications modify reduce reduced reduction increase increased decrease funding funded obligation deobligation cost costs reimbursable reimbursement supplies procurement purchase purchased purchasing payment total ceiling administrative contractual west east north south coast seaport nxg arsenal garrison afb'.split())
GENERAL=set('custom computer computers programming software upgrade upgrades updated update maintenance services emerging threat threats strategic trends initiative requirements requirement support general education training research development engineering technical management professional consulting analysis logistics'.split())
ADMIN=set('critical contractor contractors contract contracts order orders po clin clins line lines item items number numbers reference references fee fees payment payments quantity quantities award awards option options'.split())
# A facility name can help identify the customer/site, but cannot establish
# technical capability. Do not maintain hand-written company/location aliases.
_FACILITY_LOCATION=re.compile(r"\b[A-Za-z][A-Za-z'-]{1,30}\s+(?:Arsenal|AFB|Air Force Base)\b|\bFort\s+[A-Za-z][A-Za-z'-]{1,30}\b",re.I)
_STATES='Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming|District of Columbia|AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC'
_STATE_NAMES,_STATE_CODES=_STATES.split('|AL|',1)
_POSTAL_SCOPE=re.compile(r"^[a-z][a-z .'-]{0,55},\s*(?:(?:"+_STATE_NAMES+r")(?:[ .,]*\d{5}(?:-\d{4})?)?|(?:AL|"+_STATE_CODES+r")[ ,]+\d{5}(?:-\d{4})?)[ .]*(?:USA|US|United States)?[ .]*$",re.I)
LOGIC_HASH=hashlib.sha256(Path(__file__).read_bytes()+(Path(__file__).parent/'scope_stopwords.json').read_bytes()+(Path(__file__).parent/'delivery_modes.py').read_bytes()).hexdigest()

def plain(value):return re.sub(r'\s+',' ',re.sub('<[^>]+>',' ',html.unescape(str(value or '')))).strip()
def terms(value):return set(w for w in words(_FACILITY_LOCATION.sub(' ',plain(value))) if len(w)>=3 and w not in STOP and w not in NOISE)
def administrative_scope(value):
 if _POSTAL_SCOPE.fullmatch(plain(value)):return True
 t=terms(value)
 semantic={w for w in t if not any(ch.isdigit() for ch in w)}
 return not t or t<=GENERAL or not semantic or semantic<=(GENERAL|ADMIN)
def title_score(work,title):return len((terms(work)-GENERAL)&terms(re.split(r'(?i)_End_Item_|\bNSN\b',str(title or ''))[0]))

_MONTH=r'(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)'
_DATE=re.compile(r'\b(?:\d{1,2}\s+'+_MONTH+r'\s+20\d{2}|'+_MONTH+r'\s+\d{1,2}(?:st|nd|rd|th)?[,]?\s+20\d{2}|20\d{2}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/20\d{2})\b',re.I)
def date(value):
 if not value:return None
 text=re.sub(r'(\d)(?:st|nd|rd|th)\b',r'\1',str(value),flags=re.I).replace(',','').replace('Sept ','Sep ')
 for pattern in ('%Y-%m-%d','%d %B %Y','%d %b %Y','%B %d %Y','%b %d %Y','%m/%d/%Y'):
  try:return datetime.datetime.strptime(text[:10] if pattern=='%Y-%m-%d' else text,pattern).date()
  except ValueError:pass
 return None

def nomenclature(body):
 m=re.search(r'\b(?:NOM|NOMENCLATURE|ITEM NAME)\s*:\s*(.{3,180}?)\s*(?=\b(?:P/?N|PART NUMBER|APPROVED|NSN|QUANTITY)\s*[:(]|$)',plain(body),re.I)
 return m.group(1).strip(' ;,') if m else ''
def required_parts(title,body):
 text=plain(str(title or '').replace('_',' ')+' '+str(body or ''))
 return {re.sub('[^A-Z0-9]','',p) for m in re.finditer(r'\b(?:P/?N|PART[ -]?NUMBER)\s*[:#]?\s*([A-Z0-9][A-Z0-9./_-]{1,80})',text,re.I) for p in m.group(1).upper().split('/')}
def listed_sources(body):
 text=plain(body);out=set()
 for m in re.finditer(r'\bCAGE(?:\s+CODE)?\s*[:(]?\s*([A-Z0-9]{5})\b',text,re.I):
  if re.search(r'approved|manufacturer|\bOEM\b|source',text[max(0,m.start()-160):m.start()],re.I):out.add(m.group(1).upper())
 return out
def restricted_item(body):
 return bool(re.search(r'trac(?:eability|ing).{0,100}(?:OEM|original equipment manufacturer)|(?:OEM|original equipment manufacturer).{0,100}traceability|no (?:alternat|substitut)|only (?:acceptable|approved).{0,30}(?:source|part|manufacturer)',plain(body),re.I))
def explicit_qualification(body):
 return bool(re.search(r'source approval request|source approval process|qualif(?:y|ication).{0,45}(?:new|additional|alternate).{0,20}(?:source|manufacturer)|(?:new|additional|alternate).{0,25}(?:source|manufacturer).{0,50}(?:approv|qualif)|alternate.{0,30}(?:part|item).{0,30}(?:consider|accept)',plain(body),re.I))

def notice_quality(title,body,end,as_of,source):
 findings=[];passages=[];deadline=None;kind='general_procurement';category=None
 if source=='sam_notices':
  text=plain(body);dates=[]
  for m in _DATE.finditer(text):
   before=text[max(0,m.start()-220):m.start()]
   if re.search(r'\b(?:response|responses|return|submit|submission|submissions|received|receive|closing|deadline)\b',before,re.I) and re.search(r'\b(?:by|before|due|deadline|closing|later than|NLT)\b',before[-130:],re.I):
    value=date(m.group(0))
    if value:dates.append((value,text[max(0,m.start()-200):m.end()+60]))
  if dates:
   last,quote=max(dates,key=lambda x:x[0]);deadline=last.isoformat()
   if last<date(as_of) and not (date(end) and date(end)>=date(as_of)):
    findings.append('The notice text gives a past response deadline and no current structured deadline overrides it. Current availability is not established.');passages.append(quote)
  named=nomenclature(text);a=terms(re.split(r'(?i)_End_Item_|\bNSN\b',title or '')[0])-GENERAL;b=terms(named)-GENERAL
  if len(a)>=2 and len(b)>=2 and not a&b:
   findings.append('The notice title and its explicit item nomenclature identify different product families; source coherence needs verification.');passages.append(named)
  if restricted_item(text) and not explicit_qualification(text):kind='existing_item_sourcing_inquiry';category='items'
  elif explicit_qualification(text):kind='new_source_qualification'
  # A sourcing invitation for a specified component remains item demand.
  # Its qualification route is preserved separately from its display category.
  title_items=set(re.findall(r'\bNSN\s*[:#]?\s*(\d{4}[- ]?\d{2}[- ]?\d{3}[- ]?\d{4})(?!\d)',str(title or '').replace('_',' '),re.I))
  if len(title_items)==1 or (re.search(r'\bProposed procurement for NSN\s+\d{13}\b',text,re.I) and not re.search(r'\brepair\w*|\boverhaul\w*',str(title or ''),re.I)):category='items'
 if source=='sbir_topics':kind='research_eligibility_check'
 return dict(verdict='source_review_required' if findings else 'usable',reason=' '.join(findings),body_deadline=deadline,passages_json=json.dumps(passages),source_entry_kind=kind,category_override=category)

def fit_fields(work,title,named,entry,source,cage,restricted=False,qualification=False,source_cages=(),has_authority=False,delivery_mode='not_established',work_psc=''):
 if administrative_scope(work):return 'rejected','The selected history is administrative wording or an unspecific service/classification label, not evidence of the required capability.'
 target=terms(re.split(r'(?i)_End_Item_|\bNSN\b',title or '')[0]);target|=terms(named)
 overlap=(terms(work)-GENERAL)&target
 if entry!='reported_supplier_relationship' and not overlap:
  return 'rejected','The selected company history does not identify the deliverable named in the opportunity title or item nomenclature; incidental body wording is insufficient.'
 if entry!='reported_supplier_relationship' and len(terms(work)&target)<2:
  return 'rejected','Only a single product/work word is shared with the named requirement. The selected history does not establish the complete deliverable or work family.'
 compatible,observed,reason=mode_fit(work,delivery_mode,work_psc)
 if not compatible:return 'review',reason
 if entry=='reported_supplier_relationship':return 'accepted','This site is a named prior supplier in the evidenced program/vehicle work stream.'
 if source=='sbir_topics':return 'review','Research-topic relevance is not SBIR/STTR prime eligibility or an established teaming route.'
 if restricted and not qualification:
  if cage in source_cages or has_authority:return 'accepted','Exact item/source evidence supports an existing-item supply check; the inquiry does not establish approval to manufacture substitutes.'
  return 'review','This inquiry requires existing OEM supply/traceability. Comparable products do not establish access to the specified item.'
 return 'accepted','The company history identifies the named work/product family. Qualification, eligibility and available work remain source-specific checks.'

def fit_review(work,title,body,lane,entry,source,cage,has_authority=False,work_psc='',target_psc=''):
 mode,_=required_mode(title,body,target_psc,source=='sam_notices')
 return fit_fields(work,title,nomenclature(body),entry,source,cage,restricted_item(body),explicit_qualification(body),listed_sources(body),has_authority,mode,work_psc)
