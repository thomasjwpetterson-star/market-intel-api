"""Evidence-first work vocabulary over the official PSC hierarchy.

Reported PSC is preserved. Explicit scope labels supplement it; neither a
classification nor a topic overlap establishes technical interchangeability.
"""
import datetime,hashlib,html,json,re
from pathlib import Path

BASE=Path(__file__).parent
SPEC=json.loads((BASE/'work_topics.json').read_text())
VERSION=SPEC['version']
POLICY_HASH=hashlib.sha256(Path(__file__).read_bytes()+(BASE/'work_topics.json').read_bytes()+(BASE/'psc_reference.json').read_bytes()).hexdigest()
TOPICS=[(r,re.compile(r['pattern'],re.I)) for r in SPEC['topics']]
MODES=[(r,re.compile(r['pattern'],re.I)) for r in SPEC['work_types']]
ADMIN_ONLY=re.compile(r'^(?:(?:[A-Z0-9-]+\s+){0,6})(?:kick[- ]?off|status|progress|technical interchange|contract start[- ]?up)\s+meetings?(?:\s*#?\d+)?[.! ]*$',re.I)

def plain(text):
    return re.sub(r'\s+',' ',html.unescape(re.sub('<[^>]+>',' ',str(text or '')))).strip()

def administrative_only(text):
    return bool(ADMIN_ONLY.fullmatch(plain(text)))

def positive_hit(pattern,text):
    for hit in pattern.finditer(text):
        prefix=re.split(r'[.!?;:]',text[max(0,hit.start()-40):hit.start()])[-1]
        if not re.search(r'\b(?:no|not|without|excluding|except)\b.{0,25}$',prefix,re.I):return hit
    return None

def codebook(as_of):
    ref=json.loads((BASE/'psc_reference.json').read_text());groups={}
    for row in ref['records']:groups.setdefault(row['psc'],[]).append(row)
    result=[]
    for code,rows in sorted(groups.items()):
        active=[r for r in rows if (not r['effective_start'] or r['effective_start']<=as_of) and (not r['effective_end'] or r['effective_end']>=as_of)]
        candidates=active or [r for r in rows if not r['effective_start'] or r['effective_start']<=as_of] or rows
        row=max(candidates,key=lambda r:(r['effective_start'] or '',bool(r['label'])))
        parent=re.match(r'^([A-Z0-9]{1,4})\s*-',row['parent'] or '')
        result.append(dict(row,parent_code=parent[1] if parent else None,active_in_reference=bool(active),source_url=ref['source_url'],source_sha256=ref['source_sha256'],edition=ref['edition']))
    return result

def annotate(text,psc='',as_of=None,reference=None):
    text=plain(text);code=str(psc or '').strip().upper()
    if reference is None:reference={r['psc']:r for r in codebook(as_of or datetime.date.today().isoformat())}
    reported=reference.get(code)
    administrative=administrative_only(text)
    matches=[];modes=[]
    if not administrative:
        for rule,pattern in TOPICS:
            hit=positive_hit(pattern,text)
            if not hit:continue
            if rule.get('exclude') and re.search(rule['exclude'],text,re.I):continue
            matches.append(dict(topic_id=rule['id'],label=rule['label'],parent_id=rule['parent'],passage=text[max(0,hit.start()-45):min(len(text),hit.end()+85)],matched_phrase=hit[0],basis='explicit_scope_wording',rule_id=rule['id']))
        for rule,pattern in MODES:
            hit=positive_hit(pattern,text)
            if hit:modes.append(dict(work_type=rule['id'],label=rule['label'],matched_phrase=hit[0],passage=text[max(0,hit.start()-45):min(len(text),hit.end()+85)],basis='explicit_scope_wording'))
    # Product coding alone does not prove manufacturing, and J/R&D coding alone
    # remains a classification clue rather than a demonstrated work statement.
    status='administrative_only' if administrative else 'explicit_scope_labels' if matches else 'reported_psc_only' if reported else 'unclassified_scope'
    label=' / '.join(r['label'] for r in matches[:2]) or (reported or {}).get('label') or 'Work scope needs classification'
    if modes:label+=' · '+' / '.join(r['label'] for r in modes[:2])
    return dict(vocabulary_version=VERSION,policy_hash=POLICY_HASH,reported_psc=code or None,psc_label=(reported or {}).get('label'),psc_reference_status='recognized_reference_code' if reported else 'missing_or_unrecognized_code',scope_status=status,topic_ids=[r['topic_id'] for r in matches],topics=matches,work_types=modes,normalized_scope_label=label,qualification='not_established',technical_equivalence='not_established')

def evidence_verdict(text,history_kind=None,net=None,positive=None,record_id=None):
    if administrative_only(text):return 'rejected','Generic meeting wording is administrative context, not evidence of the required technical deliverable.'
    if history_kind=='prime_award' and not str(record_id or '').startswith('CONT_IDV_') and net is not None and positive is not None and float(net)<0 and float(positive)<=0:
        return 'review','The selected held award history contains a negative funding adjustment without positive obligations. Recover the original funded package or independent delivery evidence before relying on this capability.'
    return 'unchanged','Existing source and fit assessment retained; vocabulary labels do not establish qualification.'
