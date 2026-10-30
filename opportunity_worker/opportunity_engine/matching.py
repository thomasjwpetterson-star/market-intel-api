"""Bounded, explainable retrieval for new research topics, not item interchangeability.
All available company work participates; no top-company cutoff or NAICS-only match.
"""
import collections,datetime,json,math,re
from pathlib import Path
STOP=set('the and for with from this that into through other advanced systems system research development technology technical technologies capability capabilities support service services engineering model using used based new rapid maturation agile modern improved novel approach approaches solution solutions both unit high low performance design integration project programme program demonstration application applications manufacturing production state art enabled process automatic autonomous'.split())
LOCKS=set('counter cooling refueling synchronization illumination manipulation robotic laser telemetry photonic hypersonic'.split())
def tokens(s):
 s=re.sub(r'(?i)counter[- ]?(?:uas|uav|drone)s?', ' counter unmanned ',str(s or ''))
 out=[]
 for w in re.findall('[a-z][a-z0-9]+',s.lower()):
  if w in ('uas','uav','drone','drones'):w='unmanned'
  if w in ('cable','cables','wires'):w='wire'
  if w.startswith('manipulat'):w='manipulation'
  if w.startswith('countermeasure'):w='counter'
  if w in ('robot','robots','robotics'):w='robotic'
  if w not in STOP and len(w)>=3:out.append(w)
 return set(out)
def match_topics(c,topics,out,as_of):
 qs={r['topic_id']:tokens(r['title']) for r in topics};vocab=set().union(*qs.values()) if qs else set();df=collections.Counter();n=0
 # Structured provenance stays attached to each work statement.
 sql="""SELECT doc_id,company_id,source_kind,work_text,last_observed,sample_evidence_id FROM company_work WHERE try_cast(substr(last_observed,1,10) AS DATE)>=DATE '2019-01-01'
 UNION ALL SELECT award_id,company_id,'sbir_award',title||' '||abstract,award_date,award_id FROM oi_research_awards WHERE evidence_state='observed_research_activity' AND try_cast(award_date AS DATE)>=DATE '2019-01-01'"""
 cur=c.execute(sql)
 while True:
  rows=cur.fetchmany(5000)
  if not rows:break
  for _,_,_,work,_,_ in rows:df.update(tokens(work)&vocab);n+=1
 weights={w:math.log((n+1)/(df[w]+1))+1 for w in vocab};results={};cur=c.execute(sql)
 while True:
  rows=cur.fetchmany(5000)
  if not rows:break
  for doc,cid,kind,work,date,eid in rows:
   if not cid or str(date)[:10]>as_of:continue
   ws=tokens(work)&vocab
   if len(ws)<2:continue
   for tid,terms in qs.items():
    hit=terms&ws
    if len(hit)<2 or not terms:continue
    locks=terms&LOCKS
    if locks and not hit&locks:continue
    coverage=sum(weights[t] for t in hit)/sum(weights[t] for t in terms)
    if coverage<.48 or min(df[t] for t in hit)>max(2,n*.02):continue
    key=(tid,cid);item=dict(doc_id=doc,source_kind=kind,evidence_id=eid,matched_terms=sorted(hit),weighted_title_coverage=round(coverage,4),work_text=work,observed_date=str(date)[:10])
    prior=results.setdefault(key,[]);prior.append(item);prior.sort(key=lambda x:(-x['weighted_title_coverage'],x['doc_id']));del prior[3:]
 path=Path(out)/'company_topic_matches.jsonl';rows=[]
 for (tid,cid),evidence in sorted(results.items()):
  rows.append(dict(topic_id=tid,company_id=cid,score=evidence[0]['weighted_title_coverage'],evidence_json=json.dumps(evidence),match_basis='Specific title concepts recur in observed work; capability similarity only',applicant_eligibility='not_established',technical_qualification='not_established',company_addressable_value=None))
 path.write_text(''.join(json.dumps(r)+'\n' for r in rows));return path,dict(documents_screened=n,topics=len(topics),company_topic_pairs=len(rows),method='At least two title concepts, a functional discriminator where present, one term in <=2% of work documents and >=48% IDF-weighted title coverage. All matches require technical and programme-entry review; score is not a win probability.')
