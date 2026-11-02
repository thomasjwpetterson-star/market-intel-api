import copy,datetime as dt,gzip,hashlib,io,json,unittest
from types import SimpleNamespace
from opportunity_intelligence.company_index import *
NOW=dt.datetime(2026,11,2,1,0,tzinfo=dt.timezone.utc)
def row(id,**kw):
 return dict(episode_id=id,record_hash=id+'hash',source_name='sam_notices',native_identifier=id,customer_name='Navy',title='Generator',lane='published_procurement',stage='published_requirement',item_demand='false',commercial_value='100',commercial_value_kind='item_reference_estimate',priority_score='12',window_end='2026-11-03',**kw)
def payload(rows,**kw):return dict(company_id='CAGE:45934',records=rows,plans=[],business={},**kw)
class PointIndexTests(unittest.TestCase):
 def test_relevance_places_live_requests_before_more_valuable_forecasts(self):
  live=row('live');forecast=row('plan');forecast.update(stage='source_forecast',commercial_value='5000000',priority_score='999',lane='source_item_forecasts')
  self.assertEqual(sorted([forecast,live],key=rank)[0]['episode_id'],'live')
  self.assertEqual(sorted([forecast,live],key=lambda r:rank(r,'value'))[0]['episode_id'],'plan')
 def test_deadlines_are_rechecked_with_calendar_and_utc_precision(self):
  expired=row('past');expired['window_end']='2026-11-01'
  today=row('today');today['window_end']='2026-11-02'
  timestamp=row('clock');timestamp['value_inputs_json']=json.dumps(dict(source_response_deadline_precision='timestamp_utc',source_response_deadline_utc='2026-11-02T00:59:59Z'))
  self.assertEqual([r['episode_id'] for r in eligible(payload([expired,today,timestamp]),now=NOW)],['today'])
 def test_family_grouping_is_after_kind_and_expiry_filters_and_preserves_sources(self):
  a=row('a');b=row('b');a.update(native_identifier='REQUEST-00',notice_posted_date='2026-11-01',item_demand='true');b.update(native_identifier='REQUEST',notice_posted_date='2026-11-02')
  p=payload([a,b]);before=copy.deepcopy(p)
  self.assertEqual(eligible(p,now=NOW)[0]['episode_id'],'b')
  self.assertEqual(eligible(p,'items',NOW)[0]['episode_id'],'a')
  self.assertEqual(len(json.loads(eligible(p,now=NOW)[0]['related_source_records_json'])),2)
  self.assertEqual(p,before)
 def test_expired_official_plan_releases_original_follow_on_without_rebuild(self):
  work=row('follow');work.update(lane='funded_contract_follow_on',stage='expected_demand',value_inputs_json='{"record_id":"award"}')
  p=payload([work]);p['plans']=[dict(award_key='award',window_end='2026-11-02')]
  self.assertEqual(eligible(p,now=NOW),[])
  self.assertEqual(len(eligible(p,now=NOW+dt.timedelta(days=1))),1)
 def test_forecast_does_not_enter_published_headline(self):
  a=row('live');b=row('plan');b.update(stage='source_forecast',lane='source_item_forecasts',item_demand='true',commercial_value='5000000',commercial_value_kind='forecast_scenario',valuation_status='catalogue_EA_assumed_forecast_unit_not_stated',item_reference_niin='016736255')
  m=dict(run_id='release',tables={});result=overview(payload([a,b]),m,NOW)
  self.assertEqual(result['overall']['priced_demand_total'],100)
  self.assertEqual(result['forecast']['estimated_demand_total'],5000000)
 def test_advertised_objects_validate_release_company_hash_and_complete_population(self):
  manifest=dict(run_id='release',as_of='2026-11-01',tables={})
  repo=SimpleNamespace(manifest=lambda:manifest)
  self.assertIsNone(CompanyIndex(repo).load('CAGE:45934'))
 def test_quarantined_subcontract_values_do_not_return_via_overview(self):
  a=row('bad');a.update(lane='recurring_subcontract_work',commercial_value='9900000000000',commercial_value_kind='historical_package_benchmark',stage='expected_demand')
  result=overview(payload([a]),dict(run_id='release',tables={}),NOW)
  self.assertIsNone(result['overall']['priced_demand_total']);self.assertEqual(result['overall']['larger_count'],0)

class FullPointReadTests(unittest.TestCase):
 def setUp(self):
  self.manifest=dict(run_id='release',as_of='2026-11-01',tables={})
  self.repo=SimpleNamespace(manifest=lambda:self.manifest)
  self.data={};self.repo.aws=SimpleNamespace(s3=self)
  import opportunity_intelligence.company_index as module
  self.old=module.contract;module.contract=lambda repo:'query';self.addCleanup(setattr,module,'contract',self.old)
  p=payload([row('live')],release='release',binding=binding(self.manifest))
  company=self.add('objects/x.json.gz',gzip.compress(encode(p)))
  shard=self.add('shards/x.json',encode({'45934':company}))
  root=dict(complete=True,release='release',binding=binding(self.manifest),contract='query',shards={'45':shard})
  raw=encode(root);self.manifest['_company_index']=dict(version=VERSION,sha256=hashlib.sha256(raw).hexdigest())
  self.data[self.prefix+'ready.json']=raw
 def add(self,name,raw):
  self.prefix=PREFIX+'/releases/release/company-index/'+VERSION+'/'
  key=self.prefix+name;self.data[key]=raw
  return dict(key=key,sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw))
 def get_object(self,**args):
  raw=self.data[args['Key']];return dict(Body=io.BytesIO(raw),ContentLength=len(raw))
 def test_complete_population_point_read_and_authoritative_empty_tail(self):
  index=CompanyIndex(self.repo)
  self.assertEqual(index.load('CAGE:45934')['records'][0]['episode_id'],'live')
  self.assertEqual(index.load('CAGE:45935')['records'],[])
  self.assertEqual(index.load('CAGE:99999')['records'],[])
 def test_corruption_is_unavailable_not_an_empty_feed_or_warehouse_fallback(self):
  self.data[self.prefix+'objects/x.json.gz']=b'corrupt'
  with self.assertRaises(IndexUnavailable):CompanyIndex(self.repo).load('CAGE:45934')
 def test_generation_mismatch_is_rejected(self):
  self.manifest['as_of']='2026-11-02'
  with self.assertRaises(IndexUnavailable):CompanyIndex(self.repo).load('CAGE:45934')
if __name__=='__main__':unittest.main()

class RepositoryPointReadTests(unittest.TestCase):
 def setUp(self):
  from opportunity_intelligence.api import Repository
  self.repo=Repository(object());self.manifest=dict(run_id='release',as_of='2026-11-01',tables={n:{} for n in ('opportunities','demand_decisions','opportunity_vocabulary','work_vocabulary_topics','company_matches')})
  self.repo.manifest=lambda:self.manifest
  self.records=[]
  for n in range(150):
   r=row(str(n).zfill(4));r.update(source_name='dla_solicitations',window_end='2099-11-03',commercial_value=str(n*100000),priority_score=str(n))
   self.records.append(r)
  self.payload=payload(self.records)
  self.repo.indexed_company=lambda company:self.payload
  self.repo.rows=lambda sql:(_ for _ in ()).throw(AssertionError('Unexpected warehouse read'))
 def test_whole_population_value_filter_and_second_page_do_not_use_top_card_cache(self):
  first=self.repo.discovery(mode='company',company='CAGE:45934',minimum_value=10000000,include_unpriced=False,sort='value',limit=24)
  second=self.repo.discovery(mode='company',company='CAGE:45934',minimum_value=10000000,include_unpriced=False,sort='value',limit=24,offset=24)
  self.assertEqual(len(first['records']),24);self.assertEqual(len(second['records']),24)
  self.assertTrue(first['has_more']);self.assertTrue(second['has_more'])
  self.assertEqual(first['records'][0]['episode_id'],'0149');self.assertEqual(second['records'][0]['episode_id'],'0125')
  self.assertEqual(len(set(r['episode_id'] for r in first['records']) & set(r['episode_id'] for r in second['records'])),0)
 def test_unknown_and_zero_values_follow_existing_discovery_filter_rules(self):
  self.payload['records']=[dict(self.records[0],commercial_value=None),dict(self.records[1],commercial_value='0')]
  self.assertEqual(len(self.repo.discovery(mode='company',company='CAGE:45934',minimum_value=1e6)['records']),2)
  self.assertEqual(self.repo.discovery(mode='company',company='CAGE:45934',minimum_value=1e6,include_unpriced=False)['records'],[])
 def test_invalid_kind_is_rejected_before_fast_read(self):
  with self.assertRaises(ValueError):self.repo.opportunities('CAGE:45934',kind='bad')
 def test_read_does_not_mutate_saved_source_payload(self):
  original=copy.deepcopy(self.payload)
  self.repo.discovery(mode='company',company='CAGE:45934')
  self.repo.opportunities('CAGE:45934')
  self.assertEqual(self.payload,original)
 def test_quarantined_money_cannot_be_used_for_value_ranking(self):
  self.payload['records'][0].update(lane='recurring_subcontract_work',commercial_value='999000000000000',priority_score='999',stage='expected_demand')
  result=self.repo.discovery(mode='company',company='CAGE:45934',sort='value',minimum_value=1e6,include_unpriced=False)
  self.assertNotIn('0000',[r['episode_id'] for r in result['records']])
 def test_market_query_uses_held_action_values_not_award_ceilings(self):
  from opportunity_intelligence.company_markets import query
  self.repo.table=lambda name:'safe.'+name
  q=query(self.repo,'45934');self.assertIn('action_obligation',q);self.assertNotIn('reported_ceiling',q);self.assertIn("awarding_agency_code='097'",q)
  all_sites=query(self.repo,None);self.assertNotIn('CAGE:00000',all_sites);self.assertIn('PARTITION BY company_id',all_sites)
 def test_exact_item_history_is_not_limited_to_live_dibbs_requests(self):
  from opportunity_intelligence.item_notice_activity import query
  self.repo.table=lambda name:'safe.'+name
  q=query(self.repo,'a'*64);self.assertIn('sam_notices',q);self.assertIn('dla_solicitations',q);self.assertIn('recorded_stage',q);self.assertNotIn('current_date',q)
  with self.assertRaises(ValueError):query(self.repo,"a' OR true")
