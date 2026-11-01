import hashlib,json
from pathlib import Path
import duckdb
import pytest
from opportunity_engine.historical_purchases import compile_history,merge,refresh,KEY

HEADER='NIIN|SECURITY_CLASSIFICATION|FSC|UNIT|CAGE|CONTRACT_NUMBER|ORDER_QTY|AWARD_DATE|NETPRICE|PO_NUM|PO_ITMNO|ITEM_NAME|PART_NUMBER|STD_U_PRICE|NSN\n'
def raw(tmp_path,rows):
 p=tmp_path/'raw.txt';p.write_text(HEADER+'\n'.join('|'.join(r) for r in rows)+'\n');return p
def line(part='PART-A',price='10.00',security='U',date='20250815'):
 return ['012345678',security,'1234','EA','27541','CONTRACT', '2.000',date,price,'PO1','00001','Widget',part,'8.00','1234012345678']
def binding(p):return dict(key=KEY,sha256=hashlib.sha256(p.read_bytes()).hexdigest())

def test_physical_cash_is_retained_once_and_ambiguous_parts_never_get_an_exact_reference(tmp_path):
 p=raw(tmp_path,[line(),line('PART-B'),line(security='S')]);compiled,stats=compile_history(p,tmp_path/'out',binding(p),'2026-10-31')
 c=duckdb.connect();r=c.execute('SELECT record_amount_usd,part_number,source_url,source_row_locator,source_sha256 FROM read_parquet(?)',[str(compiled)]).fetchone()
 assert r==(20.0,None,None,'PO1 | 00001',binding(p)['sha256'])
 assert stats['public_rows']==2 and stats['physical_financial_lines']==1 and stats['ambiguous_part_lines']==1
 assert stats['raw_rows']==3

def test_conflicting_financial_versions_and_invalid_years_fail_before_publication(tmp_path):
 p=raw(tmp_path,[line(),line(price='12.00')])
 with pytest.raises(ValueError,match='Conflicting'):compile_history(p,tmp_path/'conflict',binding(p),'2026-10-31')
 p=raw(tmp_path,[line(date='20260815')])
 with pytest.raises(ValueError,match='Malformed'):compile_history(p,tmp_path/'wrongyear',binding(p),'2026-10-31')

def test_overlapping_snapshots_prefer_current_financial_evidence_and_keep_source_provenance(tmp_path):
 p=raw(tmp_path,[line()]);old,_=compile_history(p,tmp_path/'old',binding(p),'2026-10-31')
 p=raw(tmp_path,[line(price='15.00')]);new,_=compile_history(p,tmp_path/'new',binding(p),'2026-10-31')
 result,rows=merge([str(new),str(old)],tmp_path/'merged','2026-10-31')
 assert rows==1
 assert duckdb.connect().execute('SELECT record_amount_usd,source_sha256 FROM read_parquet(?)',[str(result)]).fetchone()==(30.0,binding(p)['sha256'])

def test_a_corrected_annual_source_does_not_retain_withdrawn_older_rows(tmp_path):
 p=raw(tmp_path,[line()]);old,stats=compile_history(p,tmp_path/'old',binding(p),'2026-10-31')
 result,rows=merge([str(old)],tmp_path/'merged','2026-10-31',('s3://a-and-d-intel-lake-newaccount/'+KEY,'different-new-source-sha'))
 assert rows==0

def test_failed_upstream_check_retains_complete_loaded_history_and_original_dates(tmp_path):
 p=raw(tmp_path,[line()]);compiled,stats=compile_history(p,tmp_path/'old',binding(p),'2026-10-31');sha=hashlib.sha256(compiled.read_bytes()).hexdigest()
 previous={'sources':{'historical_item_purchase_lines':dict(files=[dict(path=str(compiled),sha256=sha)],as_of='2026-10-31',sha256=sha)}}
 class S3:
  def head_object(self,**kwargs):raise PermissionError('private error')
 class AWS:s3=S3()
 result=refresh(previous,tmp_path/'refresh',AWS(),'2026-11-01')
 assert result['as_of']=='2026-10-31' and result['sha256']==sha
 assert result['refresh_health']['status']=='retained_historical_purchases_source_check_failed'
 assert 'private error' not in json.dumps(result)
 assert 'refresh_health' not in previous['sources']['historical_item_purchase_lines']

def test_versioned_source_refresh_archives_only_owned_outputs_and_preserves_monthly_rollover(tmp_path):
 p=raw(tmp_path,[line()]);content=p.read_bytes()
 class S3:
  def __init__(self):self.downloads=[]
  def head_object(self,**kwargs):return dict(VersionId='annual-version',ContentLength=len(content),LastModified='2026-08-21')
  def download_file(self,bucket,key,path,ExtraArgs=None):
   self.downloads.append((key,ExtraArgs));assert key==KEY;Path(path).write_bytes(content)
 class AWS:
  def __init__(self):self.s3=S3();self.uploads=[]
  def upload(self,path,key):assert key.startswith('governance/opportunity_intelligence/sources/');self.uploads.append(key)
 aws=AWS();first=refresh({'sources':{}},tmp_path/'first',aws,'2026-10-31')
 assert first['rows']==1 and first['upstream_binding']['version_id']=='annual-version'
 assert aws.s3.downloads==[(KEY,{'VersionId':'annual-version'})]
 c=duckdb.connect();monthly=tmp_path/'monthly.parquet'
 c.execute("CREATE TEMP TABLE monthly AS SELECT * REPLACE ('2026-10-01' AS transaction_date,'PO2 | 00001' AS source_row_locator,'new-month' AS pricing_record_id,'new-financial' AS source_record_id,'other-sha' AS source_sha256,'s3://monthly' AS source_s3_uri) FROM read_parquet(?)",[first['path']])
 c.execute('COPY monthly TO ? (FORMAT PARQUET)',[str(monthly)])
 sha=hashlib.sha256(monthly.read_bytes()).hexdigest();second=refresh({'sources':{'historical_item_purchase_lines':first,'item_market_price_evidence':{'files':[dict(path=str(monthly),sha256=sha)]}}},tmp_path/'second',aws,'2026-11-01')
 assert second['rows']==2 and len(aws.s3.downloads)==1
 assert c.execute('SELECT min(transaction_date),max(transaction_date) FROM read_parquet(?)',[second['path']]).fetchone()==('2025-08-15','2026-10-01')
 assert second['raw_archives'][0]['original_s3_version_id']=='annual-version'

def test_reported_nsn_is_preserved_when_its_classification_differs_but_niin_agrees(tmp_path):
 row=line();row[2]='9999';p=raw(tmp_path,[row]);compiled,stats=compile_history(p,tmp_path/'out',binding(p),'2026-10-31')
 assert duckdb.connect().execute('SELECT nsn,niin FROM read_parquet(?)',[str(compiled)]).fetchone()==('1234012345678','012345678')
 assert stats['fsc_nsn_classification_differences']==1
