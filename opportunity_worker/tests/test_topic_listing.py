import pytest
from bs4 import BeautifulSoup
from opportunity_engine.ingestion.topic_listing import collect

def page(ids,total,number=0,has_next=False):
 return BeautifulSoup('Showing 1–2 of '+str(total)+' results'+''.join('<h3><a href="/topics/'+str(i)+'">Topic</a></h3>' for i in ids)+'<a aria-current="page" href="?page='+str(number)+'">Current</a>'+('<a href="?page='+str(number+1)+'">Next</a>' if has_next else ''),'html.parser')

def test_rereads_overlap_without_lowering_expected_distinct_count():
 pages=iter([page([1,2],4,0,True),page([2,3],4,1),page([1,4],4,0,True),page([2,3],4,1)])
 calls=[]
 def fetch(url):calls.append(url);return next(pages),'hash','archive'
 links,evidence,expected=collect(fetch,'https://www.sbir.gov/topics?page=0')
 assert expected==4 and len(links)==4 and len(evidence)==4
 assert evidence[-1]['listing_pass']==2
 assert all(p['topic_urls'] for p in evidence)

def test_changed_population_is_not_presented_as_complete():
 pages=iter([page([1,2],4,0,True),page([3],3,1)])
 with pytest.raises(ValueError,match='population changed'):collect(lambda url:(next(pages),'hash','archive'),'https://www.sbir.gov/topics?page=0')

def test_missing_topic_still_fails_after_bounded_rereads():
 calls=[]
 def fetch(url):calls.append(url);return page([1,2],3),'hash','archive'
 with pytest.raises(ValueError,match='Incomplete'):collect(fetch,'https://www.sbir.gov/topics?page=0')
 assert len(calls)==3
