from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
import threading
import pytest
from opportunity_intelligence.aws import AWS


def reader(execute):
    value=object.__new__(AWS);value.cache=OrderedDict();value.lock=threading.Lock();value.pending={};value._execute=execute
    return value


def test_identical_concurrent_queries_execute_once_and_do_not_share_mutable_rows():
    entered=threading.Event();release=threading.Event();calls=[]
    def execute(sql):
        calls.append(sql);entered.set();assert release.wait(3)
        return [dict(title='source',nested=['source'])],{'QueryExecutionId':'q'}
    aws=reader(execute)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first=pool.submit(aws.result,'SELECT release_a');assert entered.wait(3)
        second=pool.submit(aws.result,'SELECT release_a');release.set()
        a=first.result()[0];b=second.result()[0]
    a[0]['nested'].append('changed');assert b[0]['nested']==['source']
    assert aws.result('SELECT release_a')[0][0]['nested']==['source'] and calls==['SELECT release_a']


def test_failed_query_is_not_cached_and_later_attempt_can_retry():
    calls=[]
    def execute(sql):
        calls.append(sql)
        if len(calls)==1:raise RuntimeError('temporary')
        return [dict(ok=True)],{}
    aws=reader(execute)
    with pytest.raises(RuntimeError):aws.result('SELECT release_a')
    assert aws.pending=={} and aws.result('SELECT release_a')[0]==[dict(ok=True)]


def test_release_table_changes_do_not_reuse_the_old_release_cache():
    calls=[]
    def execute(sql):calls.append(sql);return [dict(sql=sql)],{}
    aws=reader(execute);aws.result('SELECT * FROM oi_release_a');aws.result('SELECT * FROM oi_release_b')
    assert len(calls)==2
