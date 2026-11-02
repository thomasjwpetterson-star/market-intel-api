"""Read-only adapter for the versioned opportunity release. No build/ingest code."""
import json
import copy
from concurrent.futures import Future
import threading
import time
from collections import OrderedDict
import boto3
from botocore.config import Config
BUCKET = 'a-and-d-intel-lake-newaccount'
DATABASE = 'market_intel_platform_candidate'
PREFIX = 'governance/opportunity_intelligence'

class AWS:
    def __init__(self):
        cfg = Config(connect_timeout=5, read_timeout=15, retries={'max_attempts': 2, 'mode': 'standard'})
        self.s3 = boto3.client('s3', region_name='us-east-1', config=cfg)
        self.athena = boto3.client('athena', region_name='us-east-1', config=cfg)
        self.cache = OrderedDict()
        self.lock = threading.Lock()
        self.pending = {}

    def result(self, sql):
        # Share identical concurrent public evidence queries. Each caller gets
        # its own row objects because presentation enrichers may mutate them.
        with self.lock:
            cached = self.cache.get(sql)
            if cached and cached[0] > time.monotonic():
                return copy.deepcopy(cached[1]), {}
            future = self.pending.get(sql)
            leader = future is None
            if leader:
                future = Future()
                self.pending[sql] = future
        if not leader:
            rows, execution = future.result(timeout=60)
            return copy.deepcopy(rows), execution
        try:
            rows, execution = self._execute(sql)
            with self.lock:
                self.cache[sql] = (time.monotonic() + 300, copy.deepcopy(rows))
                while len(self.cache) > 128:
                    self.cache.popitem(last=False)
            future.set_result((copy.deepcopy(rows), execution))
            return rows, execution
        except BaseException as error:
            future.set_exception(error)
            raise
        finally:
            with self.lock:
                self.pending.pop(sql, None)

    def _execute(self, sql):
        qid = self.athena.start_query_execution(
            QueryString=sql, QueryExecutionContext={'Database': DATABASE},
            ResultConfiguration={'OutputLocation': 's3://' + BUCKET + '/athena-query-results/'},
            WorkGroup='primary',
            ResultReuseConfiguration={'ResultReuseByAgeConfiguration': {'Enabled': True, 'MaxAgeInMinutes': 5}})['QueryExecutionId']
        started = time.monotonic()
        while True:
            execution = self.athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']
            state = execution['Status']['State']
            if state in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
                break
            if time.monotonic() - started > 40:
                self.athena.stop_query_execution(QueryExecutionId=qid)
                raise TimeoutError('Opportunity query timed out')
            time.sleep(.5)
        if state != 'SUCCEEDED':
            raise RuntimeError('Opportunity query failed')
        rows, names = [], None
        for page in self.athena.get_paginator('get_query_results').paginate(QueryExecutionId=qid):
            for row in page['ResultSet']['Rows']:
                values = [cell.get('VarCharValue') for cell in row['Data']]
                if names is None:
                    names = values
                else:
                    rows.append(dict(zip(names, values)))
        return rows, execution
