"""Read-only adapter for the versioned opportunity release. No build/ingest code."""
import json
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

    def result(self, sql):
        # Immutable release table names are part of each key. Cached results
        # therefore cannot cross a release change. No user-private data is cached.
        with self.lock:
            cached = self.cache.get(sql)
            if cached and cached[0] > time.monotonic():
                return cached[1], {}
        qid = self.athena.start_query_execution(
            QueryString=sql, QueryExecutionContext={'Database': DATABASE},
            ResultConfiguration={'OutputLocation': 's3://' + BUCKET + '/athena-query-results/'},
            WorkGroup='primary')['QueryExecutionId']
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
        with self.lock:
            self.cache[sql] = (time.monotonic() + 60, rows)
            while len(self.cache) > 128:
                self.cache.popitem(last=False)
        return rows, execution
