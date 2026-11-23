import copy
from dataclasses import replace
from decimal import Decimal
import re
import unittest
from unittest.mock import Mock

import duckdb

from market_scope_membership import Binding, KEY, MEMBERSHIP_COLUMNS, extent_digest, project_memberships, _digest
from market_scope_contract import SCOPE_POLICY
from market_scope_detail import DETAIL_COLUMNS, compile_scope_query
from market_scope_continuity import audit_scope_carry
from market_scope_build import ScopeCarryRelations
from market_scope_facets import canonical_facets_sql
from market_professional_detail import query_for, read_page


def fixture():
    binding = Binding(*[char * 64 for char in 'abcd'])
    suffix = 'f' * 20
    descriptor = {'schemaVersion': 1, 'mainRelease': 'mimir-main-platform-v7-test',
        'candidateId': 'cohesive-market-abcd', 'binding': binding.__dict__,
        'bindingSha256': binding.sha256, 'proofSha256': 'e' * 64,
        'factTable': 'market_scope_financial_facts_' + suffix,
        'membershipTable': 'market_scope_memberships_' + suffix,
        'stockTable': 'market_native_stock_links_' + suffix,
        'factColumns': sorted(DETAIL_COLUMNS), 'membershipColumns': sorted(MEMBERSHIP_COLUMNS)}
    descriptor['outputInventory'] = {role:{'database':'market_intel_platform_candidate','table':table,
        'catalogVersion':'1','schemaSha256':'9'*64,'objectInventoryPin':{
            'key':'governance/market_intelligence/candidates/test/'+role+'.json','version':'v1','sha256':'8'*64}}
        for role,table in [('facts',descriptor['factTable']),('memberships',descriptor['membershipTable']),
                           ('stock',descriptor['stockTable']),('detail','market_professional_detail_'+suffix)]}
    descriptor['outputBindingSha256'] = _digest(descriptor['outputInventory'])
    descriptor['policy'] = copy.deepcopy(SCOPE_POLICY)
    descriptor['policySha256'] = _digest(SCOPE_POLICY)
    payload = {'mainRelease': descriptor['mainRelease'], 'candidateId': descriptor['candidateId'],
        'market': 'US_FIGHTER_AIRCRAFT', 'view': 'contracts', 'years': [2025, 2026],
        'nodes': ['US_FIGHTER_AIRCRAFT'], 'scopeMembership': descriptor,
        'table': 'market_professional_detail_' + suffix}
    facts = []
    for native, year, amount, allocation in [('same-native', 2025, '100.00', 'a'),
            ('same-native', 2026, '-20.00', 'a'), ('equal-native', 2026, '100.00', 'a'),
            ('zero-native', 2026, '0.00', 'a'), ('split-native', 2026, '60.00', 'a'),
            ('split-native', 2026, '40.00', 'b')]:
        facts.append(dict(zip(KEY, ('PRIME_AWARD', 'CONT_AWD_TEST', year, native, allocation)),
            scope_sha256='1' * 64, market_node='US_AIRCRAFT_PROPULSION', net_usd=amount))
    extents = [{**{k: f[k] for k in (*KEY, 'scope_sha256')}, 'decision_id': 'engine-fighter',
                'decision_revision': 1, 'binding_sha256': binding.sha256}
               for f in facts if not (f['native_record_id'] == 'split-native' and f['allocation_id'] == 'b')]
    digest = extent_digest(extents)
    for row in extents: row['extent_sha256'] = digest
    decision = {'decision_id': 'engine-fighter', 'decision_revision': 1, 'review_status': 'APPROVED',
        'registry_sha256': binding.registry_sha256, 'target_dimension': 'market',
        'target_id': 'US_FIGHTER_AIRCRAFT', 'relationship': 'dedicated',
        'evidence_sha256': '2' * 64, 'extent_sha256': digest}
    bridge = project_memberships(binding, [decision], extents)
    return payload, facts, bridge


class ScopeIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.payload, self.facts, self.bridge = fixture()
        self.db = duckdb.connect()
        self.db.execute('CREATE SCHEMA market_intel_platform_candidate')
        self.db.execute('CREATE MACRO json_format(x) AS CAST(x AS varchar)')
        self.db.create_function('regexp_like', lambda value, pattern: bool(re.search(pattern, value)), [str, str], bool)
        self.d = self.payload['scopeMembership']
        columns = ','.join(k + (' INTEGER' if k == 'fiscal_year' else ' DECIMAL(20,2)' if k == 'net_usd'
                else ' VARCHAR[]' if k == 'platform_ids' else ' VARCHAR') for k in sorted(DETAIL_COLUMNS))
        self.db.execute(f"CREATE TABLE market_intel_platform_candidate.{self.d['factTable']} ({columns})")
        self.db.execute(f"CREATE TABLE market_intel_platform_candidate.{self.d['membershipTable']} (" +
            ','.join(k + (' INTEGER' if k in ('fiscal_year', 'decision_revision') else ' VARCHAR') for k in sorted(MEMBERSHIP_COLUMNS)) + ')')
        for fact in self.facts:
            values = {**fact, 'company': 'Engine awardee', 'uei': 'QN1BCFY7JDJ5', 'cage': '52661',
                'customer': 'NAVY', 'contract_id': 'TEST', 'description': 'Engine activity',
                'platform_ids': ['F_35'], 'report_year': str(fact['fiscal_year'])}
            self.db.execute(f"INSERT INTO market_intel_platform_candidate.{self.d['factTable']} VALUES (" +
                ','.join('?' for _ in DETAIL_COLUMNS) + ')', [values.get(k) for k in sorted(DETAIL_COLUMNS)])
        for row in self.bridge * 2:  # duplicate evidence paths must never duplicate finance
            self.db.execute(f"INSERT INTO market_intel_platform_candidate.{self.d['membershipTable']} VALUES (" +
                ','.join('?' for _ in MEMBERSHIP_COLUMNS) + ')', [row[k] for k in sorted(MEMBERSHIP_COLUMNS)])
        self.db.execute(f"CREATE TABLE market_intel_platform_candidate.{self.d['stockTable']} "
            '(source_kind VARCHAR,source_record_id VARCHAR,fiscal_year INTEGER,native_record_id VARCHAR,nsn VARCHAR)')
        for year, nsn in [(2025, '4820011111111'), (2026, '4820012222222'), (2026, '4820013333333')]:
            self.db.execute(f"INSERT INTO market_intel_platform_candidate.{self.d['stockTable']} VALUES (?,?,?,?,?)",
                ['PRIME_AWARD', 'CONT_AWD_TEST', year, 'same-native', nsn])

    def tearDown(self):
        self.db.close()

    def rows(self, payload=None):
        query = compile_scope_query(payload or self.payload)
        cur = self.db.execute(query.sql)
        return [dict(zip([c[0] for c in cur.description], row)) for row in cur.fetchall()]

    def test_union_partial_signed_zero_equal_values_and_existing_primary(self):
        self.assertEqual(self.rows()[0]['net'], Decimal('240.00'))
        p = copy.deepcopy(self.payload); p['nodes'] += ['US_AIRCRAFT_PROPULSION']
        self.assertEqual(self.rows(p)[0]['net'], Decimal('280.00'))
        p['activityMode'] = 'primary'; p['nodes'] = ['US_FIGHTER_AIRCRAFT']
        self.assertEqual(self.rows(p), [])
        metrics = self.db.execute(compile_scope_query(self.payload).metrics_sql).fetchall()
        self.assertEqual(sum(r[2] for r in metrics), 5)
        self.assertEqual(sum(r[5] for r in metrics), Decimal('240.00'))
        totals=self.db.execute(compile_scope_query(self.payload).totals_sql).fetchall()
        self.assertEqual(totals[0][2],1)
        self.assertEqual(sum(r[3] for r in metrics),2)
        self.assertEqual(totals[0][3],5)

    def test_action_full_year_grain_and_stock_join_never_repeat_money(self):
        p = {**self.payload, 'view': 'contract-actions', 'award': 'CONT_AWD_TEST'}
        rows = self.rows(p)
        self.assertEqual(len(rows), 5)
        self.assertEqual(sum(r['net'] for r in rows), Decimal('240.00'))
        selected = [r for r in rows if r['native_record_id'] == 'same-native']
        self.assertEqual(len({r['id'] for r in selected}), 2)
        by_year = {r['fiscal_year']: r for r in selected}
        self.assertNotIn('4820013333333', by_year[2025]['stock_nsns'])
        self.assertNotIn('4820011111111', by_year[2026]['stock_nsns'])

    def test_company_customer_nsn_views_and_metadata(self):
        for view in ['contracts', 'contractors', 'customers', 'award-nsns']:
            with self.subTest(view=view):
                rows = self.rows({**self.payload, 'view': view})
                self.assertEqual(sum(r['net'] for r in rows), Decimal('240.00'))
        self.assertEqual(self.rows()[0]['uei'], 'QN1BCFY7JDJ5')

    def test_single_source_contract_and_release_pins_fail_closed(self):
        for change in [lambda p:p.update(sources=['PRIME_AWARD', 'DLA_AWARD']),
                       lambda p:p.update(sources=['PRIME_AWARD', 'PRIME_AWARD']),
                       lambda p:p['scopeMembership'].update(bindingSha256='0' * 64),
                       lambda p:p['scopeMembership'].update(mainRelease='mimir-main-platform-v7-other'),
                       lambda p:p.update(award='CONT_AWD_UNSUPPORTED'),
                       lambda p:p['scopeMembership']['outputInventory']['facts'].update(table='market_scope_financial_facts_'+'0'*20),
                       lambda p:p.update(includeShared='false'), lambda p:p.update(nodes='US')]:
            p = copy.deepcopy(self.payload); change(p)
            with self.assertRaises(ValueError): compile_scope_query(p)

    def test_immutable_context_and_complete_award_envelope(self):
        p = copy.deepcopy(self.payload); p.update(view='contract-actions', award='CONT_AWD_TEST')
        q = compile_scope_query(p); original = q.context_sha256
        p['scopeMembership']['binding']['registry_sha256'] = 'f' * 64
        q.selection['award'] = 'WRONG'
        self.assertEqual(q.context_sha256, original)
        self.assertEqual(q.selection['award'], 'CONT_AWD_TEST')
        with self.assertRaises(ValueError): compile_scope_query(p)
        self.assertEqual(query_for(self.payload)[0], compile_scope_query(self.payload).sql)

    def test_real_pager_binds_cursors_to_new_proof_without_new_query(self):
        athena = Mock(); athena.start_query_execution.return_value = {'QueryExecutionId': 'a' * 8 + '-1234-1234-1234-' + 'b' * 12}
        athena.get_query_execution.return_value = {'QueryExecution': {'Status': {'State': 'SUCCEEDED'}}}
        athena.get_query_results.return_value = {'ResultSet': {'ResultSetMetadata': {'ColumnInfo': [{'Name': 'id'}, {'Name': 'net'}]},
            'Rows': [{'Data': [{'VarCharValue': 'id'}, {'VarCharValue': 'net'}]}, {'Data': [{'VarCharValue': 'CONT_AWD_TEST'}, {'VarCharValue': '240.00'}]}]}, 'NextToken': 'page2'}
        one = read_page(athena, self.payload, 'local-secret', 'bucket', '868631722720')
        self.assertEqual(one['selection']['bindingSha256'], self.d['bindingSha256'])
        p = copy.deepcopy(self.payload); p['cursor'] = one['next']; p['scopeMembership']['proofSha256'] = 'f' * 64
        with self.assertRaises(ValueError): read_page(athena, p, 'local-secret', 'bucket', '868631722720')
        self.assertEqual(athena.start_query_execution.call_count, 1)

    def test_first_tier_and_dla_stay_separate_from_prime_market_size(self):
        binding = Binding(**self.d['binding'])
        for source, amount, view in [('FIRST_TIER_REPORT','50.00','subcontracts'),('DLA_AWARD','70.00','parts')]:
            fact = {**self.facts[0],'source_kind':source,'source_record_id':source+'-record',
                'native_record_id':source+'-native','fiscal_year':2026,'net_usd':amount}
            values = {**fact,'company':source+' supplier','prime_company':'Parent prime',
                'customer':'NAVY','platform_ids':['F_35'],'report_year':'2026'}
            self.db.execute(f"INSERT INTO market_intel_platform_candidate.{self.d['factTable']} VALUES (" +
                ','.join('?' for _ in DETAIL_COLUMNS)+')',[values.get(k) for k in sorted(DETAIL_COLUMNS)])
            extent = {**{k:fact[k] for k in (*KEY,'scope_sha256')},'decision_id':source,
                'decision_revision':1,'binding_sha256':binding.sha256}
            digest=extent_digest([extent]);extent['extent_sha256']=digest
            decision={'decision_id':source,'decision_revision':1,'review_status':'APPROVED',
                'registry_sha256':binding.registry_sha256,'target_dimension':'market',
                'target_id':'US_FIGHTER_AIRCRAFT','relationship':'dedicated',
                'evidence_sha256':'2'*64,'extent_sha256':digest}
            row=project_memberships(binding,[decision],[extent])[0]
            self.db.execute(f"INSERT INTO market_intel_platform_candidate.{self.d['membershipTable']} VALUES ("+
                ','.join('?' for _ in MEMBERSHIP_COLUMNS)+')',[row[k] for k in sorted(MEMBERSHIP_COLUMNS)])
            rows=self.rows({**self.payload,'view':view})
            self.assertEqual(sum(r['net'] for r in rows),Decimal(amount))
            self.assertEqual(rows[0]['company'],source+' supplier')
        self.assertEqual(self.rows()[0]['net'],Decimal('240.00'))

    def test_canonical_normalizer_carries_all_existing_roles_and_subjects(self):
        key=[self.facts[0][k] for k in KEY]
        for table in ['own_subjects','own_holds']:
            self.db.execute('CREATE TABLE '+table+' ('+','.join(k+(' INTEGER' if k=='fiscal_year' else ' VARCHAR') for k in KEY)+',facet_id VARCHAR,relationship VARCHAR)')
            self.db.execute('INSERT INTO '+table+' VALUES (?,?,?,?,?,?,?)',key+['flow.valves' if table=='own_subjects' else 'held-own-scope','retained'])
        self.db.execute(f"UPDATE market_intel_platform_candidate.{self.d['factTable']} SET work_type='services.engineering',work_role='OWN_WORK',application_domain='AIR',technology_subject='flow.valves'")
        query=canonical_facets_sql('market_intel_platform_candidate.'+self.d['factTable'],
            'own_subjects','own_holds','market_intel_platform_candidate.'+self.d['membershipTable'],
            fact_columns=DETAIL_COLUMNS,edge_columns=(*KEY,'facet_id','relationship'))
        rows=self.db.execute(query).fetchall()
        self.assertEqual({r[5] for r in rows},{'mapping','subject','hold','association'})
        tags={r[6] for r in rows}
        self.assertTrue({'work_role:OWN_WORK','application_domain:AIR','technology_subject:flow.valves','platform:F_35'}<=tags)
        with self.assertRaises(ValueError):
            canonical_facets_sql('facts','subjects','holds','memberships',fact_columns=DETAIL_COLUMNS,edge_columns=KEY[:-1])


class CarryGateTests(unittest.TestCase):
    def setUp(self):
        _, self.old, _ = fixture(); self.new = copy.deepcopy(self.old)
        self.facets = [(tuple(r[k] for k in KEY), ('market', 'US_AIRCRAFT_PROPULSION')) for r in self.old]
        self.subjects = [(self.facets[0][0], ('work-subject', 'flow.valves'))]
        self.holds = [(self.facets[1][0], ('hold', 'changed-own-scope'))]

    def run_gate(self, **changes):
        args = dict(previous_subjects=self.subjects,current_subjects=self.subjects,
                    previous_holds=self.holds,current_holds=self.holds)
        args.update(changes)
        return audit_scope_carry(self.old,self.new,self.facets,args.pop('current_facets',self.facets),**args)

    def test_value_change_sign_zero_and_new_rows_preserve_every_facet(self):
        self.new[0]['net_usd'] = '-1234.56'; self.new[1]['net_usd'] = '0.00'
        result = self.run_gate()
        self.assertEqual(result['repriced_slices_not_reclassified'], 2)
        self.assertEqual(result['financial_writes'], 0)

    def test_lost_unrelated_mapping_subject_or_hold_blocks_projection(self):
        for kwargs in [{'current_facets':self.facets[1:]}, {'current_subjects':[]}, {'current_holds':[]}]:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError): self.run_gate(**kwargs)

    def test_retired_native_requires_authority_and_is_not_reinserted(self):
        removed = self.new.pop(); key = tuple(removed[k] for k in KEY)
        with self.assertRaises(ValueError): self.run_gate(current_facets=self.facets[:-1])
        result = self.run_gate(current_facets=self.facets[:-1], retired=[{'key':key,'authority_sha256':'a'*64}])
        self.assertEqual(result['retired_dispositions'],1)
        self.assertEqual(len(self.new),5)

    def test_scope_and_identity_changes_need_exact_authority(self):
        oldkey = self.facets[0][0]; self.new[0]['native_record_id'] = 'reviewed-reissue'
        key = tuple(self.new[0][k] for k in KEY)
        facets = [(key if k==oldkey else k,v) for k,v in self.facets]
        subjects = [(key,v) for _,v in self.subjects]
        with self.assertRaises(ValueError): self.run_gate(current_facets=facets,current_subjects=subjects)
        result = self.run_gate(current_facets=facets,current_subjects=subjects,
            identity_transitions=[{'old_key':oldkey,'current_key':key,'authority_sha256':'b'*64}])
        self.assertEqual(result['reviewed_identity_transitions'],1)


class DistributedCarryGateTests(unittest.TestCase):
    def setUp(self):
        _, self.facts, _ = fixture()
        self.db = duckdb.connect()
        self.db.create_function('regexp_like', lambda value, pattern: bool(re.search(pattern,value)), [str,str], bool)
        self.gate = ScopeCarryRelations('old_facts','new_facts','old_facets','new_facets',
                                       'aliases','retired','removals','a'*64)
        self.kcols = ','.join(k + (' INTEGER' if k=='fiscal_year' else ' VARCHAR') for k in KEY)
        for name in ['old_facts','new_facts']:
            self.db.execute(f'CREATE TABLE {name} ({self.kcols},scope_sha256 VARCHAR,net_usd DECIMAL(20,2))')
            for row in self.facts:
                self.db.execute(f'INSERT INTO {name} VALUES (?,?,?,?,?,?,?)',[row[k] for k in (*KEY,'scope_sha256','net_usd')])
        for name in ['old_facets','new_facets','removals']:
            extra=',authority_sha256 VARCHAR,binding_sha256 VARCHAR' if name=='removals' else ''
            self.db.execute(f'CREATE TABLE {name} ({self.kcols},kind VARCHAR,facet_id VARCHAR,relationship VARCHAR{extra})')
        for i,row in enumerate(self.facts):
            for name in ['old_facets','new_facets']:
                self.db.execute(f'INSERT INTO {name} VALUES (?,?,?,?,?,?,?,?)',
                    [row[k] for k in KEY]+['mapping','US_AIRCRAFT_PROPULSION','primary'])
                if i==0:
                    for kind,target in [('subject','flow.valves'),('hold','own-scope-review')]:
                        self.db.execute(f'INSERT INTO {name} VALUES (?,?,?,?,?,?,?,?)',
                            [row[k] for k in KEY]+[kind,target,'retained'])
        self.db.execute('CREATE TABLE aliases ('+','.join(prefix+k+(' INTEGER' if k=='fiscal_year' else ' VARCHAR')
            for prefix in ['old_','current_'] for k in KEY)+',authority_sha256 VARCHAR,binding_sha256 VARCHAR)')
        self.db.execute(f'CREATE TABLE retired ({self.kcols},authority_sha256 VARCHAR,binding_sha256 VARCHAR)')

    def tearDown(self): self.db.close()

    def issues(self): return dict(self.db.execute(self.gate.validation_sql()).fetchall())

    def test_distributed_current_value_and_all_canonical_facets(self):
        self.db.execute('UPDATE new_facts SET net_usd=-net_usd*2')
        self.assertEqual(self.issues(),{})
        self.db.execute("DELETE FROM new_facets WHERE facet_id='flow.valves'")
        self.assertEqual(self.issues()['LOST_PRIOR_FACET'],1)

    def test_retirement_requires_bound_source_authority_without_reinsertion(self):
        row=self.facts[-1];key=[row[k] for k in KEY]
        self.db.execute('DELETE FROM new_facts WHERE allocation_id=\'b\'')
        self.db.execute('DELETE FROM new_facets WHERE allocation_id=\'b\'')
        self.assertIn('LOST_NATIVE_WITHOUT_DISPOSITION',self.issues())
        self.db.execute('INSERT INTO retired VALUES (?,?,?,?,?,?,?)',key+['b'*64,'a'*64])
        self.assertEqual(self.issues(),{})
        self.db.execute("UPDATE retired SET binding_sha256='stale'")
        self.assertIn('STALE_AUTHORITY',self.issues())

    def test_duplicate_financial_slices_and_lost_holds_are_rejected(self):
        self.db.execute('INSERT INTO new_facts SELECT * FROM new_facts LIMIT 1')
        self.assertIn('DUPLICATE_CURRENT_FINANCIAL_SLICE',self.issues())
        self.db.execute("DELETE FROM new_facets WHERE kind='hold'")
        self.assertIn('LOST_PRIOR_FACET',self.issues())

    def test_same_native_scope_change_requires_exact_bound_authority(self):
        self.db.execute("UPDATE new_facts SET scope_sha256='"+'2'*64+"' WHERE fiscal_year=2025")
        self.assertIn('UNREVIEWED_SCOPE_CHANGE',self.issues())
        self.db.execute('CREATE TABLE scope_review ('+self.kcols+',old_scope_sha256 VARCHAR,current_scope_sha256 VARCHAR,authority_sha256 VARCHAR,binding_sha256 VARCHAR)')
        key=[self.facts[0][k] for k in KEY]
        self.db.execute('INSERT INTO scope_review VALUES (?,?,?,?,?,?,?,?,?)',key+['1'*64,'2'*64,'b'*64,'a'*64])
        self.gate=replace(self.gate,approved_scope_changes='scope_review')
        self.assertEqual(self.issues(),{})
        self.db.execute("UPDATE scope_review SET current_scope_sha256='"+'3'*64+"'")
        self.assertIn('INVALID_SCOPE_CHANGE_AUTHORITY',self.issues())

    def test_retired_and_aliased_native_dispositions_are_mutually_exclusive(self):
        oldkey=[self.facts[0][k] for k in KEY];newkey=oldkey.copy();newkey[3]='reviewed-reissue'
        self.db.execute("UPDATE new_facts SET native_record_id='reviewed-reissue' WHERE fiscal_year=2025")
        self.db.execute("UPDATE new_facets SET native_record_id='reviewed-reissue' WHERE fiscal_year=2025")
        self.db.execute('INSERT INTO aliases VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',oldkey+newkey+['b'*64,'a'*64])
        self.assertEqual(self.issues(),{})
        self.db.execute('INSERT INTO retired VALUES (?,?,?,?,?,?,?)',oldkey+['b'*64,'a'*64])
        self.db.execute('DELETE FROM new_facets WHERE fiscal_year=2025')
        issues=self.issues()
        self.assertIn('CONFLICTING_NATIVE_DISPOSITION',issues)
        self.assertIn('LOST_PRIOR_FACET',issues)


if __name__ == '__main__': unittest.main()
