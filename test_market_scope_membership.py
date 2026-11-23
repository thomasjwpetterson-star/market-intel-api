"""Execute the exact membership selector against native-grain local fixtures."""
from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path
import unittest

import duckdb

from market_scope_membership import (
    Binding, KEY, FACT_COLUMNS, MEMBERSHIP_COLUMNS, NativeMarketSelection,
    extent_digest, project_memberships,
)

BINDING = Binding(*[char * 64 for char in 'abcd'])
ENGINE, FIGHTER, F35, F15, F16 = 'AIRCRAFT_PROPULSION_ENGINES', 'US_FIGHTER_AIRCRAFT', 'F_35', 'F_15', 'F_16'


class MembershipTests(unittest.TestCase):
    def setUp(self):
        self.db = duckdb.connect()
        self.addCleanup(self.db.close)
        self.db.execute('''CREATE TABLE facts(
            source_kind VARCHAR,source_record_id VARCHAR,fiscal_year INTEGER,
            native_record_id VARCHAR,allocation_id VARCHAR,scope_sha256 VARCHAR,
            market_node VARCHAR,net_usd DECIMAL(38,2),description VARCHAR,
            company VARCHAR,customer VARCHAR,nsn VARCHAR)''')
        self.db.execute('CREATE TABLE memberships(' + ','.join(
            field + (' INTEGER' if field in ('fiscal_year','decision_revision') else ' VARCHAR')
            for field in sorted(MEMBERSHIP_COLUMNS)) + ')')
        self.selector = NativeMarketSelection(BINDING,'facts','memberships',FACT_COLUMNS,MEMBERSHIP_COLUMNS)
        self.add('engine',100,ENGINE)
        self.add('airframe',200,FIGHTER)
        self.add('multi',120,ENGINE)
        self.add('credit',-20,ENGINE)
        self.add('zero',0,ENGINE)
        self.add('equal-a',100,'US')
        self.add('equal-b',100,'US')
        self.add('split',340,'US_SUBMARINES',allocation='PROGRAM')
        self.add('split',60,'US_SUBMARINES',allocation='INDUSTRIAL')
        self.associate('engine',FIGHTER)
        self.associate('engine',F35)
        self.associate('multi',FIGHTER)
        self.associate('multi',F15,relationship='shared')
        self.associate('multi',F16,relationship='shared')
        self.associate('credit',FIGHTER)
        self.associate('zero',FIGHTER)

    def add(self,award,amount,market,*,source='PRIME_AWARD',year=2026,native=None,allocation='ALL'):
        self.db.execute('INSERT INTO facts VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
            [source,award,year,native or award,allocation,'e'*64,market,amount,
             'original funded purpose '+award,'original awardee','original customer','1234567890123'])

    def authority(self,award,target,relationship='dedicated',allocation=None,revision=1):
        where = 'source_record_id=?' + (' AND allocation_id=?' if allocation else '')
        args = [award] + ([allocation] if allocation else [])
        fields = (*KEY,'scope_sha256')
        extents = [dict(zip(fields,row)) for row in self.db.execute(
            'SELECT '+','.join(fields)+' FROM facts WHERE '+where,args).fetchall()]
        extent_hash = extent_digest(extents)
        decision = dict(decision_id=award+'-'+target,decision_revision=revision,review_status='APPROVED',
            registry_sha256=BINDING.registry_sha256,
            target_dimension='platform' if target in (F35,F15,F16) else 'market',
            target_id=target,relationship=relationship,
            extent_sha256=extent_hash,evidence_sha256='f'*64)
        for extent in extents:
            extent.update(decision_id=decision['decision_id'],decision_revision=revision,
                extent_sha256=extent_hash,binding_sha256=BINDING.sha256)
        return decision,extents

    def associate(self,award,target,*,relationship='dedicated',allocation=None,duplicates=1):
        decision,extent = self.authority(award,target,relationship,allocation)
        rows = project_memberships(BINDING,[decision],extent)
        fields = sorted(MEMBERSHIP_COLUMNS)
        self.db.executemany('INSERT INTO memberships VALUES ('+','.join('?' for _ in fields)+')',
            [[row[field] for field in fields] for row in rows]*duplicates)

    def select(self,markets,**policies):
        self.assertEqual(self.db.execute(self.selector.validation_sql()).fetchall(),[])
        return self.db.execute(self.selector.selected_sql([n for n in markets if n not in (F35,F15,F16)],
            [2026],['PRIME_AWARD'],platforms=[n for n in markets if n in (F35,F15,F16)],**policies)).fetchdf()

    def test_engine_fighter_and_combined_union(self):
        self.assertEqual(self.select([ENGINE]).net_usd.sum(),200)
        self.assertEqual(self.select([FIGHTER]).net_usd.sum(),400)
        self.assertEqual(self.select([ENGINE,FIGHTER]).net_usd.sum(),400)
        self.assertEqual(self.select([F35]).net_usd.sum(),100)
        self.assertEqual(self.db.execute('SELECT sum(net_usd) FROM facts').fetchone()[0],1000)

    def test_multiple_paths_never_multiply_and_equal_dollars_are_distinct(self):
        self.associate('engine',FIGHTER,duplicates=3)
        self.associate('equal-a',F35)
        self.associate('equal-b',F35)
        chosen = self.select([FIGHTER,F35])
        self.assertEqual(chosen.net_usd.sum(),600)
        self.assertEqual(len(chosen),7)

    def test_family_dedicated_but_aircraft_shared_without_dollar_split(self):
        self.assertIn('multi',set(self.select([FIGHTER],include_shared=False).source_record_id))
        self.assertEqual(self.select([F15],include_shared=False).shape[0],0)
        self.assertEqual(self.select([F15,F16]).net_usd.sum(),120)
        self.assertEqual(self.select([F15,FIGHTER],include_shared=False).net_usd.sum(),400)

    def test_mixed_nonfighter_engine_not_inferred_dedicated(self):
        self.add('mixed-trainer-fighter',75,ENGINE)
        self.associate('mixed-trainer-fighter',FIGHTER,relationship='shared')
        self.assertEqual(self.select([FIGHTER]).net_usd.sum(),475)
        self.assertEqual(self.select([FIGHTER],include_shared=False).net_usd.sum(),400)

    def test_existing_primary_has_precedence_over_shared_path(self):
        self.associate('airframe',FIGHTER,relationship='shared')
        chosen = self.select([FIGHTER],include_shared=False)
        self.assertEqual(chosen[chosen.source_record_id=='airframe'].activity_bucket.tolist(),['primary_allocation'])

    def test_split_extent_cannot_expand_to_other_allocation(self):
        self.associate('split',F35,allocation='INDUSTRIAL')
        chosen = self.select([F35])
        self.assertEqual(chosen.net_usd.sum(),160)
        self.assertEqual(chosen[chosen.source_record_id=='split'].allocation_id.tolist(),['INDUSTRIAL'])
        decision,extent = self.authority('split','US_INDUSTRIAL',allocation='INDUSTRIAL')
        extra = deepcopy(extent[0]); extra['allocation_id']='PROGRAM'
        with self.assertRaisesRegex(ValueError,'extent differs'):
            project_memberships(BINDING,[decision],extent+[extra])

    def test_obsolete_revision_and_unknown_decision_rejected(self):
        decision,extent = self.authority('engine',F35)
        extent[0]['decision_revision'] = 2
        with self.assertRaisesRegex(ValueError,'active decision'):
            project_memberships(BINDING,[decision],extent)
        with self.assertRaisesRegex(ValueError,'active decision'):
            project_memberships(BINDING,[],extent)

    def test_unapproved_registry_or_main_mismatch_rejected(self):
        decision,extent = self.authority('engine',F35)
        for field,value in [('review_status','PENDING'),('registry_sha256','0'*64)]:
            changed=deepcopy(decision); changed[field]=value
            with self.assertRaises(ValueError): project_memberships(BINDING,[changed],extent)
        extent[0]['binding_sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'different Main'):
            project_memberships(BINDING,[decision],extent)

    def test_hold_suppresses_association_not_primary_money(self):
        decision,extent = self.authority('engine','HELD_MARKET')
        held = [tuple(extent[0][field] for field in KEY)]
        self.assertEqual(project_memberships(BINDING,[decision],extent,held),[])
        self.assertEqual(self.select([ENGINE]).net_usd.sum(),200)

    def test_value_changes_keep_membership_and_current_amount(self):
        before=self.db.execute('SELECT * FROM memberships ORDER BY decision_id').fetchall()
        self.db.execute("UPDATE facts SET net_usd=125 WHERE source_record_id='engine'")
        self.assertEqual(self.select([F35]).net_usd.sum(),125)
        self.assertEqual(before,self.db.execute('SELECT * FROM memberships ORDER BY decision_id').fetchall())
        decision,extent=self.authority('engine',F35)
        self.assertEqual(project_memberships(BINDING,[decision],extent)[0]['target_id'],F35)

    def test_metadata_native_keys_signed_and_zero_preserved(self):
        old=self.db.execute("SELECT * FROM facts WHERE source_record_id='engine'").fetchone()
        chosen=self.select([F35]).iloc[0]
        self.assertEqual(tuple(chosen.iloc[:-1]),old)
        selected=self.select([FIGHTER])
        self.assertEqual(selected[selected.source_record_id=='credit'].net_usd.tolist(),[-20])
        self.assertEqual(selected[selected.source_record_id=='zero'].net_usd.tolist(),[0])
        self.assertEqual(self.select([FIGHTER],activity=False).net_usd.sum(),200)

    def test_full_native_source_and_year_grain(self):
        self.add('engine',7,ENGINE,year=2025)
        self.add('engine',30,ENGINE,source='FIRST_TIER_REPORT')
        self.add('engine',40,ENGINE,source='DLA_AWARD')
        self.assertEqual(self.select([F35]).net_usd.sum(),100)
        sql=self.selector.fiscal_sql([ENGINE],[2025,2026],list(('PRIME_AWARD','FIRST_TIER_REPORT','DLA_AWARD')))
        rows=self.db.execute(sql).fetchall()
        amounts={(r[0],r[1]):float(r[-1]) for r in rows}
        self.assertEqual(amounts,{('PRIME_AWARD',2025):7,('PRIME_AWARD',2026):200,
            ('FIRST_TIER_REPORT',2026):30,('DLA_AWARD',2026):40})

    def test_legacy_rolled_up_professional_schema_rejected(self):
        with self.assertRaisesRegex(ValueError,'native allocation grain'):
            NativeMarketSelection(BINDING,'old_detail','memberships',{'source_record_id','net_usd'},MEMBERSHIP_COLUMNS)

    def test_null_amount_key_and_duplicate_gate(self):
        for field in ('allocation_id','native_record_id','net_usd'):
            self.db.execute('BEGIN')
            self.db.execute(f"UPDATE facts SET {field}=NULL WHERE source_record_id='engine'")
            self.assertIn('INCOMPLETE_FINANCIAL_SLICE',[r[0] for r in self.db.execute(self.selector.validation_sql()).fetchall()])
            self.db.execute('ROLLBACK')
        self.db.execute("INSERT INTO facts SELECT * FROM facts WHERE source_record_id='engine'")
        self.assertIn('DUPLICATE_FINANCIAL_SLICE',[r[0] for r in self.db.execute(self.selector.validation_sql()).fetchall()])

    def test_changed_scope_and_stale_binding_gate(self):
        self.db.execute("UPDATE facts SET scope_sha256=? WHERE source_record_id='engine'",['0'*64])
        self.assertIn('ORPHAN_OR_CHANGED_SCOPE',[r[0] for r in self.db.execute(self.selector.validation_sql()).fetchall()])
        self.db.execute('UPDATE memberships SET binding_sha256=?',['0'*64])
        self.assertIn('STALE_MEMBERSHIP_BINDING',[r[0] for r in self.db.execute(self.selector.validation_sql()).fetchall()])

    def test_input_snapshot_and_sql_injection_rejected(self):
        mutable=set(FACT_COLUMNS)
        selector=NativeMarketSelection(BINDING,'facts','memberships',mutable,MEMBERSHIP_COLUMNS)
        mutable.clear()
        self.assertEqual(selector.fact_columns,FACT_COLUMNS)
        with self.assertRaises(ValueError): self.selector.selected_sql(["F35');DROP TABLE facts--"],[2026],['PRIME_AWARD'])
        with self.assertRaises(ValueError): self.selector.selected_sql([F35],[2026],['SOMETHING_ELSE'])
        with self.assertRaises(ValueError): self.selector.selected_sql([F35],[2026],['PRIME_AWARD'],include_shared='false')

    def test_market_and_platform_namespaces_do_not_conflate(self):
        self.assertEqual(self.db.execute(self.selector.selected_sql(
            [F35],[2026],['PRIME_AWARD'])).fetchall(),[])
        chosen=self.db.execute(self.selector.selected_sql(
            [],[2026],['PRIME_AWARD'],platforms=[F35])).fetchall()
        self.assertEqual([r[1] for r in chosen],['engine'])

    def test_saved_f135_and_columbia_native_amounts_without_financial_rewrite(self):
        witness=json.loads(Path(__file__).with_name('testdata').joinpath('engine-membership-native-witness.json').read_text())
        self.db.execute('DELETE FROM memberships')
        self.db.execute('DELETE FROM facts')
        for row in witness['native_rows']:
            self.db.execute('INSERT INTO facts VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                [row['source_kind'],row['source_record_id'],int(row['fiscal_year']),row['native_record_id'],
                 row['allocation_id'],row['scope_sha256'],row['market_node'],Decimal(row['net_usd']),
                 row['description'],None,None,None])
        before=self.db.execute('SELECT * FROM facts ORDER BY native_record_id,allocation_id').fetchall()
        p13=next(r['source_record_id'] for r in witness['native_rows'] if 'P00013' in r['native_record_id'])
        columbia=next(r['source_record_id'] for r in witness['native_rows'] if 'COLUMBIA' in r['allocation_id'])
        # Test-only approvals exercise selection, not actual canonical registry adoption.
        self.associate(p13,FIGHTER)
        self.associate(p13,ENGINE)
        self.associate(columbia,'TEST_INDUSTRIAL',allocation='MARITIME_INDUSTRIAL_BASE')
        self.assertEqual(self.db.execute(self.selector.validation_sql()).fetchall(),[])
        selected=self.selector.selected_sql([ENGINE,FIGHTER],[2026],['PRIME_AWARD'])
        public=self.db.execute('SELECT sum(signed_usd) FROM ('+self.selector.fiscal_sql(
            [ENGINE,FIGHTER],[2026],['PRIME_AWARD'])+')').fetchone()[0]
        professional=self.db.execute('SELECT source_record_id,sum(net_usd) FROM ('+selected+') GROUP BY 1').fetchall()
        self.assertEqual(public,Decimal('3811102989.67'))
        self.assertEqual(professional,[(p13,public)])
        industrial=self.selector.selected_sql(['TEST_INDUSTRIAL'],[2026],['PRIME_AWARD'])
        self.assertEqual(self.db.execute('SELECT sum(net_usd) FROM ('+industrial+')').fetchone()[0],Decimal('74000000'))
        self.assertEqual(self.db.execute('SELECT * FROM facts ORDER BY native_record_id,allocation_id').fetchall(),before)

    def test_existing_dla_eligibility_and_first_tier_separation_are_preserved(self):
        self.add('shared-prime-purchase',40,ENGINE)
        self.add('matched-dla-mirror',40,ENGINE,source='DLA_AWARD')
        # The upstream keeper decision is an input view, not a new rule inferred here.
        self.db.execute("CREATE VIEW eligible AS SELECT * FROM facts WHERE source_record_id<>'matched-dla-mirror'")
        selector=NativeMarketSelection(BINDING,'eligible','memberships',FACT_COLUMNS,MEMBERSHIP_COLUMNS)
        self.assertEqual(self.db.execute(selector.validation_sql()).fetchall(),[])
        rows=self.db.execute(selector.fiscal_sql([ENGINE],[2026],['PRIME_AWARD','DLA_AWARD'])).fetchall()
        self.assertEqual([(r[0],r[-1]) for r in rows],[('PRIME_AWARD',Decimal('240.00'))])


if __name__=='__main__':
    unittest.main()
