import hashlib,io,json,unittest
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from private_platform_reference import Reference,read_pin,BUCKET,PREFIX

class PrivateV7Reference(unittest.TestCase):
    def setUp(self):
        self.objects={}
        def pin(name,data):
            raw=json.dumps(data).encode()if name.endswith('.json')else self.parquet(data)
            key=PREFIX+'fixture/'+name;self.objects[key]=raw
            return {'bucket':BUCKET,'key':key,'s3_version_id':'fixture-version','sha256':hashlib.sha256(raw).hexdigest()}
        registry=pin('registry.parquet',[{'canonical_entity_id':'AMRAAM','canonical_label':'AMRAAM'},{'canonical_entity_id':'OTHER','canonical_label':'OTHER'}])
        rows=[{'award_key':'qualified-navy-adapter','canonical_entity_id':'AMRAAM','target_entity_ids':'AMRAAM','association_scope':'PROGRAM_SUPPORT','mapping_decision_active':True,'financial_allocation_eligible':False,'pricing_evidence_eligible':False},
              {'award_key':'qualified-shared','canonical_entity_id':None,'target_entity_ids':'AMRAAM | OTHER','association_scope':'PROGRAM_SUPPORT','mapping_decision_active':True,'financial_allocation_eligible':False,'pricing_evidence_eligible':False},
              {'award_key':'reviewed-cleared','canonical_entity_id':None,'target_entity_ids':'','association_scope':'NON_PLATFORM_WORK','mapping_decision_active':True,'financial_allocation_eligible':False,'pricing_evidence_eligible':False}]
        authority=pin('authority.parquet',rows)
        metadata=pin('metadata.parquet',[{'award_key':'qualified-navy-adapter','contract_id':'NAVYADAPTER'},{'award_key':'qualified-shared','contract_id':'SHARED'},{'award_key':'reviewed-cleared','contract_id':'CLEARED'}])
        items=pin('items.parquet',[{'award_key':'qualified-navy-adapter','canonical_entity_id':'AMRAAM','nsn':'5999014812863','niin':'014812863','item_name':'ADAPTER,TEST,AMRAAM','financial_allocation_eligible':False,'pricing_evidence_eligible':False}])
        manifest={'schema_version':'platform-award-remap-cloud-candidate-v1','release_id':'private-v7-successor','accepted_main_release_id':'main-fixture',
                  'base_platform_reference_release_id':'v7','customer_publication_eligible':False,
                  'tables':{'canonical_registry':registry,'award_mapping_authority':authority,'award_item_associations':items},
                  'files':{'data/award_resolution_decisions.parquet':metadata}}
        manifest_pin=pin('manifest.json',manifest)
        self.binding={'manifest':manifest_pin,'accepted_main_release_id':'main-fixture','base_platform_reference_release_id':'v7'}
        outer=self
        class Client:
            def get_object(self,**args):return {'Body':io.BytesIO(outer.objects[args['Key']]),'VersionId':'fixture-version'}
        self.client=Client();self.reference=Reference(self.client,self.binding)
        self.db=duckdb.connect();self.addCleanup(self.db.close)
        self.db.execute('CREATE TABLE v_transactions(award_key VARCHAR,year INTEGER,sub_agency VARCHAR,parent_agency VARCHAR,spend_amount DOUBLE,niin VARCHAR,nsn VARCHAR)')
        self.db.executemany('INSERT INTO v_transactions VALUES (?,?,?,?,?,?,?)',[
            ('qualified-navy-adapter',2025,'DEPARTMENT OF THE NAVY','DEPARTMENT OF DEFENSE',1000,None,None),
            ('qualified-shared',2025,'DEPARTMENT OF THE NAVY','DEPARTMENT OF DEFENSE',3000,None,None),
            ('qualified-navy-adapter',2024,'DEPARTMENT OF THE NAVY','DEPARTMENT OF DEFENSE',5,None,None),
            ('same-contract-different-award',2025,'DEPARTMENT OF THE AIR FORCE','DEPARTMENT OF DEFENSE',90000,None,None),
            ('reviewed-cleared',2025,'DEPARTMENT OF THE AIR FORCE','DEPARTMENT OF DEFENSE',200,None,None)])
        self.columns={'award_key','year','sub_agency','parent_agency','spend_amount','niin','nsn'}
    def test_reviewed_items_respect_market_domain_filter(self):
        self.db.execute("ALTER TABLE v_transactions ADD COLUMN market_segment VARCHAR")
        self.db.execute("UPDATE v_transactions SET market_segment='AIR'")
        out=self.reference.coverage({'program':'AMRAAM','years':[2025],'domain':'NAVAL'},self.read,self.columns|{'market_segment'})
        self.assertEqual(out['reviewed_award_count'],0);self.assertEqual(out['reviewed_items'],[])

    def test_effective_scope_excludes_reviewed_clear_and_keeps_unreviewed_primary(self):
        self.db.execute("ALTER TABLE v_transactions ADD COLUMN contract_id VARCHAR")
        self.db.execute("ALTER TABLE v_transactions ADD COLUMN platform_family VARCHAR")
        self.db.execute("ALTER TABLE v_transactions ADD COLUMN vendor_cage VARCHAR")
        self.db.execute("UPDATE v_transactions SET contract_id=award_key,platform_family='AMRAAM',vendor_cage='12345'")
        columns=self.columns|{'contract_id','platform_family','vendor_cage'}
        out=self.reference.coverage({'program':'AMRAAM','years':[2025]},self.read,columns)
        scope=out['effective_programme_scope']
        self.assertEqual(scope['action_count'],3)
        self.assertEqual(scope['contract_count'],3)
        self.assertEqual(scope['source_net_obligations_usd'],94000)
        self.assertFalse(scope['financial_allocation_eligible'])
        from private_platform_reference import summary_coverage
        summary=summary_coverage({'program':'AMRAAM','years':[2025]},out)
        self.assertEqual(summary['selected_scope']['programme_action_count'],3)
        self.assertEqual(summary['reason'],'programme_actions_have_no_item_numbers')
        self.assertFalse(summary['description_candidates_evaluated'])

    @staticmethod
    def parquet(rows):
        stream=io.BytesIO();pq.write_table(pa.Table.from_pylist(rows),stream);return stream.getvalue()
    def read(self,sql,params):
        cursor=self.db.execute(sql,params);return [dict(zip([c[0]for c in cursor.description],row))for row in cursor.fetchall()]
    def test_exact_native_award_keys_buyer_and_year_define_scope(self):
        out=self.reference.coverage({'program':'AMRAAM','agency':'Navy','years':[2025]},self.read,self.columns)
        self.assertEqual(out['reviewed_award_count'],2);self.assertEqual(out['reviewed_action_count'],2)
        self.assertEqual(sum(r['source_net_obligations_usd']for r in out['reviewed_awards']),4000)
        self.assertEqual(out['reviewed_items'][0]['evidence_count'],1)
        self.assertTrue(all(r['actions_with_native_item_number']==0for r in out['reviewed_awards']))
        self.assertFalse(out['public_publication_eligible'])
    def test_shared_source_award_is_counted_once_and_not_financially_allocated(self):
        out=self.reference.coverage({'program':'AMRAAM','years':[2025]},self.read,self.columns)
        self.assertEqual(len({r['award_key']for r in out['reviewed_awards']}),2)
        shared=next(r for r in out['reviewed_awards']if r['contract_id']=='SHARED')
        self.assertEqual(shared['canonical_entity_ids'],['AMRAAM','OTHER']);self.assertFalse(shared['financial_allocation_eligible'])
    def test_reviewed_clear_never_becomes_a_positive_association(self):
        out=self.reference.coverage({'program':'AMRAAM','agency':'Air Force','years':[2025]},self.read,self.columns)
        self.assertEqual(out['reviewed_action_count'],0);self.assertEqual(out['reviewed_items'],[])
    def test_no_fuzzy_variant_or_family_identity_inference(self):
        out=self.reference.coverage({'program':'AMRAAM NAVY'},self.read,self.columns)
        self.assertEqual(out['status'],'NO_EXACT_CANONICAL_IDENTITY');self.assertFalse(out['alias_inference_applied'])
    def test_bad_hash_and_nonprivate_locations_fail(self):
        p=dict(self.binding['manifest']);p['sha256']='0'*64
        with self.assertRaises(ValueError):read_pin(self.client,p)
        p['key']='app_cache/public.parquet'
        with self.assertRaises(ValueError):read_pin(self.client,p)
    def test_missing_agency_qualified_keys_does_not_fall_back_to_contract_text(self):
        with self.assertRaises(ValueError):self.reference.coverage({'program':'AMRAAM'},self.read,self.columns-{'award_key'})

if __name__=='__main__':unittest.main()
