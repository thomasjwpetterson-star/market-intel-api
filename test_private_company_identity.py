import json
import os
import unittest
from unittest.mock import patch
from opportunity_intelligence.api import Repository
from opportunity_intelligence.identity import present_identity


class Identity(unittest.TestCase):
    def row(self,**overrides):
        return dict(cage_code='11111',company_name='SITE',parent_cage='22222',resolved_registry_parent_cage='33333',resolved_parent_name='GROUP',resolution_status='held_registered_parent_chain',chain_path_json=json.dumps(['11111','22222','33333']),**overrides)
    def test_existing_recursive_chain_is_used_without_inheriting_capabilities(self):
        result=present_identity('11111',[self.row()],'2026-11-01')
        self.assertEqual(result['parent_relationship']['resolved_parent_cage'],'33333')
        self.assertEqual(result['parent_relationship']['chain'],['11111','22222','33333'])
        self.assertNotIn('capabilities',result)
        self.assertNotIn('revenue',result)
    def test_cycles_depth_holds_and_invalid_paths_never_promote_a_parent(self):
        for changes in [dict(resolution_status='cycle_requires_review'),dict(resolution_status='depth_limit_requires_review'),dict(chain_path_json='bad'),dict(chain_path_json='["11111","33333","33333"]'),dict(chain_path_json='["99998","33333"]'),dict(resolved_registry_parent_cage='99999')]:
            row=self.row();row.update(changes)
            result=present_identity('11111',[row],None)['parent_relationship']
            self.assertIsNone(result['resolved_parent_cage']);self.assertIsNone(result['resolved_parent_name'])
    def test_unheld_root_and_missing_chain_do_not_mean_verified_ownership_or_independence(self):
        row=self.row();row['resolution_status']='chain_ends_at_unheld_reference'
        out=present_identity('11111',[row],None)['parent_relationship']
        self.assertEqual(out['resolved_parent_cage'],'33333');self.assertIsNone(out['resolved_parent_name'])
        row=self.row();row.update(resolution_status=None,chain_path_json=None,resolved_registry_parent_cage=None)
        self.assertEqual(present_identity('11111',[row],None)['parent_relationship']['status'],'no_parent_chain_recorded')
        self.assertEqual(present_identity('11111',[],None)['status'],'not_in_registry')
    def test_publication_date_does_not_make_a_historical_parent_record_current(self):
        row=self.row();row['dla_last_update']='21-MAR-2017'
        parent=present_identity('11111',[row],'2026-11-01')['parent_relationship']
        self.assertEqual(parent['freshness'],'historical_reference');self.assertFalse(parent['current_ownership_verified'])
        row['dla_last_update']='2026-10-01'
        self.assertEqual(present_identity('11111',[row],'2026-11-01')['parent_relationship']['freshness'],'recent_registry_reference')
    def test_reviewed_ownership_preserves_the_registry_and_exact_cage_scope(self):
        rocketdyne = present_identity(
            "18ZX6", [{"cage_code": "18ZX6", "company_name": "ROCKETFORCE TECHNOLOGIES LLC"}], "2026-11-11"
        )
        self.assertEqual(rocketdyne["reviewed_ownership"]["owner_name"], "AE INDUSTRIAL PARTNERS")
        self.assertEqual(rocketdyne["reviewed_ownership"]["minority_ownership_percent"], 40)
        self.assertEqual(rocketdyne["parent_relationship"]["status"], "no_parent_chain_recorded")
        self.assertNotIn("reviewed_ownership", present_identity("02602", [], "2026-11-11"))

    def test_repository_reads_exact_company_and_existing_resolver_only(self):
        repo=Repository.__new__(Repository);seen=[]
        repo.manifest=lambda:{'as_of':'2026-11-01','tables':{'company_registry':{},'company_parent_resolution':{}}}
        repo.table=lambda name:'safe.'+name
        repo.rows=lambda sql:(seen.append(sql) or [self.row()])
        self.assertEqual(repo.company_identity('11111')['parent_relationship']['resolved_parent_cage'],'33333')
        self.assertIn("WHERE r.cage_code='11111'",seen[0]);self.assertIn('safe.company_parent_resolution',seen[0]);self.assertNotIn('LIKE',seen[0])
        with self.assertRaises(ValueError):repo.company_identity("11111'")
        with self.assertRaises(ValueError):present_identity('11111',[{'cage_code':'OTHER'}],None)
    def test_private_route_requires_proxy_and_never_exposes_provider_errors(self):
        import ast,re,hmac
        from pathlib import Path
        from unittest.mock import Mock
        from fastapi import FastAPI,HTTPException,Request,Response
        from fastapi.testclient import TestClient
        node=next(n for n in ast.parse(Path('main.py').read_text()).body if isinstance(n,ast.FunctionDef)and n.name=='private_company_identity');node.decorator_list=[]
        ns=dict(os=os,re=re,hmac=hmac,Request=Request,Response=Response,HTTPException=HTTPException,logger=Mock())
        exec(compile(ast.Module(body=[node],type_ignores=[]),'main.py','exec'),ns)
        app=FastAPI();app.get('/identity')(ns['private_company_identity']);client=TestClient(app)
        with patch.dict(os.environ,{'MIMIR_EXPORT_PROXY_SECRET':'fixture'}),patch('opportunity_intelligence.identity.lookup_identity',return_value={'cage':'11111','status':'not_in_registry'})as lookup:
            self.assertEqual(client.get('/identity?cage=11111').status_code,401);lookup.assert_not_called()
            out=client.get('/identity?cage=11111',headers={'X-Mimir-Export-Key':'fixture'})
            self.assertEqual(out.status_code,200);self.assertIn('no-store',out.headers['cache-control'])
            lookup.side_effect=RuntimeError('private-provider-details')
            out=client.get('/identity?cage=11111',headers={'X-Mimir-Export-Key':'fixture'})
            self.assertEqual(out.status_code,503);self.assertNotIn('private-provider-details',out.text)
