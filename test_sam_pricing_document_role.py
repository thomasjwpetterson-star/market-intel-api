"""Document meaning must survive valid arithmetic, filenames and parser layouts."""
import ast
import importlib.util
import unittest
from pathlib import Path
from decimal import Decimal

ROOT=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('sam_attachment_line_items',ROOT/'pricing_pipeline/sam_attachment_line_items.py')
parser=importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name]=parser
spec.loader.exec_module(parser)
SHA='84075394258b3f88d553c39b5950b58e045113c016aaaf4f270b5005f268a967'

def glue_policy():
    tree=ast.parse((ROOT/'pricing_pipeline/glue_sam_governed_unit_pricing.py').read_text())
    node=next(n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name=='non_procurement_document_class')
    namespace={};exec(compile(ast.Module(body=[node],type_ignores=[]),'<policy>','exec'),namespace)
    return namespace[node.name],node

class DocumentRoleTests(unittest.TestCase):
    def candidates(self,name,sha,row):
        return parser.parse_line_candidates([parser.DocumentChunk('page=1',row)],notice_id='test',notice_version_id='version',solicitation_id='SAMPLE',award_number='TEST-AWARD',notice_type='Award Notice',attachment_id='a',resource_id='r',attachment_sha256=sha,attachment_s3_uri='s3://test/document',attachment_name=name)

    def test_known_inventory_is_excluded_after_renaming(self):
        fn,_=glue_policy()
        self.assertEqual(fn(SHA,'executed-award.pdf',''), 'GOVERNMENT_FURNISHED_PROPERTY_INVENTORY')

    def test_new_property_documents_are_held_without_claiming_exact_inventory_basis(self):
        fn,_=glue_policy()
        for name in ['GFP-list.pdf','Draft_Government_Furnished_Property_List.PDF','archive.zip/GFP schedule.xlsx']:
            with self.subTest(name=name):self.assertEqual(fn('new-source',name,''),'NON_PROCUREMENT_DOCUMENT_REQUIRES_REVIEW')

    def test_inventory_needs_document_and_stock_context(self):
        fn,_=glue_policy()
        self.assertIsNotNone(fn('new','Asset register.pdf','0001 TOOL 1 EA $5.00 $5.00 In Stock'))
        self.assertIsNone(fn('new','Award.pdf','0001 inventory management device 1 EA $5.00 $5.00'))
        self.assertIsNone(fn('new','Award.pdf','0001 labels for government furnished property 1 EA $5.00 $5.00'))

    def test_arithmetic_does_not_promote_inventory_or_create_clin(self):
        rows=self.candidates('renamed.pdf',SHA,'1757T19 SCALE REPAIRED 1 EA $48.00 $48.00 In Stock')
        self.assertEqual(len(rows),1);r=rows[0]
        self.assertIsNone(r.clin)
        self.assertEqual(r.source_unit_price_usd,Decimal('48.00'))
        self.assertEqual(r.extended_line_amount_usd,Decimal('48.00'))
        self.assertEqual(r.evidence_type,'NON_PROCUREMENT_DOCUMENT_REFERENCE')
        self.assertEqual(r.promotion_status,'EXCLUDED_NON_PROCUREMENT_DOCUMENT')
        self.assertIn('1757T19',r.source_row_text)
        self.assertEqual(r.document_role,'GOVERNMENT_FURNISHED_PROPERTY_INVENTORY')

    def test_executed_purchase_and_its_amount_are_retained(self):
        rows=self.candidates('Award.pdf','executed-source','0001 SEAT VALVE 2 EA $62,345.00 $124,690.00')
        self.assertEqual(len(rows),1);r=rows[0]
        self.assertEqual(r.clin,'0001');self.assertEqual(r.source_unit_price_usd*r.quantity,Decimal('124690.00'))
        self.assertEqual(r.promotion_status,'PENDING_NOTICE_AND_DOCUMENT_SCOPE_VALIDATION')

    def test_both_self_contained_entrypoints_use_identical_policy(self):
        _,node=glue_policy()
        tree=ast.parse((ROOT/'pricing_pipeline/sam_attachment_line_items.py').read_text())
        other=next(n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name==node.name)
        self.assertEqual(ast.dump(node,include_attributes=False),ast.dump(other,include_attributes=False))

    def test_document_classification_precedes_default_equipment_admission(self):
        # Guard the real Spark pipeline's control flow without requiring Glue locally.
        code=(ROOT/'pricing_pipeline/glue_sam_governed_unit_pricing.py').read_text()
        self.assertLess(code.index('F.col("excluded_document_class").isNotNull()'),code.index('.otherwise(F.lit("PROCUREMENT_EQUIPMENT_LINE"))'))
        self.assertIn('EXCLUDED_NON_PROCUREMENT_DOCUMENT',code)
        self.assertIn('retained_source_observation_count',code)

if __name__=='__main__':unittest.main()
