"""Regression controls for valid numbers with the wrong economic meaning."""
import ast
import importlib.util
import io
import re
import sys
import unittest
from decimal import Decimal
from pathlib import Path

from pricing_pipeline import sam_attachment_line_items as parser
from opportunity_worker.opportunity_engine import price_evidence as policy

ROOT = Path(__file__).resolve().parent
ESTIMATE = "aa8abebbd6e303032eb32a1a63568275147577139783e2b2558cb212c7454169"
EQUIPMENT = "6f2e0fc4bb73993ba7c0f0cd226a38bd85bc9dca33a67c55efe27de1999b14bf"


class SourceValueBasisTests(unittest.TestCase):
    def parse(self, sha, row, name="renamed.docx"):
        return parser.parse_line_candidates(
            [parser.DocumentChunk("document", row)], notice_id="notice",
            notice_version_id="exact-version", solicitation_id="SAMPLE",
            award_number=None, notice_type="Award Notice", attachment_id="a",
            resource_id="r", attachment_sha256=sha,
            attachment_s3_uri="s3://source/renamed.docx", attachment_name=name,
        )

    def test_planning_estimate_retains_real_clin_amount_and_identity(self):
        row = "0016 THIGH SUPPORT FIERCELY 1 EA $1,614.62 $1,614.62"
        actual = self.parse(ESTIMATE, row)[0]
        self.assertEqual(actual.clin, "0016")
        self.assertEqual(actual.quantity, Decimal("1"))
        self.assertEqual(actual.source_unit_price_usd, Decimal("1614.62"))
        self.assertEqual(actual.extended_line_amount_usd, Decimal("1614.62"))
        self.assertEqual(actual.source_value_basis, "SOURCE_PLANNING_ESTIMATE_UNIT_PRICE")
        self.assertEqual(actual.promotion_status, "REFERENCE_ONLY_SOURCE_VALUE")
        self.assertEqual(actual.line_candidate_id, self.parse(ESTIMATE, row, "another-name.pdf")[0].line_candidate_id)

    def test_equipment_list_value_is_unestablished_not_asserted_inventory(self):
        classification = policy.non_procurement_document_class(EQUIPMENT, "executed.pdf", "")
        self.assertEqual(classification, "VALUE_BASIS_UNESTABLISHED_REFERENCE")
        self.assertEqual(policy.document_value_basis(classification), "SOURCE_VALUE_BASIS_UNESTABLISHED")
        self.assertFalse(policy.retain_document_clin(classification))

    def test_new_presolicitation_retains_clin_without_claiming_certified_estimate(self):
        actual = self.parse("unreviewed", "0016 DEVICE 1 EA $4.00 $4.00", "Presolicitation.docx")[0]
        self.assertEqual(actual.clin, "0016")
        self.assertEqual(actual.document_role, "PREAWARD_DOCUMENT_VALUE_REQUIRES_REVIEW")
        self.assertEqual(actual.source_value_basis, "SOURCE_VALUE_BASIS_UNESTABLISHED")

    def test_item_text_alone_does_not_turn_executed_line_into_planning_document(self):
        self.assertIsNone(policy.non_procurement_document_class("executed", "Award.pdf", "0001 planning estimate display device 2 EA $4.00 $8.00"))

    def test_all_policy_helpers_are_identical_in_self_contained_jobs(self):
        names = {"non_procurement_document_class", "document_value_basis", "retain_document_clin", "document_quality_status"}
        def helpers(path):
            return {n.name: ast.dump(n, include_attributes=False) for n in ast.parse(path.read_text()).body if isinstance(n, ast.FunctionDef) and n.name in names}
        expected = helpers(ROOT / "opportunity_worker/opportunity_engine/price_evidence.py")
        for path in [ROOT / "pricing_pipeline/sam_attachment_line_items.py", ROOT / "pricing_pipeline/glue_sam_governed_unit_pricing.py"]:
            self.assertEqual(helpers(path), expected)

    def test_legacy_frozen_predicate_blocks_reviewed_estimate_and_equipment(self):
        import duckdb
        c = duckdb.connect()
        try:
            c.execute("CREATE TABLE lines(include_in_mimir_range BOOLEAN,promotion_eligible_without_review BOOLEAN,quality_status VARCHAR,attachment_sha256 VARCHAR,attachment_s3_uri VARCHAR,attachment_source_row_text VARCHAR,unit_price_usd DOUBLE,quantity DOUBLE)")
            c.executemany("INSERT INTO lines VALUES (true,true,'AUTO',?,'renamed.pdf','0001 DEVICE',10,1)", [(ESTIMATE,), (EQUIPMENT,), ("executed",)])
            self.assertEqual(c.execute("SELECT attachment_sha256 FROM lines WHERE " + policy.procurement_price_sql(dialect="duckdb")).fetchall(), [("executed",)])
        finally:
            c.close()

    def transform_constant(self, name):
        tree = ast.parse((ROOT / "pricing_pipeline/glue_sam_award_pricing_evidence.py").read_text())
        return ast.literal_eval(next(n.value for n in tree.body if isinstance(n, ast.Assign)
                                     and any(isinstance(t, ast.Name) and t.id == name for t in n.targets)))

    def test_physical_support_is_not_a_support_service(self):
        pattern = self.transform_constant("ANCILLARY_QUANTITY_PATTERN")
        for description in ["THIGH SUPPORT FIERCELY", "SUPPORT, STRUCTURAL", "AIRCRAFT SUPPORT EQUIPMENT", "BEARING SUPPORT"]:
            self.assertIsNone(re.search(pattern, description), description)
        for description in ["TECHNICAL SUPPORT", "LOGISTICS SUPPORT", "SUPPORT SERVICES", "MAINTENANCE", "WARRANTY"]:
            self.assertIsNotNone(re.search(pattern, description), description)

    def test_new_parser_semantic_versions_rank_numerically(self):
        pattern = self.transform_constant("PARSER_VERSION_RANK_PATTERN")
        versions = ["mimir-sam-attachment-clin-2026-10-v7", "mimir-sam-attachment-clin-2026-11-v8-document-role",
                    "mimir-sam-attachment-clin-2026-11-v9-source-value-basis", "mimir-sam-attachment-clin-2026-11-v10"]
        self.assertEqual([int(re.search(pattern, v).group(1)) for v in versions], [7, 8, 9, 10])

    def test_governed_validation_retains_reference_clin_but_rejects_promotion(self):
        import duckdb
        tree = ast.parse((ROOT / "pricing_pipeline/glue_sam_governed_unit_pricing.py").read_text())
        node = next(n for n in tree.body if isinstance(n, ast.Assign) and
                    any(isinstance(t, ast.Name) and t.id == "unsafe_promotion_count" for t in n.targets))
        predicate = ast.literal_eval(node.value.func.value.args[0])
        c = duckdb.connect()
        try:
            c.execute("CREATE TABLE records(id VARCHAR,item_class VARCHAR,include_in_mimir_range BOOLEAN,promotion_eligible_without_review BOOLEAN,unit_price_usd DOUBLE,clin VARCHAR,quality_status VARCHAR)")
            c.executemany("INSERT INTO records VALUES (?,?,?,?,?,?,?)", [
                ("reference", "PREAWARD_PLANNING_ESTIMATE_REFERENCE", False, False, 1614.62, "0016", "REFERENCE_ONLY_SOURCE_VALUE"),
                ("bad-promotion", "PREAWARD_PLANNING_ESTIMATE_REFERENCE", True, True, 1614.62, "0016", "REFERENCE_ONLY_SOURCE_VALUE"),
                ("bad-clin", "GOVERNMENT_FURNISHED_PROPERTY_INVENTORY", False, False, 48, "1757T19", "EXCLUDED_NON_PROCUREMENT_DOCUMENT"),
                ("executed", "PROCUREMENT_EQUIPMENT_LINE", True, True, 62345, "0001", "AUTO_ADMITTED_BY_COHORT_RULE"),
            ])
            self.assertEqual(c.execute("SELECT id FROM records WHERE " + predicate + " ORDER BY id").fetchall(), [("bad-clin",), ("bad-promotion",)])
        finally:
            c.close()


if __name__ == "__main__":
    unittest.main()
