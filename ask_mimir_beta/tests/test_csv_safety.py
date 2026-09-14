import csv
import io
import unittest

from csv_safety import spreadsheet_cell
from platform_supply_chain_export import _csv_bytes


class SpreadsheetSafetyTests(unittest.TestCase):
    def test_formula_like_text_is_escaped_but_numeric_values_are_unchanged(self):
        for text in ['=1+1', '+SUM(A1:A2)', '-SUM(A1:A2)', '@SUM(A1:A2)', '\t=1+1']:
            self.assertEqual(spreadsheet_cell(text), "'"+text)
        self.assertEqual(spreadsheet_cell(-1250.25),-1250.25)
        self.assertEqual(spreadsheet_cell('012345678'),'012345678')

    def test_real_export_protects_source_text_and_preserves_financial_sign(self):
        payload=_csv_bytes([{'supplier':'=1+1','value':-1250.25}],['supplier','value'])
        row=next(csv.DictReader(io.StringIO(payload.decode('utf-8-sig'))))
        self.assertEqual(row,{'supplier':"'=1+1",'value':'-1250.25'})
