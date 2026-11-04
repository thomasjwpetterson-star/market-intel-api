import unittest
from programme_context import context_for,aliases_for,REVIEWED_CONTEXTS

class ProcurementContext(unittest.TestCase):
    def test_context_does_not_add_amraam_aliases_to_other_programmes(self):
        for n in ['F-35','M1 Abrams','Bradley','PAC-3 MSE','JASSM']:
            self.assertIsNotNone(context_for(n))
            self.assertFalse(any('AIM-120'in a or 'AMRAAM'in a for a in aliases_for(n)),n)
        self.assertIn('AIM-120',aliases_for('AMRAAM'))
    def test_sources_facts_and_interpretations_remain_separate(self):
        for row in REVIEWED_CONTEXTS:
            self.assertTrue(row['facts']);self.assertTrue(row['analytical_implications'])
            self.assertTrue(all(s['url'].startswith('https://')and s['checked_on']=='2026-11-04'for s in row['sources']))
            self.assertNotIn('funding_split',row);self.assertNotIn('award_value',row)
        self.assertIsNone(context_for('UNREVIEWED UNKNOWN'))
    def test_context_is_not_a_mutable_mapping_or_automatic_family_rollup(self):
        first=context_for('F-35');first['facts'].clear()
        self.assertTrue(context_for('F-35')['facts'])
        self.assertNotEqual(context_for('PAC-3')['programme'],context_for('PAC-3 MSE')['programme'])

if __name__=='__main__':unittest.main()
