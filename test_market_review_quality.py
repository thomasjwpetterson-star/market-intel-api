import unittest
from source_automation.market_review_quality import discover_candidates,apply_reviewed_controls,validate_review_closure

def row(key,amount='10.00',**kw):
    return dict(transaction_key=key,award_key='agency-order-parent',market_id='test',main_release_id='pinned',fiscal_year=2026,source_amount_usd=amount,**kw)
def control(key,amount='10.00',decision='INCLUDE'):
    return dict(transaction_key=key,source_amount_usd=amount,fiscal_year=2026,decision=decision,decision_basis='reviewed')

class MarketReviewTests(unittest.TestCase):
    def test_all_fields_and_concatenated_aliases_keep_unmapped_signed_actions(self):
        records=[row('a','-2.50',action_description='Funding adjustment',base_award_description='FSSUADS',platform_family=None),row('b','0.00',action_description='RDSUADS')]
        out=discover_candidates(records,['SUADS'],[2026]);self.assertEqual(len(out),2);self.assertEqual(out[0]['record']['source_amount_usd'],'-2.50');self.assertTrue(all(r['review_status']=='PENDING_REVIEW'for r in out))
    def test_equal_dollar_same_day_distinct_natives_survive(self):
        self.assertEqual(len(discover_candidates([row('a',description='SUADS'),row('b',description='SUADS')],['SUADS'],[2026])),2)
        with self.assertRaises(ValueError):discover_candidates([row('a'),row('a')],['SUADS'],[2026])
    def test_recovery_and_explicit_hold_conserve_signed_cents(self):
        src=[row('a','-2.50')];sc=[]
        a,b=apply_reviewed_controls(src,sc,[control('a','-2.50'),control('b','10.00','HOLD')],[row('b')])
        self.assertEqual(len(a),2);self.assertEqual(b[0]['reviewed_market_amount_usd'],'-2.50');self.assertFalse(b[1]['approved_market_fact']);self.assertEqual(b[1]['scope_unallocated_amount_usd'],'10.00')
    def test_offsetting_errors_cannot_pass_net_only(self):
        a,b=apply_reviewed_controls([row('a'),row('b')],[],[control('a'),control('b')],[])
        b[0]['reviewed_market_amount_usd']='11.00';b[1]['reviewed_market_amount_usd']='9.00'
        with self.assertRaises(ValueError):validate_review_closure(a,b,[control('a'),control('b')])
    def test_hold_never_overwrites_approved_mapping(self):
        a,b=apply_reviewed_controls([row('a')],[],[control('a')],[])
        with self.assertRaises(ValueError):apply_reviewed_controls(a,b,[control('a',decision='HOLD')],[])
    def test_missing_record_and_source_revision_fail(self):
        with self.assertRaises(ValueError):apply_reviewed_controls([],[],[control('missing')],[])
        with self.assertRaises(ValueError):apply_reviewed_controls([row('a','9.00')],[],[control('a')],[])

if __name__=='__main__':unittest.main()
