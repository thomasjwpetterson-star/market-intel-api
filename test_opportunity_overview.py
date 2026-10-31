import unittest
from opportunity_intelligence.overview import summary

class OverviewTests(unittest.TestCase):
 def row(self,id,value=100,**extra):
  return dict(episode_id=id,record_hash=id+'hash',title='Generator',customer_name='Navy',item_demand='false',commercial_value_kind='item_reference_estimate',commercial_value=str(value),notice_item_pricing_json='[{"nsn":"6115012704801"}]',**extra)
 def test_counts_are_full_feed_counts_and_historical_forecasts_are_excluded(self):
  rows=[self.row('one'),self.row('benchmark',9000000),self.row('forecast',3000000)]
  rows[1]['commercial_value_kind']='historical_comparison';rows[2]['commercial_value_kind']='forecast_scenario'
  result=summary({'work_count':'203','items_count':'474','observed_prime_net_obligations':'281000000'},rows,'release')
  self.assertEqual(result['categories']['work']['count'],203)
  self.assertEqual(result['categories']['work']['priced_demand_total'],100)
  self.assertEqual(result['categories']['items']['priced_demand_total'],None)
  self.assertEqual(result['materiality_floor'],1000000)
 def test_same_buyer_item_uses_largest_and_retains_all_related_evidence(self):
  rows=[self.row('one',756715.32),self.row('two',700000)]
  result=summary({'work_count':'2'},rows,'release')['categories']['work']
  self.assertEqual(result['priced_demand_total'],756715.32)
  self.assertEqual(result['priced_source_count'],2);self.assertEqual(result['priced_requirement_count'],1)
  self.assertEqual(len(result['value_components'][0]['related_requirements']),2)
 def test_different_buyers_are_separate_and_unknown_values_are_not_zero(self):
  rows=[self.row('one',0.1),{**self.row('two',0.2),'customer_name':'Air Force'},self.row('bad','NaN')]
  result=summary({'work_count':'3'},rows,'release')['categories']['work']
  self.assertEqual(result['priced_demand_total'],0.3)
  self.assertEqual(result['priced_requirement_count'],2)
  self.assertIsNone(summary({'work_count':'3'},[],'release')['categories']['work']['priced_demand_total'])

if __name__=='__main__':unittest.main()
