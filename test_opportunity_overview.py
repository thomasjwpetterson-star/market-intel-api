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
 def test_company_total_combines_categories_without_double_counting_same_requirement(self):
  rows=[self.row('work',756715.32),{**self.row('item',700000),'item_demand':'true'},self.row('other',500000)]
  rows[2]['customer_name']='Air Force'
  result=summary({'work_count':'28','items_count':'474'},rows,'release')
  self.assertEqual(result['overall']['count'],502)
  self.assertEqual(result['overall']['priced_demand_total'],1256715.32)
  self.assertEqual(result['overall']['priced_requirement_count'],2)
  self.assertEqual(len(result['overall']['value_components'][0]['related_requirements']),2)

class ForecastOverviewTests(unittest.TestCase):
 def test_forecast_is_dated_and_separate_from_published_demand(self):
  row=dict(episode_id='forecast',record_hash='version',title='Matrix',customer_name='DLA',item_demand='true',item_reference_niin='012031861',commercial_value_kind='forecast_scenario',commercial_value='8043027.12',valuation_status='catalogue_EA_assumed_forecast_unit_not_stated',market_price_median='223417.42',window_start='2026-11-01',window_end='2029-10-01',annual_scenarios_json='[{"calendar_year":"2027","market_price_scenario":2681009.04,"government_reference_scenario":5749380}]')
  result=summary({'items_count':461},[row],'release')
  self.assertIsNone(result['overall']['priced_demand_total'])
  self.assertEqual(result['forecast']['estimated_demand_total'],8043027.12)
  self.assertEqual(result['forecast']['annual_values'][0]['estimated_demand'],2681009.04)
  row['valuation_status']='forecast_unit_conversion_unverified'
  self.assertIsNone(summary({},[row],'release')['forecast']['estimated_demand_total'])
 def test_separate_purchase_requests_for_same_item_are_not_collapsed(self):
  rows=[OverviewTests().row('one',100,value_inputs_json='{"purchase_request_number":"1"}'),OverviewTests().row('two',200,value_inputs_json='{"purchase_request_number":"2"}')]
  result=summary({},rows,'release')
  self.assertEqual(result['overall']['priced_demand_total'],300)
  self.assertEqual(result['overall']['priced_requirement_count'],2)


class CoverageTests(unittest.TestCase):
 def test_business_benchmark_exposes_gap_without_filling_it_with_reseller_sales(self):
  from opportunity_intelligence.overview import coverage_summary
  r=coverage_summary({'annual_net_obligations':'100000000','other_seller_item_sales':'500000000','interpretation':'held channel source methodology'},{'priced_demand_total':10000000},{'estimated_demand_total':60000000})
  self.assertEqual(r['coverage_benchmark'],300000000)
  self.assertEqual(r['coverage_status'],'below_benchmark_even_before_overlap')
  self.assertEqual(r['published_demand_to_annual_ratio'],0.1)
  self.assertEqual(r['full_horizon_forecast_to_annual_ratio'],0.6)
  self.assertNotIn('combined_pipeline',r)
  self.assertEqual(r['catalogue_sales_interpretation'],'held channel source methodology')
 def test_big_full_horizon_forecast_is_retained_but_not_a_qualified_pipeline(self):
  from opportunity_intelligence.overview import coverage_summary
  r=coverage_summary({'annual_net_obligations':1000000},{'priced_demand_total':2000000},{'estimated_demand_total':8000000})
  self.assertEqual(r['coverage_status'],'requires_overlap_and_access_review')
  self.assertEqual(r['full_horizon_forecast_to_annual_ratio'],8)

if __name__=='__main__':unittest.main()


class ReportingLagBenchmarkTests(unittest.TestCase):
 def test_defense_scale_uses_observed_lagged_year_without_extrapolating_recent_period(self):
  from opportunity_intelligence.overview import coverage_summary
  business=dict(annual_net_obligations=26000000,annual_dod_net_obligations=26000000,lag_adjusted_annual_net_obligations=35000000,lag_adjusted_annual_dod_net_obligations=35000000,lag_adjusted_period_start='2025-08-01',lag_adjusted_period_end='2026-07-31')
  r=coverage_summary(business,{},{});self.assertEqual(r['coverage_benchmark'],105000000);self.assertEqual(r['annual_net_obligations'],26000000);self.assertEqual(r['benchmark_period_end'],'2026-07-31');self.assertEqual(r['benchmark_basis'],'reporting_lag_adjusted_observed_year')
