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

class UnitPriceApplicationTests(unittest.TestCase):
 def test_purchase_prices_cannot_value_repairs_or_exceed_an_explicit_cap(self):
  from opportunity_intelligence.notice_facts import price_scenario_fact
  import json
  row=dict(title='Generator',notice_quantity=3,notice_quantity_nsn='6115012704801',notice_quantity_unit='EA',notice_item_pricing_json=json.dumps([dict(nsn='6115012704801',catalogue_unit='EA',government_reference_unit_price=100)]))
  self.assertTrue(price_scenario_fact(row))
  self.assertEqual(price_scenario_fact(dict(row,title='Repair generator')), {})
  self.assertEqual(price_scenario_fact(dict(row,notice_aggregate_ceiling_usd=250)), {})

class AdditionalSourceValueTests(unittest.TestCase):
 def test_usage_and_planned_package_bands_remain_separate_from_published_and_source_forecasts(self):
  rows=[dict(episode_id='stock',commercial_value_kind='annual_usage_scenario',commercial_value='800',item_reference_niin='000000001'),dict(episode_id='plan',commercial_value_kind='planned_package_reference',commercial_value='250000000',value_low='250000000',value_high='1000000000')]
  r=summary({},rows,'release')
  self.assertIsNone(r['overall']['priced_demand_total']);self.assertIsNone(r['forecast']['estimated_demand_total'])
  self.assertEqual(r['replenishment']['conditional_annual_usage_value'],800)
  self.assertEqual(r['planned_packages']['planned_package_low'],250000000);self.assertEqual(r['planned_packages']['planned_package_high'],1000000000)
  rows.append(dict(episode_id='upper-only',commercial_value_kind='planned_package_reference',commercial_value='0',value_low=None,value_high='2000000'))
  r=summary({},rows,'release');self.assertIsNone(r['planned_packages']['planned_package_low']);self.assertEqual(r['planned_packages']['planned_package_high'],1002000000)

class ValuationCoverageTests(unittest.TestCase):
 def test_unknown_work_is_counted_separately_from_priced_requests_and_historical_guides(self):
  result=summary({'published_records_count':10,'published_priced_records_count':3,'published_historical_guide_records_count':5,'published_first_deadline':'2026-11-02','published_last_deadline':'2027-04-01'},[],'release')
  v=result['valuation_coverage']
  self.assertEqual(v['records_without_request_estimate'],7)
  self.assertEqual(v['records_without_any_value_guide'],2)
  self.assertEqual(v['historical_comparison_only_records'],5)
  self.assertIsNone(result['overall']['priced_demand_total'])
 def test_next_year_forecast_uses_month_values_not_full_horizon_or_annualization(self):
  row=dict(episode_id='one',commercial_value_kind='forecast_scenario',commercial_value='800',forecast_next_12m_value='200',valuation_status='catalogue_EA_assumed_forecast_unit_not_stated',item_reference_niin='001234567')
  result=summary({},[row],'release')
  self.assertEqual(result['forecast']['estimated_demand_total'],800)
  self.assertEqual(result['forecast']['next_12_month_estimated_demand'],200)
  row.pop('forecast_next_12m_value')
  self.assertIsNone(summary({},[row],'release')['forecast']['next_12_month_estimated_demand'])

class RecurringWorkSummaryTests(unittest.TestCase):
 def test_recurring_work_is_not_added_to_published_or_dla_totals(self):
  r=summary({'work_count':1},[dict(episode_id='work',commercial_value_kind='recurring_demand_scenario',commercial_value=18000000,title='Radar work',customer_name='Navy',value_inputs_json=json.dumps(dict(next_12_month_median=6000000,next_36_month_median=18000000)))],'release')
  self.assertEqual(r['work_forecast']['next_12_month_estimated_buying'],6000000)
  self.assertEqual(r['work_forecast']['next_36_month_estimated_buying'],18000000)
  self.assertIsNone(r['overall']['priced_demand_total'])
  self.assertIsNone(r['forecast']['estimated_demand_total'])
