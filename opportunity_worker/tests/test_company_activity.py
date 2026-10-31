import pytest
from opportunity_engine.company_activity import action_query

def test_annual_actions_resolve_revisions_before_window_and_keep_source_identity():
 q=action_query('2026-10-31','market_intel_platform_candidate.oi_source_identity')
 assert 'PARTITION BY contract_transaction_unique_key' in q
 assert q.index('row_number()')<q.index("date_add('year',-1")
 for field in ('transaction_key','award_key','action_obligation','source_url','evidence_id','awarding_agency_code'):assert field in q
 assert 'current_date' not in q
 with pytest.raises(ValueError):action_query('2026-10-31','untrusted')
 with pytest.raises(ValueError):action_query('bad','market_intel_platform_candidate.oi_source_identity')
