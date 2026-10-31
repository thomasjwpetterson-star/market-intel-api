from opportunity_engine.reporting_lag import period

def test_mature_year_keeps_full_months_and_avoids_defense_delay():
 assert period('2026-10-31')==('2025-08-01','2026-07-31')
 assert period('2026-11-01')==('2025-08-01','2026-07-31')
 assert period('2025-06-01')==('2024-03-01','2025-02-28')
