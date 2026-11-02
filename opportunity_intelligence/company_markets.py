"""Economically ranked held DoD activity by the award's reported PSC/FSC."""
import re

def query(repo,cage,level='psc',limit=8):
 if (cage is not None and not re.fullmatch('[A-Z0-9]{5}',cage)) or level not in ('psc','family') or not 1<=limit<=25:raise ValueError('Invalid company market request')
 actions=repo.table('company_reporting_lag_actions');awards=repo.table('company_award_facts');period=repo.table('company_reporting_lag_baselines');reference=repo.table('work_vocabulary_reference')
 code="nullif(trim(a.psc),'')" if level=='psc' else "substr(nullif(trim(a.psc),''),1,2)"
 sql="""WITH activity AS (SELECT x.*,CODE market_code FROM ACTIONS x JOIN AWARDS a ON a.award_key=x.award_key WHERE x.company_id='CAGE:CAGEVALUE' AND x.awarding_agency_code='097'),
 markets AS (SELECT company_id,market_code,sum(coalesce(action_obligation,0)) net_obligations,sum(greatest(coalesce(action_obligation,0),0)) positive_obligations,count(*) action_count,count(DISTINCT award_key) award_count,max(action_date) latest_action_date,max_by(source_url,action_date) example_award_url FROM activity GROUP BY company_id,market_code),
 labels AS (SELECT psc,max(label) label,max(source_url) classification_source_url FROM REFERENCE GROUP BY psc)
 SELECT m.*,coalesce(l.label,'Unclassified activity') market_label,l.classification_source_url,p.lag_adjusted_period_start period_start,p.lag_adjusted_period_end period_end,'Reported PSC on the linked award; obligations are held signed actions in the reporting-lag-adjusted year, not future opportunities or company revenue.' value_basis
 FROM markets m LEFT JOIN labels l ON l.psc=m.market_code LEFT JOIN PERIOD p ON p.company_id=m.company_id
 ORDER BY net_obligations DESC,market_code NULLS LAST LIMIT LIMITVALUE""".replace('CODE',code).replace('ACTIONS',actions).replace('AWARDS',awards).replace('REFERENCE',reference).replace('PERIOD',period).replace('CAGEVALUE',cage or '00000').replace('LIMITVALUE',str(limit))
 if cage is not None:return sql
 sql=sql.replace("x.company_id='CAGE:00000' AND ",'').replace(' ORDER BY net_obligations DESC,market_code NULLS LAST LIMIT '+str(limit),'')
 return 'SELECT * FROM (SELECT market_rows.*,row_number() OVER(PARTITION BY company_id ORDER BY net_obligations DESC,market_code NULLS LAST) market_rank FROM ('+sql+') market_rows) WHERE market_rank<='+str(limit)
