"""Release invariants for vocabulary coverage, source binding and fit hygiene."""
def validate(c,m,check):
 if not m.get('work_vocabulary_policy'):return
 check('vocabulary_official_psc_unique',c.execute('SELECT count(*)-count(DISTINCT psc) FROM work_vocabulary_reference').fetchone()[0]==0)
 check('vocabulary_official_psc_has_provenance',c.execute("SELECT count(*) FROM work_vocabulary_reference WHERE length(source_sha256)<>64 OR source_url NOT LIKE 'https://www.acquisition.gov/%'").fetchone()[0]==0)
 check('vocabulary_scope_unique',c.execute('SELECT count(*)-count(DISTINCT scope_id) FROM work_vocabulary_scopes').fetchone()[0]==0)
 check('vocabulary_capability_universe_complete',c.execute('SELECT count(*) FROM company_capability_vocabulary').fetchone()[0]==m['tables']['company_capability_history']['rows'])
 check('vocabulary_opportunity_universe_complete',c.execute('SELECT count(*) FROM opportunity_vocabulary').fetchone()[0]==m['tables']['opportunities']['rows'])
 check('vocabulary_opportunity_versions_exact',c.execute('SELECT count(*) FROM opportunity_vocabulary v LEFT JOIN opportunities o USING(episode_id) WHERE o.record_hash IS NULL OR v.record_hash<>o.record_hash').fetchone()[0]==0)
 check('vocabulary_classifications_never_qualification',c.execute("SELECT count(*) FROM work_vocabulary_scopes WHERE json_extract_string(annotation_json,'$.qualification')<>'not_established' OR json_extract_string(annotation_json,'$.technical_equivalence')<>'not_established'").fetchone()[0]==0)
 check('vocabulary_topic_passages_retained',c.execute("SELECT count(*) FROM work_vocabulary_scopes s,unnest(from_json(s.annotation_json->'topics','[{\"matched_phrase\":\"VARCHAR\",\"passage\":\"VARCHAR\"}]')) t(v) WHERE nullif(t.v.matched_phrase,'') IS NULL OR nullif(t.v.passage,'') IS NULL").fetchone()[0]==0)
 if m['work_vocabulary_policy']['version']=='work-vocabulary-v1.1':
  check('vocabulary_work_type_passages_retained',c.execute("SELECT count(*) FROM work_vocabulary_scopes s,unnest(from_json(s.annotation_json->'work_types','[{\"matched_phrase\":\"VARCHAR\",\"passage\":\"VARCHAR\"}]')) t(v) WHERE nullif(t.v.matched_phrase,'') IS NULL OR nullif(t.v.passage,'') IS NULL").fetchone()[0]==0)
  check('vocabulary_context_not_delivered_scope',c.execute("SELECT count(*) FROM work_vocabulary_scopes WHERE scope_status='explicit_scope_labels' AND json_array_length(work_scope_topic_ids_json)=0").fetchone()[0]==0)
  check('vocabulary_topics_have_distinct_facets',c.execute("SELECT count(*) FROM work_vocabulary_topics WHERE facet NOT IN ('navigation','work_scope','platform_context','mission_context') OR facet IS NULL").fetchone()[0]==0)
 check('vocabulary_admin_not_capability',c.execute("SELECT count(*) FROM work_vocabulary_scopes WHERE scope_status='administrative_only' AND json_array_length(topic_ids_json)>0").fetchone()[0]==0)
 check('vocabulary_all_work_edges_explained',c.execute('SELECT count(*) FROM company_match_vocabulary').fetchone()[0]==m['tables']['company_work_matches']['rows'])
 check('vocabulary_evidence_audit_complete',c.execute('SELECT count(*) FROM company_match_evidence_reviews').fetchone()[0]==m['tables']['company_work_matches']['rows'])
 check('vocabulary_rejected_evidence_not_served',c.execute("SELECT count(*) FROM company_match_evidence_reviews e JOIN company_matches m ON m.company_id=e.company_id AND m.episode_id=e.episode_id WHERE e.evidence_verdict='rejected'").fetchone()[0]==0)
 check('vocabulary_incomplete_history_not_accepted',c.execute("SELECT count(*) FROM company_match_evidence_reviews e JOIN company_work_matches w USING(match_id) WHERE e.evidence_verdict='review' AND w.fit_verdict='accepted'").fetchone()[0]==0)
 check('vocabulary_source_financial_checks_versioned',bool(m['work_vocabulary_policy'].get('source_checks_query_ids')))
