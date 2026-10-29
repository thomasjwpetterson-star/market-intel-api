Mimir opportunity intelligence: additive interface
=================================================
30 October 2026. Owner: opportunity engine. This is an integration note, not a company-dashboard specification.

Available capabilities
----------------------
The shared AWS release contains 33 versioned read-model tables. Release 20261029_225314_ff8ea0 was built successfully on 30 October, with remote row-count readback. It contains 286,563 demand episodes, 818,283 company-demand links, 284,305 links supported by retrieved company work, 532,656 catalogue/item links, and 501,168 company-specific forecast pricing records. These are candidate relationships, not verified sales opportunities or probability estimates.

Company lookup covers 4,140,081 held CAGE identities. Evidence-rich profiles cover a much smaller subset (248,952 CAGE/UEI profiles); registry presence alone does not create capabilities or recommendations. Exact NSN/catalogue retrieval and technical work-text retrieval both operate across held company evidence. No company-specific seed list or ten-item cap is required.

Procurement maturity and company access are separate: published requirements, explicit new-source/teaming enquiries, emerging work packages, item forecasts, historical watches, resolved records and evidence gaps. A match does not establish technical qualification, manufacturing source, data rights, set-aside eligibility, or an available subcontract. Direct government procurement and inferred prime supply-chain demand remain distinct lanes.

Storage and refresh
-------------------
S3 bucket a-and-d-intel-lake-newaccount, prefix governance/opportunity_intelligence/. current.json points to an immutable releases/<run_id>/manifest.json, rules.json, validation.json and Parquet tables. Glue database market_intel_platform_candidate; table bindings are supplied by the manifest, never hardcoded per release. Key tables: opportunities, demand_decisions, company_registry, company_profiles, company_scope_features, company_work_matches, company_matches, company_item_matches, company_item_valuations, published_calls, research_awards, research_topics_current, notice_price_links, planning_narratives, force_programme_links and programme_funding. Retained company_work evidence has its own manifest source binding.

The existing Glue job mimir-opportunity-evidence-refresh runs daily at 02:30 UTC (14:30 Fiji). It publishes only after validation; failed runs retain the preceding pointer. This refresh does not imply that every upstream producer has refreshed. Source dates and 15/16 retained work-partition coverage remain visible. Updating raw company/history producers is still required for complete ongoing coverage.

HTTP interface
--------------
Main FastAPI mounts /opportunity-intelligence. The browser uses same-origin /api/opportunity-pilot; the page is /opportunity-pilot, unlinked and noindex. The Next.js page and every proxy GET verify Supabase getUser() and reject anonymous accounts. The main API verifies the existing server-only application proxy credential plus the asserted verified user UUID. A bearer header alone grants no access. Do not call the main API directly from browser code or expose the proxy credential.

GET /companies?q=<name-prefix-or-CAGE>&limit=20
GET /companies/<CAGE>
GET /opportunities?company=CAGE:<CAGE>&kind=work|items|all&limit=24&offset=0
GET /companies/<CAGE>/monitoring?limit=24&offset=0
GET /opportunities/<episode_id>
GET /work-evidence/<doc_id>
GET /opportunities/<episode_id>/price-evidence
GET /companies/<CAGE>/research
GET /research-topics?cage=<CAGE>
GET /research-topics/<topic_id>/benchmarks
GET /innovation-signals
GET /sources
GET /methodology

Pagination is bounded (100 maximum per request); ordering is deterministic within a release. Consumers must discard/restart pagination when release changes. No 18-month predicate is applied by the read API. Forecast horizons follow the dated source; distant programme funding is not converted into a near-term solicitation.

Output contract v1 (additive)
-----------------------------
The existing response is {records, release, status, eligibility}; monitoring currently returns {records,status}. AWS/Athena scalar fields arrive as strings or null. Clients should parse finite numeric fields explicitly and parse *_json strings, without treating missing values as zero. Additive fields may be added; clients must ignore unknown fields.

Identity: episode_id (stable demand-episode hash), record_hash (version of the episode), native_identifier, source_evidence_id, source_name, source_locator, source_url. Company identity: CAGE:<five-character-code> or UEI:<12-character-code>, site CAGE, match_id, match_route, matched_terms, company_evidence_caveat, matched_doc_id. Preserve episode_id + record_hash + release in saved/exported records so later changes can be resolved rather than silently replacing evidence.

Scope: title, scope_text, customer_name, programme, psc, naics, lane. Timing: window_start, window_end, date_basis, notice_posted_date. Do not call a response deadline an expected award date, or a performance period a solicitation window. Decision JSON: rule_id, stage, participation_route, reason_title, next_action, source_basis, source_period, unresolved, negative_evidence, source_passages, template_lineage_present, company_addressable_value. The last field remains null unless defensible company-level value exists.

Value: value_low/median/high, value_basis, value_scope and value_inputs_json; these can be observed expenditure, an analogue estimate or a same-item price scenario, as labelled. For item forecasts: forecast_quantity, forecast_catalogue_unit (catalogue-derived, not a unit printed in the forecast), government_reference_unit_price, supplier_average_unit_price, government_reference_lot_scenario, supplier_average_lot_scenario, supplier_average_last_sale, monthly_evidence_json, catalogue_evidence_json. The two gross scenarios are alternatives, not a range or additive value. No unexplained programme ceiling is assigned to a supplier.

Dashboard readiness and remaining work
-------------------------------------
Ready for a company feed: authenticated company search, current demand records, separate historical context, provenance drilldown, next actions, source-specific timing and value, pagination, and CSV of loaded records. The pilot supports browser-local saves scoped to the account. Cross-device watchlists, persisted notifications, change-event delivery and CRM integrations are not implemented by this integration. No CRM stages or dashboard UI are added.

The release pointer, episode/version hashes, source decisions and existing resolution states can support future change events and forecast-to-notice/award reconciliation. They are not yet a complete per-user watch service or guaranteed same-scope reconciliation. A later consumer can add its own user->episode subscriptions and change-event outbox without migrating these core tables.

Quality limits: platform/component relationships are incomplete; matching currently emphasises short, specific retained work scopes, with explicit platform/work-mode conflicts rejected. It will miss opportunities described only by long text or unmapped programme relationships. Parent/group resolution is not complete, and CAGE-level catalogue association is not technical approval. Competitor lists and actual unsuccessful-bidder identities are not established by these matches. Sparse companies may legitimately have no credible current non-item lead. The UI order is a review order, not validated top-three recommendations or win probabilities.

Acquisition Gateway and long horizons
------------------------------------
The public service was inspected on 30 October: 12,395 listed records and a working public read endpoint at https://ag-dashboard.acquisitiongateway.gov/api/v3.0/resources/forecast. Existing Glue metadata across five databases did not identify an Acquisition Gateway feed. Its current 16-agency filter does not list DoD or DHS. Its CSV export is incomplete relative to the displayed total (732 downloaded records; configured export limit 3,000). It must not be ingested as a complete federal mirror without paginated coverage checks.

The source includes stable node IDs, revision-bearing listing IDs, scope, agency/office, fiscal year/quarter, solicitation date/link, period of performance, value bands, acquisition phase, set-aside, incumbent/contract identifiers where present, contacts, and changed dates. Recommended adapter: immutable public-response snapshots; node ID as entity key, content hash as revision; retain value bands and date precision; distinguish estimated solicitation, estimated award and performance dates; reconcile explicit solicitation/contract IDs before similarity; preserve cancellation/award changes. This adapter is not yet in the daily engine. It is useful for adjacent federal markets; it is not a substitute for defence budget/programme evidence. The GSA expiring-contract dashboard uses public FPDS data and largely overlaps existing award-history inputs.

Programme and FYDP evidence already extends to FY2031 and some contract tails further. There is no global 18-month cap. The present DLA episode seed uses an 18-month window. A bounded native-file check confirmed an explicit FCST_DT column and some dates beyond the filename's nominal 24 months (for example 202902). Those dates are source statements, not generated by the read API. The existing derived item episodes still retain their earlier windows; extending these episode rollups to the full native horizon remains backend work, separate from the programme/API support already available.

Competitive-risk reuse (deferred system)
----------------------------------------
Existing rules and source versioning can later flag new-source qualification calls against a company's observed items, new competitor awards in a genuinely overlapping capability, negative/reduced programme demand and force-plan retirement signals. The same provenance and entity IDs should be reused. Each needs role-aware competitor/entity resolution and user relevance checks; an award to another company is not automatically a loss and a distributor is not automatically a competing manufacturer. No full risk-monitoring service or MCP/dashboard deployment is part of this phase.
