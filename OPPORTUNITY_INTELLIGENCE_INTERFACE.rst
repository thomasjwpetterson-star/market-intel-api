Mimir opportunity intelligence: additive interface
=================================================
30 October 2026. Owner: opportunity engine. This is an integration note, not a company-dashboard specification.

Available capabilities
----------------------
The actual authenticated opportunity page and company home consume the same versioned AWS read model. There is no separate sampled backend. current.json is authoritative for the published release; a build in progress is not an available capability. Source and derived row counts are recorded in each release manifest and validation report. Candidate links are evidence relationships, not verified sales opportunities or probability estimates.

Company lookup covers 4,140,081 held CAGE identities. Evidence-rich profiles cover a smaller subset; registry presence alone does not create capabilities or recommendations. Exact NSN/catalogue retrieval and technical work-text retrieval both operate across held company evidence. No company-specific seed list or ten-item cap is required.

Procurement maturity and company access are separate: published requirements, explicit new-source/teaming enquiries, emerging work packages, item forecasts, historical watches, resolved records and evidence gaps. A match does not establish technical qualification, manufacturing source, data rights, set-aside eligibility, or an available subcontract. Direct government procurement and inferred prime supply-chain demand remain distinct lanes.

Storage and refresh
-------------------
S3 bucket a-and-d-intel-lake-newaccount, prefix governance/opportunity_intelligence/. current.json points to an immutable releases/<run_id>/manifest.json, rules.json, validation.json and Parquet tables. Glue database market_intel_platform_candidate; table bindings are supplied by the manifest, never hardcoded per release. Key tables: opportunities, demand_decisions, company_registry, company_profiles, company_scope_features, company_work_matches, company_matches, company_item_matches, company_item_valuations, published_calls, research_awards, research_topics_current, notice_price_links, planning_narratives, force_programme_links and programme_funding. Retained company_work evidence has its own manifest source binding.

The existing Glue job mimir-opportunity-evidence-refresh runs daily at 02:30 UTC (14:30 Fiji). It publishes only after validation; failed runs retain the preceding pointer. This refresh does not imply that every upstream producer has refreshed. Native refreshes read the complete award and reported subcontract projections, resolve revisions, and rebuild all 16 work partitions with a complete-source count check. Source dates remain visible; catalogue and some planning/inventory bindings may still precede the current run. The job refresh does not advance those upstream producers.

HTTP interface
--------------
Main FastAPI mounts /opportunity-intelligence. The browser uses same-origin /api/opportunities; the page is /opportunities, unlinked and noindex. The former /opportunity-pilot and /api/opportunity-pilot URLs are compatibility aliases for the same capability. The Next.js page and every proxy GET verify Supabase getUser() and reject anonymous accounts. The main API verifies the existing server-only application proxy credential plus the asserted verified user UUID. A bearer header alone grants no access. Do not call the main API directly from browser code or expose the proxy credential.

GET /companies?q=<name-prefix-or-CAGE>&limit=20
GET /companies/<CAGE>
GET /companies/<CAGE>/relationships?limit=50&offset=0
GET /opportunities?company=CAGE:<CAGE>&kind=work|items|all&limit=24&offset=0
GET /companies/<CAGE>/monitoring?limit=24&offset=0
GET /opportunities/<episode_id>
GET /opportunities/<episode_id>/value-evidence
GET /opportunities/<episode_id>/supplier-evidence?kind=primes|reports|placements&limit=25&offset=0
GET /value-cohorts/<cohort_id>/awards?limit=25&offset=0
GET /companies/<CAGE>/awards
GET /companies/<CAGE>/subcontracts
GET /companies/<CAGE>/coverage
GET /companies/<CAGE>/events?episode_id=<optional>&limit=50&offset=0
GET /work-evidence/<doc_id>
GET /work-evidence/<doc_id>/sources?limit=25&offset=0
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
The existing response is {records, release, status, eligibility}; monitoring also returns release. History/coverage/events return availability=available|not_published, distinguishing a true empty result from a table absent in the current release. AWS/Athena scalar fields arrive as strings or null. Clients should parse finite numeric fields explicitly and parse *_json strings, without treating missing values as zero. Additive fields may be added; clients must ignore unknown fields.

Identity: episode_id (stable demand-episode hash), record_hash (version of the episode), native_identifier, source_evidence_id, source_name, source_locator, source_url. Company identity: CAGE:<five-character-code> or UEI:<12-character-code>, site CAGE, match_id, match_route, matched_terms, company_evidence_caveat, matched_doc_id, review_category, company_entry_status (when published). Preserve episode_id + record_hash + release in saved/exported records so later changes can be resolved rather than silently replacing evidence.

Scope: title, scope_text, customer_name, programme, psc, naics, lane. Timing: window_start, window_end, date_basis, notice_posted_date. Do not call a response deadline an expected award date, or a performance period a solicitation window. Decision JSON: rule_id, stage, participation_route, reason_title, next_action, source_basis, source_period, unresolved, negative_evidence, source_passages, template_lineage_present, company_addressable_value. The last field remains null unless defensible company-level value exists.

Value: value_low/median/high, value_basis, value_scope and value_inputs_json; these can be observed expenditure, an analogue estimate or a same-item price scenario, as labelled. For item forecasts: forecast_quantity, forecast_catalogue_unit (catalogue-derived, not a unit printed in the forecast), government_reference_unit_price, supplier_average_unit_price, government_reference_lot_scenario, supplier_average_lot_scenario, supplier_average_last_sale, monthly_evidence_json, catalogue_evidence_json. The two gross scenarios are alternatives, not a range or additive value. No unexplained programme ceiling is assigned to a supplier.

Dashboard readiness and remaining work
-------------------------------------
Ready for a company feed: authenticated company search, current demand records, separate historical context, provenance drilldown, next actions, source-specific timing and value, pagination, and CSV of loaded records. The opportunity page supports browser-local saves scoped to the account; personal company-home state is owned by the parallel company-home integration. Cross-device watchlists, persisted notifications, change-event delivery and CRM integrations are not implemented by this integration. No CRM stages or dashboard UI are added.

The company_opportunity_events table records release-to-release source, value, timing, fit and resolution changes when present in the published manifest. Missing snapshots are not cancellations. This is not yet a complete per-user notification service or guaranteed forecast-to-notice/award reconciliation. A later consumer can add its own user->episode subscriptions and change-event outbox without migrating these core tables.

Quality limits: platform/component relationships are incomplete; matching uses full retained scope fragments plus direct reported-supplier lineage, with explicit platform/work-mode conflicts rejected. Broader hardware similarity without a supported entry route is market context rather than a current company opportunity. Exact programme/platform links are context, not technical interchangeability. Unmapped programme/component relationships can still cause misses. Parent/group resolution is not complete, and CAGE-level catalogue association is not technical approval. Competitor lists and actual unsuccessful-bidder identities are not established by these matches. Sparse companies may legitimately have no credible current non-item lead. The UI order is a review order, not validated top-three recommendations or win probabilities.

Acquisition Gateway and long horizons
------------------------------------
The public service was inspected on 30 October: 12,395 listed records and a working public read endpoint at https://ag-dashboard.acquisitiongateway.gov/api/v3.0/resources/forecast. Existing Glue metadata across five databases did not identify an Acquisition Gateway feed. Its current 16-agency filter does not list DoD or DHS. Its CSV export is incomplete relative to the displayed total (732 downloaded records; configured export limit 3,000). It must not be ingested as a complete federal mirror without paginated coverage checks.

The source includes stable node IDs, revision-bearing listing IDs, scope, agency/office, fiscal year/quarter, solicitation date/link, period of performance, value bands, acquisition phase, set-aside, incumbent/contract identifiers where present, contacts, and changed dates. Recommended adapter: immutable public-response snapshots; node ID as entity key, content hash as revision; retain value bands and date precision; distinguish estimated solicitation, estimated award and performance dates; reconcile explicit solicitation/contract IDs before similarity; preserve cancellation/award changes. This adapter is not yet in the daily engine. It is useful for adjacent federal markets; it is not a substitute for defence budget/programme evidence. The GSA expiring-contract dashboard uses public FPDS data and largely overlaps existing award-history inputs.

Programme and FYDP evidence already extends to FY2031 and some contract tails further. There is no global 18-month cap. The native DLA forecast rollup includes every future dated month in its connected source; no 18-month seed cap remains. The connected forecast producer may be an earlier snapshot: its source date and monthly rows remain visible. Future dates are source statements, not generated predictions.

Competitive-risk reuse (deferred system)
----------------------------------------
Existing rules and source versioning can later flag new-source qualification calls against a company's observed items, new competitor awards in a genuinely overlapping capability, negative/reduced programme demand and force-plan retirement signals. The same provenance and entity IDs should be reused. Each needs role-aware competitor/entity resolution and user relevance checks; an award to another company is not automatically a loss and a distributor is not automatically a competing manufacturer. No full risk-monitoring service or MCP/dashboard deployment is part of this phase.

Complete native derivation
--------------------------
The native pipeline aggregates all source award actions before site attribution, retains parent award/agency identities, and connects subcontract recipients through unambiguous UEI/CAGE mappings. Supplier episodes group current funded work by prime, programme/vehicle, functional scope and work mode, rather than repeating one lead for every task order. Explicit new work phases on older contracts are included; administrative and funding-only changes are excluded. Historical supplier reports must predate the signal. Current reports are downstream-placement evidence, not proof that the full package remains open. All underlying prime, supplier and placement records are paginated through supplier-evidence.

Historical value cohorts retain all comparator awards in a normalized observation table, with a short preview and full paginated drilldown. Approximate medians and observed ranges describe historical award packages. Exact item prices without quantities stay unit prices. Company-addressable value is not obtained by dividing or allocating a prime/programme total.

Personal review overlay (separate consumer contract)
---------------------------------------------------
The company-home owner stores focus, description, interests, saved status,
qualify/disqualify decision and notes as an authenticated user-owned overlay.
These are personal judgments, not changes to source demand or global stages,
and are not automatic engine-training inputs. A future reader can join by
company_id/site CAGE, episode_id (or explicit entity type and stable source ID),
saved record_hash/release and updatedAt. Retain the decision when evidence
changes, expose a review-needed flag and link the saved/current evidence
versions. Structured rejection reasons can support explicit relevance filters;
company-team policy requires separate ownership/authorization. This release
adds no global personalization or feedback-learning service.

Exact registry relationship reader
----------------------------------
GET /companies/<CAGE>/relationships returns bounded parent/child edges from the native parent_cage field and separately typed replacement_cage edges. Each has stable relationship_id, content record_hash, subject/object CAGEs and names, evidence_field, source_last_update and record_coverage. Response provenance includes the immutable registry table hash, source bindings and release. No name matching or ultimate-parent consolidation occurs. An empty available result means no valid retained exact edge; it does not establish that the company is independent. These candidate relationships do not transfer qualifications or authorize financial consolidation.

Source quality: item forecasts containing negative monthly quantities in the future horizon are withheld from actionable read feeds pending reconciliation. Published item purchase requests are separate and remain eligible. Source rows stay retained; no cancellation or negative sale value is inferred.
