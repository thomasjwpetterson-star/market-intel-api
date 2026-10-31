Mimir opportunity intelligence: additive interface
=================================================
30 October 2026. Owner: opportunity engine. This is an integration note, not a company-dashboard specification.

Available capabilities
----------------------
The actual authenticated opportunity page and company home consume the same versioned AWS read model. There is no separate sampled backend. current.json is authoritative for the published release; a build in progress is not an available capability. Source and derived row counts are recorded in each release manifest and validation report. Candidate links are evidence relationships, not verified sales opportunities or probability estimates.

Company lookup covers 4,140,108 held CAGE identities. Evidence-rich profiles cover a smaller subset; registry presence alone does not create capabilities or recommendations. Exact NSN/catalogue retrieval and technical work-text retrieval both operate across held company evidence. No company-specific seed list or ten-item cap is required.

Procurement maturity and company access are separate: published requirements, explicit new-source/teaming enquiries, emerging work packages, item forecasts, historical watches, resolved records and evidence gaps. A match does not establish technical qualification, manufacturing source, data rights, set-aside eligibility, or an available subcontract. Direct government procurement and inferred prime supply-chain demand remain distinct lanes.

Storage and refresh
-------------------
S3 bucket a-and-d-intel-lake-newaccount, prefix governance/opportunity_intelligence/. current.json points to an immutable releases/<run_id>/manifest.json, rules.json, validation.json and Parquet tables. Glue database market_intel_platform_candidate; table bindings are supplied by the manifest, never hardcoded per release. Key tables: opportunities, demand_decisions, company_registry, company_profiles, company_scope_features, company_work_matches, company_matches, company_item_matches, company_item_valuations, published_calls, research_awards, research_topics_current, notice_price_links, planning_narratives, force_programme_links and programme_funding. Retained company_work evidence has its own manifest source binding.

The existing Glue job mimir-opportunity-evidence-refresh runs daily at 02:30 UTC (14:30 Fiji). It publishes only after validation; failed runs retain the preceding pointer. This refresh does not imply that every upstream producer has refreshed. Native refreshes read the complete award and reported subcontract projections, resolve revisions, and rebuild all 52 work partitions with a complete-source count check. Source dates remain visible; catalogue and some planning/inventory bindings may still precede the current run. The job refresh does not advance those upstream producers.

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
Ready for a company feed: authenticated company search, current demand records, separate historical context, provenance drilldown, next actions, source-specific timing and value, pagination, and CSV of the complete selected category. Opportunity-page saves now use the same authenticated owner-only review store as company home, with episode ID, company CAGE, release/hash and saved evidence. Existing notes, decisions and follow-up metadata are preserved on unsave; revision conflicts require explicit reload. Previous browser-only IDs are retained on the device and are not silently assigned to a CAGE. Notification delivery and CRM integrations are not implemented by this engine integration. No CRM stages or dashboard UI are added.

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

Read consistency: authenticated requests pin one immutable release for table selection and response metadata. Evidence responses include release; consumers must restart pagination/drilldowns if it changes. This prevents a slow database read from returning prior-generation records labeled with a newly published release.

Runtime capacity
----------------
The full-market worker uses the existing Glue job/role with Glue 5.0, two G.2X workers and the same declared source/read and opportunity/output permissions. A frozen wheel bundle includes native Linux CPython 3.11 dependencies. Logs retain the existing authorized /aws-glue/python-jobs prefix. Company matching uses narrow candidate keys, then supporting scope records; compute partitions do not truncate the company or opportunity population. The recurring trigger is restored only after successful end-to-end publication following a runtime change.

Historical valuation coverage (30 October 2026)
----------------------------------------------
The native comparator library holds 16,725,041 of 17,932,825 positive awards with latest action on/after 2021-10-30: 93.3% by record count and 77.7% by observed lifetime net obligations ($2.441T of $3.143T). This is library coverage, not the fraction of new opportunities priced, annual spending or a company-addressable market. Scope/work-mode/buyer comparison requirements still leave individual opportunities unvalued. Query 6c4257c7-f141-4975-81b1-f1aa39ecf77c and the bounded coverage audit retain the population definition.

Retained product-reference evidence
-----------------------------------
A metadata-only Parquet audit confirmed 17,467,537 retained company/item reference rows, including CAGE, NIIN/NSN, part number, unit of issue, government reference price, supplier average price, last sale, RNCC, RNVC and RNSC, plus immutable source hash and row provenance. These codes must not be described as absent. The price drilldown uses exact company/item references. The source-access review additionally checks an exact notice NIIN and named part against nonconflicting RNSC A, RNVC 2 and RNCC 3/5 references; it retains code values, source hash and row. This supports procurement-source evidence, not data rights or transferable manufacturing qualification. Document availability/data-rights fields are not in this particular normalized reference projection. Do not infer their absence from the wider source estate. Catalogue association, design-control evidence, procurement approval and usable technical data are distinct facts.

Supplier roles and source recency
--------------------------------
Supplier-evidence report/placement records now optionally include supplier_role_assessment, supplier_profile_hash and retained manufacturer/wholesale award indicators, joined by exact company_id to the same published company profile. Existing manufacturing, distribution and mixed-role classifications are reused; unknown roles remain unknown. These are company-level indicators, not package-specific manufacturing approval or confirmed bidding competition.

A bounded recency audit found broad retained prime latest-action coverage through July 2026 and sparse later months. The prime API ingestion, bronze-to-silver and embargo-reconciliation jobs all succeeded on 30 October. GSA documents a 90-day restriction on public DoD award data (https://open.gsa.gov/api/contract-awards/); do not interpret the recent drop as reduced demand or, on its own, an ingestion failure. The existing daily DoD contract-announcement producer also succeeds, but that source is not yet a demand input to this engine. A build timestamp is not an observed transaction date.

Complete category exports
--------------------------
The opportunity page traverses every page of the selected company/category in bounded authenticated requests, with progress and cancellation. A changed release or repeated/invalid identifier aborts before download. Saved-only exports filter the complete current category using the user-owned saved IDs; they do not export archived or absent source records. CSV rows include company CAGE and evidence release, clean display titles, original source titles, provenance and value inputs. JSON evidence is preserved as valid JSON and spreadsheet formula starts are neutralized. This replaces the former loaded-card export restriction.

Operational refresh metadata
----------------------------
GET /sources adds refresh.status and optional started_at, finished_at and refresh_mode. Values are running, succeeded, failed_preserved_previous_current_pointer, status_stale or unavailable. Error strings and infrastructure diagnostics are withheld. This operational report is separate from the pinned evidence release and does not advance source observation dates. The page rechecks while visible and offers an explicit reload for newer releases; it does not replace evidence underneath an open review or mix releases within exports.

Source coherence and company-fit review
---------------------------------------
The native daily worker applies a versioned quality pass to the complete retrieved edge population. A bounded quality-only mode can re-review the same immutable published sources without re-extracting award actions. This is the actual backend used by /opportunities; there is no separate limited company sample or recommendation quota. Legacy pilot URLs redirect to the canonical page, and their proxy routes are compatibility aliases.

opportunity_quality_reviews retains quoted conflicting nomenclature/response deadlines, the original source record hash and prior decision JSON. Conflicting current records become evidence_gap pending source revalidation, preserving their source records. A newly posted timestamp cannot make an old response date current. Existing-OEM sourcing inquiries are item demand; they are not automatically new-manufacturer qualification calls.

company_match_assessments records accepted/review/rejected scope findings, selected company work/evidence IDs, requirement/profile hashes and policy hash. Administrative wording and incidental geographic/body overlap are rejected from serving links. Unknown OEM access and SBIR applicant/teaming eligibility remain review-only context. Direct reported-supplier lineage is retained for meaningful work streams; generic administrative descriptions do not establish such a package. These deterministic gates improve precision but do not constitute calibrated semantic accuracy or eligibility guarantees.

Additive response fields when quality tables are published: source_entry_kind, source_quality_verdict/reason/passages_json, source_body_deadline, fit_verdict, fit_reason and company_source_access_evidence_json. Category overrides are applied before sorting/pagination, including CSV export. Qualification/data rights remain separate from reference-code authorization. Consumers must preserve unknown fields and the pinned release.

Related notice display
-----------------------
The reader groups only SAM records sharing the same buyer, normalized complete title and procurement identifier after removing a trailing -00 variant. The latest posted retained record represents the group; related_source_records_json retains every episode, identifier, source URL and evidence ID. Different procurement IDs for the same item are kept separate. This is transparent display grouping, not proof of an agency amendment relationship. Values are never summed. Grouping precedes pagination/export.

Explicit single-item notice quantities
--------------------------------------
The serving projection exposes notice_quantity, notice_quantity_unit, notice_quantity_basis, notice_quantity_evidence, notice_quantity_policy and notice_quantity_source_evidence_id when a retained SAM notice has one NSN and a consistent explicit estimated-spares/quantity-required statement. Multiple item IDs, conflicting quantities, ranges and recurring rates are not reduced to a lot size. The quoted text and source hash remain available. These facts do not turn an inquiry into an order or price it from a different item. CSV preserves the additive fields. GET /methodology includes these rules and the published source/fit policy version/hash.

Serving authority
------------------
company_work_matches retains the retrieved evidence population, including fit_verdict=rejected rows for audit. company_matches is the filtered serving-edge projection. Consumers should use the authenticated API or join the serving edges; raw candidate/review-category counts are not counts of qualified opportunities.

Exact saved-source checks
-------------------------
GET /opportunities/<episode_id> returns the same source_entry_kind, source-quality verdict/reason/passages/body deadline, top-level source stage and quoted notice_quantity facts as list projection. Quality joins bind both episode_id and record_hash. The response source_check_scope explicitly states that this company-independent endpoint checks demand evidence only: it does not revalidate company fit, source access, applicant eligibility, personal qualification decisions or a saved company overlay. Existing-OEM inquiries must retain their source-entry wording instead of falling back to a generic new-manufacturer qualification action.

Work-family and delivery-mode precision
---------------------------------------
Policy scope-and-source-v3 rejects non-lineage matches supported by only one shared work/product word. It assesses component supply, repair/overhaul, installation, onsite services and facility construction separately. Explicit scope wording and unambiguous PSC coding of the selected company document support this constraint; neither PSC nor NAICS independently establishes relevance or eligibility. A prime's service PSC is not inherited into its historical component supplier's work. Requirement and selected-work modes, classification rows and shared terms remain in the assessment. Evidence for component supply does not establish execution of onsite replacement services. Conversely repair history does not automatically establish new manufacturing. These are evidence gates, not a finding that a company lacks a capability. Native retrieval prefers compatible delivery evidence when multiple company records match.

Source metadata optionally adds required_delivery_mode, delivery_mode_basis, requirement_psc and psc_reference_url. Company lists optionally add observed_delivery_mode, observed_work_psc, work_classification_evidence_json, mode_reason and shared_scope_terms_json. Review-only caveats state the actual gap; the pre-quality caveat remains in the audit data. Rejected candidates use review_category=not_recommended and remain absent from serving edges.

GET /companies/<CAGE>/opportunities/<episode_id>/match-review exposes the current assessment, including rejected candidates, bound to the current source and company-profile hashes. engine_company_fit_status is not_recommended, review_required, source_not_actionable, context_only, not_currently_served or current_scope_match_access_unconfirmed. It does not overwrite private saved qualification or notes. An empty assessment is not a finding that the company lacks a capability; item-reference matching is separate. Consumers can combine this with exact-source detail for a deliberate current-source/current-fit check.

Business coverage, complete horizons and source clocks (31 October 2026)
------------------------------------------------------------------------
The additive overview.business_coverage contract exposes the recent reported
annual_net_obligations/annual_dod_net_obligations, action/award counts and period.
Separate lag_adjusted_* fields contain an observed full twelve-month comparison
ending in the last calendar month before the nominal 90-day DoD publication-delay
window. Neither window is extrapolated or certified complete. Consumers should
use benchmark_annual_net_obligations, benchmark_basis and benchmark_period_start/
end to explain coverage_benchmark, rather than recomputing three times the recent
reported-year field. The current reference window is 2025-08-01 to 2026-07-31.
The documented source is https://www.usaspending.gov/data/about-the-data-download.pdf.
GET /companies/<CAGE>/business-evidence accepts kind=actions,
kind=lag-adjusted-actions and kind=catalogue-sales with bounded limit/offset.

The corresponding normalized stores are company_activity_actions,
company_activity_baselines, company_reporting_lag_actions,
company_reporting_lag_baselines, catalogue_sales_evidence,
company_catalogue_sales and company_pipeline_coverage. The last evaluates the
whole evidence-backed profile/reference-year population, flags large sites
without work and material-value gaps, and records a diagnostic benchmark. A
coverage target never creates an opportunity, increases a price or loosens fit.
The registry remains searchable even where capability evidence is insufficient.

Published requirements and the full source horizon of DLA forecasts are separate
headline values. Forecast summaries retain monthly quantities, annual scenarios,
source period, dated same-item/unit market prices and fallback government
references. Consistent catalogue EA is an explicit forecast-unit assumption;
unverified pack conversions and conflicting/negative monthly rows are withheld
from valued totals. Inventory/reorder snapshots are context, not quantities to
subtract from a procurement forecast. Source dates are not advanced by a build.
item_request_progression links a forecast to a current exact-NIIN request with
stable relationship IDs and both hashes; it does not establish fulfilment, an
award or permission to add/deduct overlapping quantities. Price and stock changes
produce before/after evidence events even when a forecast ID is unchanged.

Other-seller item purchases are historical product-market context. Exact part
references and shared-reference ambiguity remain drillable. They are not added
to direct obligations, manufacturer revenue or future pipeline. Selected-CAGE
RNCC/RNVC/RNSC and part records are retained by company_item_reference_evidence
and summaries; GET /companies/<CAGE>/opportunities/<episode>/item-source-references
returns the underlying rows. These source facts remain separate from data rights.

The shared canonical SAM response parser now accepts ISO timestamps and offsets.
Native opportunities retain source_response_deadline_raw, precision and exact UTC
clock in value_inputs_json. Date-only values do not acquire an invented time.
The reader expires precise UTC deadlines between daily snapshots. Inactive agency
flags are retained and tracked distinctly from an inferred cancellation or a
record simply absent from a subsequent snapshot. The canonical SQL is versioned
under opportunity_worker/infrastructure/sam_response_deadline_canonical.sql.

Agency-stated item approvals (producer 0.5.39)
---------------------------------------------
The producer adds direct item links for structured DLA notices that explicitly
name an approved CAGE/part and one NSN, corroborated by the exact held catalogue
tuple. This bypasses missing lexical work matches without transferring approval
from a different product or site. Current exact RFQ/NIIN DIBBS matches are reused;
the SAM approval keeps its own notice ID/hash/URL in
company_approved_notice_sources. Multiple current agency copies remain evidence
rather than extra company prospects. Additive item fields are agency_approval_quote,
agency_approval_source_url, agency_approval_part_number and
agency_other_sources_json. Other named references may be legacy sources; they
are not confirmed current bidding competitors or proof of corporate independence.

Structured DLA Line/Qty/UI prose supplies quoted single-item quantity facts,
including notice_quantity_is_estimate and any separately stated aggregate ceiling.
A minimum, ceiling or recurring rate is not substituted for the requested quantity.
New-item purchase references do not value repair, installation, facilities or R&D
work; scenarios above a stated aggregate ceiling are withheld. Both the quantity
quote and price evidence remain inspectable. The methodology endpoint includes
producer hashes and the source-approval/reporting-lag policies. Availability is
manifest-driven; consumers must preserve missing optional fields on older releases.

Forecast correction consistency (producer 0.5.40)
-------------------------------------------------
The whole-market-item-demand-v2 policy inspects every retained future monthly row,
including corrections before the first positive month or after the last positive
month. Any negative future quantity or duplicate monthly source row blocks a
price scenario. Quality refreshes rebuild the same valuation/context tables before
coverage diagnostics, using downloaded immutable table paths. A separate release
gate checks the complete future-month population independently of the positive
demand window. Native episode fingerprints also include signed and zero source
rows, preserving the stable item ID while recording corrections as source changes.
The methodology response exposes item_demand_policy additively.

Early announcement source audit
--------------------------------
The existing independently refreshed DoD announcement feed contains 7,552 entries
through 2026-10-30. In the August-October window, 1,183 announcements include 641
with explicit funding fields. Exact primary-contract matching found 305 unique
contract links; 124 have historical supplier reports. The 7,280 observed
announcement/supplier relationships are candidate account links, not qualified
work packages. The changed work itself must match the supplier's delivered scope:
an F-35 assembly-station upgrade does not imply a new turbomachine order merely
because that supplier appears under the same prime contract. Announcement amounts,
cumulative ceilings and supplier-addressable work remain separate. This source
already exists; the missing work is its controlled connection and scope validation
in the opportunity engine, not a new upstream news ingestion system.

Independent item-source scale check (2026-10-31)
-----------------------------------------------
A separate reconciliation of all held future forecast months, exact company/NIIN
references, consistent EA catalogue units and dated physical procurement lines
finds 497 Ontic Chatsworth (45934) referenced forecast items. Of these, 444 can be
priced, giving a gross reference scenario of $68,901,487.30 before company access
review and overlap with published requests. This is not manufacturer revenue or
an open-order pipeline. It is independent of the serving release and must be
reconciled to that release rather than substituted for its totals.

Of that scenario, $37,449,474.19 across 88 items uses dated observed purchase
medians. The other $31,452,013.11 across 356 items uses government references with
no retained effective date. Calendar-year amounts are approximately $5.51m in
November-December 2026, $34.63m in 2027, $20.54m in 2028, $6.83m in 2029,
$1.38m in 2030 and $17,896 in 2031. The last observed month is July 2031;
the far end does not imply uniform forecast coverage through that year.

The same calculation gives only $3.79m for GE CAGE 07639, $7.61m for Raytheon
CAGE 15090 and $411.98m for Boeing CAGE 81205. Compared with those sites' observed
contract activity, this demonstrates why NSN demand cannot establish adequate
prime/integration/service coverage. Contract, current-notice and supplier-work
lanes must be assessed separately. Company scale is a coverage diagnostic, never
a justification for adding programme ceilings, reseller turnover or unplaced
supplier work without evidence.

The held canonical SAM source also contains 18,658 Defense Award Notices posted
in August-October 2026; 18,648 carry positive award amounts and every row carries
an award number, date and recipient name. These are notice amounts, not incremental
obligations. Existing native notice-chain heads already resolve matching published
requirements when an award or cancellation supersedes them. Among all 19,670
recent notices carrying award numbers (including justifications and consolidation
notices), 726 have an unambiguous historical PIID link and 595 have an award date
later than the linked retained latest action. Two PIID links are ambiguous and
18,942 lack a held historical contract link. The normalized award-recipient
projection has names, not CAGE/UEI fields; do not infer a site from a name match.
This is a useful additional early-activity connection to develop alongside the
DoD announcement feed, with separate value semantics and source reconciliation.

Held-document and reference freshness checks
--------------------------------------------
The CAGE master contains 4,104,335 distinct codes without duplicate source rows.
Its held address/status objects were published into this lake on 2026-07-08;
the most recent parseable company-update field is 2026-01-14. No newer object
exists in those held prefixes. This does not establish current registration or
technical approval. The registry also incorporates separately dated SAM and
observed-work identities; do not present the complete CAGE population as active
Defense suppliers. A daily consumer rebuild cannot advance an upstream snapshot.

The existing SAM attachment candidate table contains 1,146,531 extracted lines
from 12,108 notices / 9,931 distinct attachment hashes. Of those lines, 1,140,759
have untested arithmetic and all retain pending notice/document scope validation.
The governed price table already used by this engine contains 2,133 accepted
lines from 113 notices / 107 attachment hashes, linked to 2020-2026 actions.
The separate structured SAM solicitation-evidence table exposes CAGE/UEI columns
but currently contains zero records; schema presence is not populated coverage.

An exact-notice join of candidates to active Defense notices with an unexpired
structured response date found 12 candidate lines under two notices, all from
one workbook-instruction attachment. The apparent quantities include numbered
instructions such as "2 Each workbook ...", not purchasing requirements.
Allowing recent notices without a verified future window increases overlap to
196 notices / 1,853 lines, largely including elapsed August requirements; it
does not establish current availability. No candidate shares a populated canonical
selected-version ID in this test. This is why candidate quantities/prices must not
enter opportunity totals unchecked. Reuse the existing extraction stack, but
prioritise current-document coverage, physical-line deduplication, latest-version
linkage and real CLIN/scope validation before broader promotion.

Full-market recovery and bounded aggregation (0.5.43)
----------------------------------------------------
The native expansion completed all 52 matching partitions: 205,085,071 candidate
comparisons produced 957,301 selected company/work links and 574,785 item links
before final source-quality review. These are candidate relationships, not counts
of accepted commercial opportunities. A late ordered forecast aggregation exceeded
the driver's memory cap. The completed matching tables and source checksums were
retained in an unpublished checkpoint rather than discarding the completed work.

Version 0.5.41 partitions forecast and annual evidence aggregation by whole NIIN,
preserving each item's complete ordered source history. A regression over
2,021,446 future monthly rows and 106,140 items completed under a 1 GB DuckDB cap
in 10.56 seconds, with no lost rows, divided item groups or duplicated quantities.
The worker also accepts an explicit, owned complete-derivation checkpoint through
the quality/validation path. It verifies the current published parent, required
matching tables, immutable paths and content hashes, then reruns source-quality,
valuation, relationship, change tracking and publication gates. Checkpoints are
never themselves serving releases. Recovery arguments and fixed assessment dates
must not be persisted in daily job defaults.

The full monthly history occupies approximately 1.62 GB before compression across
529,406 company/item edges. Downstream relationship/source-evidence rewrites use
one writer thread and 2,048-row Parquet groups to bound the write buffer. This
preserves complete evidence rather than shortening each item's history to fit
the previous default 122,880-row group. The request rewrite retains its 2 GB
query cap and the company-reference rewrite retains its 3 GB cap.

Coverage and evidence-change checks also use 32 complete-company partitions under
their existing 3 GB query caps. Evidence contexts are serialized once and reused
across partitions. The quality path archives completed reviewed outputs if any
later coverage, event or validation step fails, preserving the published parent
without exposing incomplete output. These changes bound memory without reducing
the company population, shortening histories or creating values to meet a quota.

The serving API adds optional ``inventory_priority_points`` and
``inventory_priority_basis`` under commercial-materiality-v3. Only source-item
forecasts are adjusted. A nonfuture snapshot no older than 90 days at assessment
adds 10 points for reported backorders, otherwise 6 for stock below its stated
reorder point; stock sufficient for the next twelve-month forecast reduces
priority by 8. Missing, conflicting, future or older evidence is neutral. These
are transparent policy weights, not calibrated win probabilities. Monetary values
are unchanged. The methodology endpoint exposes the policy and implementation
hash; CSV and the existing rationale drawer retain the row-specific explanation.

Forecast demand does not establish net new buying. Stock allocations, supply
already on order and existing contract coverage can affect replenishment. The
engine does not estimate those missing quantities or subtract a dated stock
snapshot to manufacture a net procurement value. The public planning distinction
is documented in AFMCI 23-105, chapter 3:
https://static.e-publishing.af.mil/production/1/afmc/publication/afmci23-105/afmci23-105.pdf

Scope precision follow-up (0.5.44 / scope-and-source-v6)
------------------------------------------------------
A full recheck of all 957,301 selected work relationships identified 1,327
previously accepted and 354 review-only relationships whose selected evidence
does not establish the named technical scope. General facility-name patterns and
administrative purchase-order/code-only descriptions no longer count as capability
evidence. The correction uses no company-specific or hand-written site aliases.
Specific voice/data services, turboprop-engine supply and identified product/manual
licensing remain usable scope; a shared site name or a PO number does not.
The named raw historical relationships remain available for inspection. A
successful source-backed supplier history is still separate from proof that a
future package is unplaced. These precision corrections do not increase or pad
monetary opportunity values.

Verified non-item forecast ingestion gap
---------------------------------------
The held canonical Defense SAM corpus contains 23 long-range acquisition forecast
notice rows since 2025, including notice revisions. None joins to an extracted
pricing-candidate attachment in the current corpus. These cover notices must not
be counted as 23 independent planned contracts. NAVAIR publishes current
requirement-level forecasts separately; the opportunity engine does not yet
explode and version those planned contract rows.

For example, NAWCWD's January 2026 public forecast row 4 identifies F/A-18 sensor
system support, a $250m-$1bn anticipated total including options, a FY2027 Q4
solicitation and FY2028 Q3 award, 60 months, and an anticipated sole-source route.
The named prior contract N6893623D0034 resolves in Mimir to CAGE 4U884 / UEI
NBSLHP77ZJQ1, rather than every Raytheon site. The underlying IDV has zero direct
obligations; child orders and the future planning band are distinct evidence.
This planning example is not connected to serving totals. A future adapter must
retain fiscal-quarter precision, source value ranges, option/IDIQ semantics,
incumbent-contract identity, eligibility, latest document hashes and later notice
resolution. It must not turn an entire forecast document or programme ceiling
into a company's addressable revenue.

Official forecast:
https://www.navair.navy.mil/nawcwd/sites/g/files/jejdrs601/files/document/%5Bfilename%5D/NAWCWD%20LRAF%20FY%202026.pdf
Current NAVAIR forecast index: https://www.navair.navy.mil/osbp/

Publication and evidence retention (0.5.45)
------------------------------------------
The full-market release 20261031_143426_2a8e28 is published after all 197 local
validation gates and complete Athena count readback passed. The approved daily
trigger is active at 14:30 Fiji, using native refresh with no recovery checkpoint,
fixed assessment date or auxiliary-retention override in its default arguments.

Publication recovery restores SHA-verified completed tables and re-runs their
validation; it does not rerun matching or alter monetary calculations. Raw
archives are content-addressed and checked against their retained SHA-256.
One standalone innovation feed retains the prior complete archived snapshot with
its original source dates because the newer DIU detail raw file was unavailable.
A dependency guard verified that no opportunity or monetary outputs depend on
that auxiliary feed. The manifest records auxiliary_source_retention and exposes
its source status; a refresh timestamp must not imply that this source advanced.
The verification report records representative-company checks and actual serving
results separately from release gates.
