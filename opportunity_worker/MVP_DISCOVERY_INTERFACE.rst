MVP interest discovery: additive authenticated API
================================================

The existing signed-in proxy authenticates each request. Both API adapters add
GET /opportunity-intelligence/work-tags and /discovery. No source publication,
worker schedule, IAM change or database migration is required for these routes.
The current manifest remains authoritative; local order/assembly models are not
implicitly promoted. Existing opportunity/detail/evidence identifiers remain.

work-tags
---------
q (optional, max100), facet=all|area|work_scope|context|work_type|classification,
limit1..100, offset0..10000. Response: records, release, availability,
vocabulary_version, tag_contract_version, catalogue_hash, total, has_more.
Each record: tag_id, label, facet, parent_tag_id, selectable, selection_basis,
description, aliases, legacy_topic_id when relevant. Store tag_id, not label.
Default blank/all omits thousands of PSCs. Search or facet=classification adds
PSC nodes. Current data supports96curated tags and3862PSC nodes. No freeform
label is silently promoted to a canonical identifier.

scope:radar = explicit work; area:electronics = recursive work descendants;
context:rotorcraft = source platform/mission context, not delivered capability;
work:design_development = work mode; psc:5840 = reported work classification,
not technical equivalence. Descendants are followed to any retained depth,
with cycle checks and facet separation. Manufacturer/distributor roles, source
rights and qualification are independent of a user's chosen interests.

When bundled source rules and official PSC reference have exactly the published
policy hash and counts, the picker needs no warehouse query. If they differ,
read the released vocabulary tables; never offer newly inferred labels as
already indexed. Missing required tables means availability=not_published.

Discovery feed
--------------
mode=interests|company|combined; tags=comma-separated canonical IDs (max50IDs,
4096characters); match=any|all; company=CAGE:XXXXX or UEI:XXXXXXXXXXXX;
kind=all|work|items; limit1..100; offset0..1000000; release optional expected ID.
Interests needs tags, no company. Company requires a company and accepts no tags.
Combined is the union of eligible company suggestions and chosen interests,
not their intersection. One source episode appears once. Narrow same-buyer,
same-full-title notice repost families are also grouped; all source links remain
in related_source_records_json. Prefer the company's eligible representative,
so a later generic repost does not erase its source/fit relationship.

Responses include records, release, availability, mode, match, selected_tags,
has_more, next_offset, financial_readiness and interpretation. Record identity,
source, timing and value basis remain intact. Added fields:

* discovery_routes: company_match and/or selected_interest.
* matched_interest_tags: tag_id,label,facet,basis,source passages.
* company_fit_established: false for interest-only rows; true means an existing
  engine candidate link, not certified capability or procurement qualification.
* fit_assessment: existing_engine_candidate|not_assessed_for_company.
* qualification: not_established.

Company quantities, unit prices, access evidence and fit caveats are retained
on combined cards. A selected interest does not inflate value or create a win
probability. There is no18monthcap. Source-quality/maturity and company eligibility
queries remain the authority. Release mismatch returns409: reload rather than
mix generations. Ordering is deterministic; interest matches are not qualified
commercial opportunities. Retain episode_id+record_hash+release in saves/CSV.

Free/paid enforcement belongs to the existing verified Next.js proxy and
protected account entitlement source. Technical backend limits are not a paid
plan. The browser never receives the server proxy credential. CSV must use the
existing export entitlement/allowance service, not a free pagination bypass.
Do not add CRM stages or send watch emails merely because records can be saved.

Subcontract financial quarantine
-------------------------------
financial_validation=not_validated marks recurring_subcontract_work and
new_prime_historical_supplier_flow until a rebuilt release explicitly proves
reconciliation to production's adjusted subcontract source. Canonical value
bounds/commercial amounts are null; monetary amounts do not boost priority,
pass size filters or enter overview value counts. Raw figures remain under
unvalidated_financial_evidence_json and original nested source inputs, with a
clear reason; they are audit data, not a validated contract size or pipeline.

Raw subcontract-history, supplier-report, underlying company-work/market-work
report and source-event monetary highlights receive the same status. IDs,
descriptions, dates, source hashes and human decisions remain intact. Historical
source hashes do not change merely because this display policy changes.

Prime awards, funded follow-ons, recurring government work, DLA unit prices and
agency plans are separate sources. The main platform's independently cleaned
company-network finance is not quarantined by this opportunity-engine guard.
Do not hide all subcontract company summaries without checking their authority.

To unmask amounts, a complete release must declare subcontract_opportunity_finance
state=validated, value_basis=production_description_assisted_adjusted_reported_subcontract_value,
source_view=market_intel_gold.ref_company_network and validation_query_id, and
company_subcontract_facts must retain value_basis,included_in_adjusted_total,
source_report_identity. A schema flag alone cannot validate old derived amounts.

Launch boundaries
-----------------
The current published source has incomplete detailed technology labels, raw
subcontract history requiring reconciliation, partial forecast-to-notice/award
resolution and uneven upstream freshness. The new order and assembly context
remains local. The scheduled worker cannot read the cleaned Gold view metadata
under its present scoped policy; prepare and approve the exact read-only delta
before rebuilding, with no wider write scope. No clean-source or IAM promotion
is included in this API-only change. No email delivery or public launch links
are enabled by these endpoints.
