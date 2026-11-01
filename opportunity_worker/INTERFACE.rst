Opportunity engine: company and demand interface
===============================================

This is an additive extension of the existing authenticated opportunity API.
Published records remain immutable, versioned AWS outputs. The daily native
worker regenerates the company/work inputs, market demand, company links,
progression and coverage tables; publication requires all release gates and
remote readback. No primary contract or company source tables are modified.

Implemented capabilities
------------------------

* Published SAM/DIBBS requirements, DLA source forecasts, stock-supported
  outlooks, agency plans and evidenced prime-to-subcontract pull remain available.
* Whole-market award/report histories now generate recurring customer/work
  demand and funded-contract follow-on planning records.
* Company matching profiles cover every held registered CAGE identity. Readiness
  distinguishes observed evidence, declared classifications and identity only.
  A registry row is not proof of an active defence business or qualification.
* Normalized work capability histories retain actual company/customer/scope,
  PSC, NAICS, work mode, programme context and source evidence. Exact observed
  service capabilities can identify a different buyer; hardware/repair source
  qualification is not transferred by similarity.
* Current registered parent-CAGE chains retain their paths, source dates and
  unresolved/cycle states. Parent references do not transfer site qualification.
* Repeat-buying estimates use observed cadence and completed historical package
  amounts. Subcontract and follow-on package amounts remain benchmarks.
* SAM notice associations distinguish explicit incumbent-contract references
  from candidate same-office/scope matches. Association alone does not establish
  a successor award, applicant eligibility or cancellation of a buying stream.

Tables and identifiers
----------------------

The current manifest maps logical names to immutable Glue/S3 tables. Consumers
must pin one release for the entire request. Never construct table names.

Still-funded older contracts remain included even without a recent action. Buyer
labels can be resolved from exact held office/UEI references; missing labels do
not trigger fuzzy corporate assignment. Unsupported source references are held out.

Company identifiers: CAGE:<five uppercase characters>; unresolved UEI identities
remain UEI:<identifier>. Source attribution stays at site/observed identity level.
Opportunity episode_id is stable for its customer/work stream and reason;
record_hash versions evidence, calculations and associations. A saved opportunity
should retain both plus its release. Procurement events retain source IDs/hashes.

Additional logical tables:
company_matching_profiles, company_parent_resolution, company_capability_history,
market_work_history, market_recurring_demand, market_continuation_work,
market_supplier_history, market_capability_customer_links,
market_recurrence_training, market_recurrence_validation,
market_contract_reference_index, market_demand_progression, market_customer_references.

API contract
------------

Existing /opportunity-intelligence endpoints remain backwards compatible.

* /companies/{cage}/overview adds work_forecast with separate 12/36-month
  conditional buying totals and every contributing stream/calculation.
* /companies/{cage}/matching-profile supplies readiness and paginated capabilities.
* /opportunities and /opportunities/{episode_id} retain the existing row contract.
  New lanes: recurring_government_work, recurring_subcontract_work,
  funded_contract_follow_on. New stage: expected_demand; never a response deadline.
* /opportunities/{episode_id}/work-evidence supplies the actual historical buying
  records. Optional cage selects the exact company evidence behind its match.
* /opportunities/{episode_id}/progression supplies notice associations, their
  source versions and the distinction between explicit reference and candidate fit.
* Existing /companies/{cage}/events and match-review endpoints remain available.

value_inputs_json holds comparator sources, assumptions, cadence, training cutoff,
forecast reference month, projected 12/36-month ranges, prior supplier counts and
linked notices. company_addressable_value remains null without workshare evidence.
Clock-only daily runs do not rewrite the monthly recurrence projection.
/methodology exposes the recurrence, subcontract and follow-on assumptions.
The two-year follow-on engagement lead is illustrative account-planning timing,
not an estimated solicitation date.

commercial_value_kind adds recurring_demand_scenario and
historical_package_benchmark. The former is gross multi-year customer buying;
the latter is a size guide, not estimated company revenue. Published requests,
DLA forecasts and work forecasts are not additive until overlap is resolved.
Ranking compares recurring work on next-year demand; follow-on guides account
for annualized historical scale and distant engagement windows. No win probability
is inferred from the ranking score.

Dashboard readiness and limitations
----------------------------------

The signed-in company feed, saved episode references, CSV, evidence drilldowns,
change history and separate forecast summaries can feed a company dashboard.
This work does not change MCP deployment, public marketing or dashboard UI files.
Public exposure remains a separate product/access decision.

Capability normalization is deterministic and conservative, not a complete
semantic ontology. Platform/component links remain incomplete. Programme funding
is not allocated to supplier work without supporting package evidence. Retrospective
recurrence screens use current reconstructed records and are not point-in-time
forecast accuracy or company-retention estimates. Unknown eligibility, actual
supplier selection and unreported subcontract placement remain explicit.
