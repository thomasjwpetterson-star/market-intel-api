Live opportunity intelligence worker
====================================

This is the versioned source of the AWS worker serving the authenticated Mimir
opportunity page and company home through /opportunity-intelligence. The HTTP
adapter is ../opportunity_intelligence; worker-only dependencies are isolated.

The existing Glue job mimir-opportunity-evidence-refresh is configured daily
at 14:30 Pacific/Fiji. The full-market runtime uses Glue 5.0 with two G.2X
workers; infrastructure/runtime.json records its configuration. Frozen Linux
Python 3.11 wheels and their hashes are recorded in RELEASE.json. The trigger
is paused during runtime changes and restored only after successful validation. Source tables are read-only. All derived writes remain under the
existing governance/opportunity_intelligence prefix and versioned oi_* tables.
A release becomes current only after local evidence/financial gates and Athena
readback. Public publication permission is independent of authenticated access.
Demand identity/version checks run before company matching. Forecast rows without
a valid NIIN remain in the source ledger, with excluded-row counts in source
status; they cannot generate anonymous item opportunities. Late validation or
publication failures archive completed derivations in an unpublished operations
checkpoint for controlled recovery, while retaining the prior current pointer.

Every native run refreshes award and subcontract projections, resolves revision
rows before net financial aggregation, rebuilds company work and profiles,
derives new-prime supplier packages from exact parent/program lineage, attaches
historical same-scope/work-mode/buyer comparators, and regenerates company links.
No company sampling or per-company recommendation quota is used.

Values distinguish historical obligations, historical supplier-package ranges,
reference-price scenarios, future demand and company-addressable value. The
last two remain unknown unless their inputs support them. Programme/prime
ceilings are not allocated to a supplier. Eligibility and technical access are
separate from similarity. Subcontract revisions are not summed as new spend.

Tests: PYTHONPATH=opportunity_worker python3 -m pytest opportunity_worker/tests
Rebuild: python3 -m opportunity_engine --from-current --workdir /tmp/mimir-opportunity-engine --refresh --publish
Use the existing AWS account and worker scope. Keep daily schedules and trust
unchanged when updating code. RELEASE.json records the deployed wheel/run.

Business coverage and longer horizons
------------------------------------

company_activity_actions retains revision-resolved trailing-year transaction
identifiers and source URLs. company_activity_baselines reports net and positive
obligations, DoD obligations, award/action counts and the observed source period.
company_pipeline_coverage assesses every evidence-backed profile and annual
activity identity. Three times annual obligations is a diagnostic benchmark;
shortfalls never create demand or loosen eligibility gates. Materiality and
zero-work flags identify where source coverage still needs investigation.

Published purchase requirements, full-source-horizon DLA forecasts, historical
contract-size comparisons and catalogue-market seller sales remain separate.
Forecasts are not capped at eighteen months. Monthly evidence, annual scenarios,
exact item/unit price observations and inventory/reorder snapshots are retained.
Only consistent catalogue EA assumptions are priced; other conversions and
conflicting monthly source quantities are unresolved. Inventory is context and
is not deducted from a procurement forecast. The source operational snapshot
remains September 2026 until upstream monthly ingestion supplies a newer release.

catalogue_sales_evidence links physical price rows to a company's NIIN and exact
part references. company_catalogue_sales splits direct and other sellers and
records shared-reference ambiguity. Other-seller purchases are product-market
context, not attributed manufacturer revenue or additional future pipeline.
company_item_reference_evidence retains selected-CAGE part/RNCC/RNVC/RNSC rows;
these are inspectable facts, with no blanket transfer of manufacturing rights.
item_request_progression links forecasts to current exact-NIIN requests without
claiming fulfilment, an award or an additive value. Stable relationship IDs and
before/after evidence hashes support subsequent change tracking.

The canonical SAM view normalizes ISO deadlines with offsets to UTC and retains
raw text and precision. Date-only values never acquire an invented clock.
Native snapshots retain active/inactive status and include normal solicitations,
sources sought and presolicitations. The serving API removes elapsed precise
response clocks between daily builds. The SQL repair is versioned under
infrastructure/sam_response_deadline_canonical.sql. Producer module SHA256 hashes
and methodological policies are included in each release for reproducibility.

Defense annual comparisons also retain company_reporting_lag_actions and
company_reporting_lag_baselines. The 90-day public reporting delay can make a
recent trailing-year total incomplete. The alternative is an observed full
twelve-month window ending in the last month before that delay window, with
its own dates and source transactions. It is not an annualized estimate or a
certification of complete reporting. Both periods remain inspectable; the
lag-adjusted year informs Defense company coverage benchmarks and materiality.
