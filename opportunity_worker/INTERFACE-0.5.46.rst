Opportunity intelligence — additional coverage contract
=======================================================

This extends the existing authenticated feed without a database migration.
The opportunity page remains unlinked and noindex. Company-home and MCP deployment
files are unchanged. No new cloud role or source permissions are required.

Company scale
-------------
``business_coverage`` adds aligned other-seller purchases and exact per-part source
roles for all profiled companies. ``direct_and_linked_part_market_reference`` is
positive observed direct annual obligations plus same-period other-seller purchases
carrying the company's exact item-identifying part. ``linked_market_coverage_reference``
is three times that comparison reference. It is neither manufacturer revenue nor
qualified pipeline; alternative sources, stock and other sites may be represented.
Missing purchase months are not annualized. Direct-seller item purchases are excluded
from the addition. The existing direct-obligations benchmark remains available.

``companies/{cage}/business-evidence?kind=item-references`` returns bounded source
records with their original RNCC/RNVC/RNSC codes and immutable source provenance.
RNSC K indicates manufacturing OR design control, not proof of both or of all rights.
An item association does not transfer technical qualification.

Conditional replenishment outlook
----------------------------------
``lane=stock_supported_item_demand``; ``commercial_value_kind=annual_usage_scenario``.
Recent public DLA inventory (at most 90 days old) must show unambiguous positive
annual demand and backorders or stock below that demand. Any future DLA forecast
row, including zero or negative demand, takes precedence and excludes this lane.
Matching requires a current RNVC 2 reference; held RNSC B records are not a route.
Pricing requires consistent catalogue EA and a held same-item price. The source's
inventory quantity unit is unstated; EA is an explicit assumption.

This is annual usage repeated for the next twelve months, not a predicted order,
net stock shortage, procurement date or company revenue. Incoming supply and
existing contracts remain unknown. Snapshot, quantity, price and assumptions are
retained. ``valuation_status=reported_annual_usage_EA_assumed_not_source_forecast``.
The compatible monthly_evidence_json field holds one explicitly ANNUAL interval;
it contains no fabricated monthly distribution or source forecast month.

``overview.replenishment`` has count, conditional_annual_usage_value and all contributing
records. It is excluded from published-demand totals and source-forecast totals.
Exact NIIN links to actual published requests are retained without asserting
conversion or subtracting quantities.

Agency planned work packages
---------------------------
Official NAVAIR consolidated workbook and NAWCWD forecast PDF are parsed with strict
layout/population checks. Original source bands include options and may be one-sided;
no midpoint is invented. Source fiscal solicitation and award periods are distinct
from response deadlines. Duplicate identities, conflicting dates, elapsed planned
solicitation periods without a linked outcome, and superseded unparsed PAX documents
are held for review. The newer NAWCAD book currently requires validated OCR.

``source_name=agency_acquisition_forecasts``; ``lane=explicit_work_package_plan``;
``commercial_value_kind=planned_package_reference``. UI must display value_low/high
as a band; commercial_value is a ranking reference using a lower bound where held,
never an invented midpoint or upper bound point estimate. Company addressable value
is unknown. ``overview.planned_packages`` retains original per-package bands;
unknown lower/upper aggregate bounds remain null rather than zero.

An exact source-named prior contract resolving to one native CAGE creates
``company_entry_status=documented_incumbent_contract`` and
``match_route=exact_agency_incumbent_contract-v1``. This is a retention prospect,
not a promised award or transferable qualification. Competitors with generic
hardware similarity or restricted/sole-source/set-aside/vehicle routes stay under
review. Later published notices link by exact procurement identifiers; absence
from a later listing is not interpreted as cancellation.

Storage and operations
----------------------
Additional immutable release tables: company_product_market_coverage,
item_replenishment_context, agency_forecast_requirements, agency_forecast_incumbents,
and the original item_reference_facts binding. Existing opportunities, decisions,
company matches, value evidence, source archives and change events carry their
usual stable identities and record hashes. The complete catalogue now supports
price lookup independent of whether a request or forecast already exists.

The scoped daily native worker refreshes these derivations and official forecast
files. It cannot make held upstream DLA data newer than its ingestion pipeline.
Source-specific retrieval/OCR gaps are recorded. All new semantic and reconciliation
gates must pass before the atomic current-release pointer advances.
