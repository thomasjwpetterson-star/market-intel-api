Prepared source coverage completion — additive interface
=======================================================

Not deployed; additional source-read approval remains required. This branch
includes the promoted-Main catalogue connector described in
COMPANY-REFERENCE-READ-ACCESS.rst and the historical purchase recovery described in
HISTORICAL-PURCHASE-READ-ACCESS.rst. Existing routes, company IDs and episode IDs
remain compatible; no schema migration is needed.

New input: sources.historical_item_purchase_lines. It retains a sixteen-month
window of immutable dated financial observations so rolling twelve-month and
reporting-lag comparisons can each use the appropriate dates. Original object
versions, source SHA256, PO/line, contract number and raw archives remain linked.
Repeated/ambiguous parts do not multiply cash or gain arbitrary exact attribution.
Known annual-source corrections replace older observations from that source.

Catalogue-sales evidence may add contract_number; it remains null when not held.
Its sixteen-month retained population is distinct from company_catalogue_sales,
which remains a trailing twelve-month summary. company_product_market_coverage
continues to use its declared comparison period. Source-market values must not be
added to manufacturer revenue, forecasts or qualified company pipeline.

No new future-opportunity lane or price estimate is introduced by this history.
The existing price/quantity calculation for future demand uses current sources.
New release gates check physical order-line uniqueness across retained snapshots
and exact reconciliation of the trailing-year sales summary to its own period.

The worker checks the existing held 2025 file by exact S3 object version. It also
retains already-observed monthly prices when monthly sources roll into a new
year. It writes only its already-owned opportunity namespace. A denied first
read cannot publish a partial result; a later failed check retains complete
history and reports failed-check health with original transaction dates.

Activation must use a full native refresh, since a newly promoted catalogue can
introduce CAGEs not yet present in evidence-bearing company profiles. Validate
those identities, company qualification routes, source versions, derived-market
periods and ordinary publication/readback gates before advancing the live pointer.
