# DLA operational commercial-metric pipeline

This release-scoped job turns typed DLA operational facts and direct DLA
solicitations into compact serving sidecars without expanding the existing
`nsn_cage_reference.parquet` relationship file:

- `nsn_supply_state_lookup.parquet`: stock, backorder, ADQ, reorder point,
  3/6/12/24-month forecast, stock cover and a deterministic supply signal.
- `nsn_price_summary_lookup.parquet`: latest observed price plus trailing
  12-month price range, median, mean, quantity-weighted price and source depth.
- `nsn_opportunity_summary_lookup.parquet`: one row per NIIN with active
  solicitation count and the next deadline, quantity and purchase request.
- `nsn_opportunity_detail.parquet`: keyed active DLA solicitation lines for
  authenticated lists, Ask Mimir evidence and exports.

Full inventory, reorder-point, forecast and price observations remain in
partitioned Parquet for Data Explorer and Ask Mimir evidence queries. Only rows
explicitly marked public-release eligible are included in the serving sidecars.
Solicitations are matched on their direct NSN field and filtered by the release
as-of date; text-search inference is not used.

The operational source repeats NIIN totals on some condition/source rows.
Consequently the NIIN-level materialization uses `MAX`, not `SUM`, for stock,
backorder, ADQ and reorder point. Source row counts and condition-code coverage
are retained for audit.

The two on-hand files can report different stock values for the same NIIN and
date. `total_stock` comes from the on-hand/backorder product, while
`reorder_assessment_stock` is retained from the on-hand/ROP product and is the
only stock value used for `below_reorder_point` and `reorder_point_gap`.
`stock_sources_agree` exposes the cross-product reconciliation result.

Example candidate build:

```bash
python3 dla_operational_v2/materialize_operational_metrics.py \
  --source-release 2026-09-16-1038 \
  --retrieval-date 2026-09-24 \
  --serving-candidate-prefix \
    s3://a-and-d-intel-lake-newaccount/mimir/nsn-enrichment-candidates/2026-09-16-1038/app_cache \
  --register-glue
```

The job writes only release-scoped candidate prefixes and release-specific Glue
candidate tables. It never changes the legacy `market_intel_silver.ops_*`
tables, live `app_cache/`, or a current release pointer.

After a successful validated build it updates only the non-production pointer
`mimir/nsn-enrichment-candidates/candidate_manifest.json`. The main ETL copies
the exact pinned sidecar versions from that manifest into its own run-scoped
candidate prefix and stamps the shared ETL run ID. Production still requires the
normal unified-manifest promotion gate.
