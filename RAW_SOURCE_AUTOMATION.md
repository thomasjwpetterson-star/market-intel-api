# Raw-source automation rollout

## Implemented candidate path

This is a candidate-only first release. It automates the largest safe unit of
USAspending work without changing a production Bronze path, Silver table, Glue
Catalog entry, Main Mimir cache, Ask Mimir cache, or release pointer.

The monthly workflow:

1. asks USAspending's official bulk-download API for all prime contract
   transactions and procurement subawards for the current and prior fiscal years,
   with `columns=[]` so the publisher returns its complete CSV schemas;
2. retains each original ZIP under an immutable run prefix;
3. locates prime-transaction and subaward CSV members by required columns,
   rather than relying on filenames;
4. records every source URL, publisher date, SHA-256, full header, schema hash,
   row count and byte count in `manifest.json`;
5. transforms both datasets into isolated Parquet candidates and records
   source/output counts in `silver_manifest.json`; and
6. stops. There is no automatic production promotion.

The generated ZIP is the same full-column bulk-download product used for
historical CSV loads. The
existing daily USAspending DoD feed remains the low-latency canonical increment.
The monthly current/prior-FY run reconciles late corrections and supplies the
full subaward snapshots.

## Explicit boundaries

- USAspending remains canonical for award facts.
- SAM Contract Awards will be an enrichment layer only. It must never replace
  USAspending obligations, action rows or award totals.
- DoD contract announcements remain an independent daily Ask Mimir leading
  indicator. This change does not modify their job, schedule, manifest or cache.
- DLA bulk/reference automation is not changed while the DLA update is in progress.
- The Main Scheduler remains candidate-only. Candidate validation now bypasses
  the manual-comparison state whose control artifact candidate mode does not build;
  full shadow executions still run that comparison.

## AWS resources and isolation

`infrastructure/automated_etl_refresh.yaml` now defines the
`mimir-raw-source-candidates` state machine, its monthly Scheduler entry, a
candidate Glue transform, least-privilege Glue role, failure notification and
alarm. Both the Main and raw-source schedules default to `DISABLED`.

The new Glue role can write only below:

```text
s3://a-and-d-intel-lake-newaccount/mimir/raw-source-candidates/usaspending-contract-archive/
```

It cannot write production `bronze/usaspending/`, `silver/usaspending/`,
`app_cache/`, or any current manifest.

Each run has this contract:

```text
mimir/raw-source-candidates/usaspending-contract-archive/<run-id>/
  manifest.json
  landing/fy=<fy>/<official archive>.zip
  bronze/dataset=prime_contracts/fy=<fy>/<source csv>
  bronze/dataset=sub_contracts/fy=<fy>/<source csv>
  silver/dataset=prime_contracts/fy=<fy>/*.parquet
  silver/dataset=sub_contracts/fy=<fy>/*.parquet
  silver_manifest.json
```

The run fails closed if the response is not a ZIP, either dataset is absent, a
CSV has zero rows, required key/date columns disappear, rows report the wrong FY,
keys are blank, or deduplication produces an impossible count. It writes no
mutable `latest` or `current` pointer.

## Safe deployment sequence

1. Upload the checked-in Glue script:

   ```bash
   aws s3 cp source_automation/glue_usaspending_archive_to_candidate.py \
     s3://a-and-d-intel-lake-newaccount/glue-scripts/source_automation/glue_usaspending_archive_to_candidate.py \
     --profile new-account --region us-east-1
   ```

2. Build and push a new digest-pinned ETL image. The Dockerfile now includes
   `source_automation`.
3. Update CloudFormation with both schedules still `DISABLED`.
4. Manually start `mimir-raw-source-candidates` once and inspect both manifests.
   Confirm object versions under production Bronze, Silver, `app_cache/`, and
   both current manifests did not change.
5. Compare each candidate FY to current Silver: schema/types; distinct keys and
   duplicates; rows and totals by FY/award type; action/modified date bounds;
   UEI/CAGE/PIID coverage; company-network join coverage/fan-out; and
   representative Main/Ask queries.
6. Repeat on three publisher editions. Only then enable the raw schedule. It
   will continue to build candidates only.
7. Add promotion only after a rollback-safe Silver release boundary exists.
   Promotion must pin the old partitions, switch both FYs as one release, rebuild
   a Main candidate, pass consumer tests, and update one root pointer. Any failure
   restores the prior pointer.

## Remaining source adapters

The explicit source registry is `source_automation/catalog.py`.

- **SAM Contract Awards:** add a credentialed adapter that lands immutable pages
  and deletion state. Join by durable award IDs, measure match and one-to-many
  rates, and expose only enrichment columns. Monthly is appropriate for the
  public DoD-delayed data.
- **SAM entities:** resolve the official Public V2 URL, retain the monthly source,
  build a candidate `ref_sam_entities`, and gate UEI uniqueness, CAGE cardinality
  and company-network join coverage. Quarterly is an acceptable cost fallback.
- **DLA bulk/reference:** leave excluded until the in-progress DLA release has a
  validated candidate manifest, then schedule its monthly release workflow.
- **Budget/GAO:** annual or as-published immutable backfills, not daily gates.

## Main cadence and old Parquets

The Main schedule can run in test/candidate mode after deployment and still will
not overwrite live `app_cache/`. Candidate runs prove that older Parquets can be
rebuilt; they become live only after explicit promotion. Twice weekly is a
reasonable steady-state Main-candidate cadence for cost, independent of the
daily announcement pipeline.

## Verification

### Isolated full-load test — 2026-09-24

Execution `test-v3-20260924T0408Z` used run ID
`44175d74-0c1c-46a8-972e-2e3e28d90988` and the digest-pinned candidate image
`sha256:320e171d4e5e588e69c79e811638e6d84feef843e2460f597961ba9f6d9f1b68`.
It requested the complete FY2026 and FY2025 prime-transaction and procurement-
subaward packages in parallel. USAspending did not finish either package inside
the two-hour acquisition timeout, so the execution failed closed with:

```text
USAspending full prime/subaward downloads timed out for FYs [2025, 2026]
```

The run-scoped candidate prefix remained empty, the candidate Glue job had no
runs, and no downstream state was entered. Post-run checks matched the recorded
pre-run production baselines exactly:

- `app_cache/summary.parquet`: version
  `wH4Adr4J33jLda8wcjqPyb9wasMG_5WQ`, ETag
  `2c2cce484024bb8355da0f8ecd3d108f-30`;
- `app_cache/opportunities.parquet`: version
  `gHifZ4gjjCJvHjeggUGrgpn4faGkYtA4`, ETag
  `d51795ca93a4d703ae3ae5c26fe07e18-4`;
- `ask_mimir/runtime/current_manifest.json`: version
  `F41Hf8x7hNITmCMafGtyf4m4w.qgSKKx`, ETag
  `046cf88bff502bfb1f50a0a5062ed4d7`; and
- `mimir/runtime/current_manifest.json`: absent before and after the test.

Both Scheduler entries remained `DISABLED`. This proves the isolation and
fail-closed controls, but it does **not** yet prove a successful full ingestion.
Before another AWS run, test the lower-latency source split: use USAspending's
published full contract archive for prime transactions and request a separate
`All_Subawards` procurement export. Preserve the same per-FY manifest, schema,
row-count and no-promotion gates. Do not enable either schedule until that split
passes end to end in the candidate prefix.

```bash
python3 -m unittest -v test_source_automation.py test_etl_automation.py
aws cloudformation validate-template \
  --template-body file://infrastructure/automated_etl_refresh.yaml \
  --profile new-account --region us-east-1
```
