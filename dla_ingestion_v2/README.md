# DLA Monthly Procurement Ingestion v2

This candidate pipeline uses the monthly procurement-history file as the financial
backbone and the matching contract-history file for item descriptions and part-number
references. It does not replace any production view until the candidate checks pass.

## Financial Grain

- One financial row is retained for each distinct DLA procurement line.
- Repeated contract-history rows never multiply quantity or value.
- A part number receives an observed procurement association only when the financial
  line has exactly one distinct reported part-number reference.
- Lines with multiple part-number references remain financially attributable to the
  NIIN and recipient CAGE, while all references are retained in the sidecar table.
- `observed_value` is DLA order quantity multiplied by DLA net price. It should not be
  labelled as supplier revenue.

## August 17, 2026 Source Validation

| Measure | Result |
| --- | ---: |
| Physical rows in each source file | 713,274 |
| Unique procurement lines | 597,099 |
| Contract-only or procurement-only keys | 0 |
| Unique procurement-line value | $6,612,421,074.02 |
| Lines with one part-number reference | 553,486 |
| Value on one-part-reference lines | $5,549,553,646.26 |
| Lines with multiple part-number references | 43,613 |
| Value on multi-reference lines | $1,062,867,427.76 |

The raw contract-history expansion totals about $9.06B if summed directly. That is
about $2.44B too high because the same procurement line is repeated for its reference
numbers.

## First Candidate Run

Upload the immutable source ZIPs and their extracted text members:

```bash
cd /Users/tompetterson/Documents/ChatGPT/Mimir/dla-ingestion-v2

AWS_PROFILE=new-account ./upload_dla_monthly_snapshot.sh \
  2026-08-17 \
  /Users/tompetterson/Downloads/2026-08-17-1347_contracthist.zip \
  /Users/tompetterson/Downloads/2026-08-17-1347_prochist.zip
```

Upload the Glue script and create the isolated v2 job:

```bash
aws s3 cp glue_dla_monthly_procurement_snapshot.py \
  s3://aws-glue-assets-868631722720-us-east-1/scripts/glue_dla_monthly_procurement_snapshot.py \
  --profile new-account --region us-east-1

aws glue create-job \
  --name dla_monthly_procurement_snapshot_v2 \
  --role arn:aws:iam::868631722720:role/GlueBronzeToSilverRole \
  --command Name=glueetl,ScriptLocation=s3://aws-glue-assets-868631722720-us-east-1/scripts/glue_dla_monthly_procurement_snapshot.py,PythonVersion=3 \
  --glue-version 5.0 \
  --worker-type G.1X \
  --number-of-workers 5 \
  --timeout 240 \
  --profile new-account --region us-east-1
```

Run it against the extracted snapshot:

```bash
aws glue start-job-run \
  --job-name dla_monthly_procurement_snapshot_v2 \
  --arguments '{
    "--SNAPSHOT_DATE":"2026-08-17",
    "--CONTRACTHIST_PATH":"s3://a-and-d-intel-lake-newaccount/bronze/dla/monthly_procurement/source_snapshot_date=2026-08-17/contracthist/",
    "--PROCHIST_PATH":"s3://a-and-d-intel-lake-newaccount/bronze/dla/monthly_procurement/source_snapshot_date=2026-08-17/prochist/"
  }' \
  --profile new-account --region us-east-1
```

## Athena Candidate Setup

Run each SQL file separately and in numeric order. Athena accepts one statement per
execution:

1. `athena_01_create_fact_procurement_history_v2.sql`
2. `athena_02_create_fact_contract_history_part_references_v2.sql`
3. `athena_03_view_dla_contract_history_financial_v2_candidate.sql`
4. `athena_04_view_dla_contract_history_part_references_v2_candidate.sql`
5. `athena_05_view_dla_part_number_procurement_observed_v2_candidate.sql`
6. Run validation files `athena_06` through `athena_09`, separately.

The candidate must leave 2019-2025 unchanged, replace the incomplete calendar-2026
slice rather than append to it, reconcile all line keys, and produce zero differences
in `athena_09` before any production-view promotion.

## Promotion And Rollback

After every validation passes:

1. Run `athena_11_create_legacy_financial_view_20260910.sql` and confirm it matches
   the current production view.
2. Run `athena_12_promote_financial_view_v2.sql` to switch the existing view name to
   the latest validated monthly snapshot for each calendar year.
3. If a downstream check fails, run `athena_13_rollback_financial_view.sql`.

After promotion, `athena_15` creates the safe single-part observed-procurement view
and `athena_16` creates the complete part-reference history view. The former contains
financial values only where one procurement line has one reported part reference;
the latter carries all references and no financial value.

The promotion preserves the exact production schema. Existing consumers continue to
read `market_intel_silver.view_dla_contract_history_financial` without code changes.
