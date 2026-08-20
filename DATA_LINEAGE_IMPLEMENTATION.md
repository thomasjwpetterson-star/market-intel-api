# Data Lineage Implementation Order

The files in this directory are staged changes. The Athena views have been syntax-checked with `EXPLAIN`, but they have not been applied to AWS automatically.

## 1. Apply Athena views

Run each file separately in Athena, in this order:

1. `athena_00_view_flis_mgmt_current.sql`
2. `athena_01_global_spend_transactions.sql`
3. `athena_02_dashboard_master_view.sql`
4. `athena_03_ref_company_network.sql`

The first view chooses one real, internally coherent FLIS management row per NIIN. It also retains the complete set of source-of-supply codes and management organizations. It does not create a synthetic row by independently taking the maximum of unrelated fields.

The next two views append authoritative prime award and transaction identities. Existing PIIDs remain available as customer-facing contract IDs.

The company-network view uses USAspending's current-state subaward reports directly. It removes the second, coarser gold-layer deduplication and appends the source report identity and last-modified timestamp.

## 2. Rebuild incomplete FY2020 USAspending silver data

Upload the prepared Glue script:

```bash
cd /Users/tompetterson/Documents/my-saas-projects/market-intel-api

aws s3 cp glue_usaspending_fy2020_rebuild.py \
  s3://aws-glue-assets-868631722720-us-east-1/scripts/glue_usaspending_fy2020_rebuild.py \
  --profile new-account \
  --region us-east-1
```

Create the one-time job:

```bash
aws glue create-job \
  --name usaspending_fy2020_rebuild \
  --role arn:aws:iam::868631722720:role/GlueBronzeToSilverRole \
  --command Name=glueetl,ScriptLocation=s3://aws-glue-assets-868631722720-us-east-1/scripts/glue_usaspending_fy2020_rebuild.py,PythonVersion=3 \
  --glue-version 5.0 \
  --worker-type G.1X \
  --number-of-workers 10 \
  --timeout 480 \
  --profile new-account \
  --region us-east-1
```

Then start it:

```bash
aws glue start-job-run \
  --job-name usaspending_fy2020_rebuild \
  --profile new-account \
  --region us-east-1
```

The job refuses to overwrite FY2020 unless transaction keys are unique and the resulting DoD obligations are between $350B and $500B. It dynamically overwrites only the FY2020 partition.

## 3. Add the calendar-year 2025 DLA contract-history file

The downloaded file runs from January 1 through December 31, 2025. The existing normalizer recognizes `2025ContractHistory.txt`; upload the local file under that name:

```bash
aws s3 cp /Users/tompetterson/Downloads/contracthist2025.txt \
  s3://a-and-d-intel-lake-newaccount/bronze/dla/contract_history/2025ContractHistory.txt \
  --profile new-account \
  --region us-east-1
```

Run the normalizer, rebuild silver, and refresh the Glue catalog:

```bash
aws s3 cp \
  s3://aws-glue-assets-868631722720-us-east-1/scripts/dla_contract_history_rebuild_to_silver.py \
  s3://aws-glue-assets-868631722720-us-east-1/scripts/backups/dla_contract_history_rebuild_to_silver_before_lineage.py \
  --profile new-account \
  --region us-east-1

aws s3 cp glue_dla_contract_history_rebuild_to_silver.py \
  s3://aws-glue-assets-868631722720-us-east-1/scripts/dla_contract_history_rebuild_to_silver.py \
  --profile new-account \
  --region us-east-1

aws glue start-job-run \
  --job-name normalize_dla_contract_history \
  --arguments '{"--RAW_PREFIX":"bronze/dla/contract_history/"}' \
  --profile new-account \
  --region us-east-1

aws glue start-job-run \
  --job-name dla_contract_history_rebuild_to_silver \
  --profile new-account \
  --region us-east-1

aws glue start-crawler \
  --name crawl_dla_silver \
  --profile new-account \
  --region us-east-1
```

Wait for each job to succeed before starting the next one.

The revised rebuild removes only exact source-row repetitions. Quantity and price are included in the identity, and the canonical row retains the latest normalized source filename. All source snapshots and revisions remain in bronze.

## 4. Rebuild affected API caches

After the Athena and silver changes are complete, rebuild the affected Parquet files:

```bash
cd /Users/tompetterson/Documents/my-saas-projects/market-intel-api
source venv/bin/activate

AWS_PROFILE=new-account \
ONLY_FORCE_REBUILD_FILES=1 \
FORCE_REBUILD_FILES=summary.parquet,geo.parquet,profiles.parquet,risk.parquet,kpis.parquet,network.parquet,transactions.parquet,contracts_rolled.parquet,products.parquet,nsn_summary.parquet,nsn_profile_lookup.parquet,nsn_supplier_lookup.parquet,nsn_cage_reference.parquet,platform_bom.parquet \
python run_etl.py
```

The broad targeted list is deliberate: FY2020 prime coverage and FY2025 DLA history affect financial rollups, while the identity, FLIS, subaward, and platform changes affect their respective sidecars.

## 5. Validation

After the rebuild, confirm:

```sql
SELECT
  action_date_fiscal_year AS fiscal_year,
  COUNT(*) AS rows,
  COUNT(DISTINCT contract_transaction_unique_key) AS transaction_keys,
  SUM(TRY_CAST(federal_action_obligation AS DOUBLE)) AS obligations
FROM market_intel_silver.dataset_prime_contracts
WHERE action_date_fiscal_year = '2020'
GROUP BY 1;
```

```sql
SELECT
  source_system,
  COUNT(*) AS rows,
  COUNT(DISTINCT transaction_key) AS transaction_keys,
  COUNT_IF(transaction_key IS NULL OR TRIM(transaction_key) = '') AS missing_transaction_keys,
  SUM(spend_amount) AS spend
FROM market_intel_gold.global_spend_transactions
GROUP BY 1;
```

```sql
SELECT
  COUNT(*) AS rows,
  COUNT(DISTINCT source_report_id) AS source_reports,
  COUNT_IF(source_report_id IS NULL OR TRIM(source_report_id) = '') AS missing_source_reports
FROM market_intel_gold.ref_company_network;
```

```sql
SELECT
  COUNT(*) AS bridge_rows,
  COUNT(DISTINCT niin) AS mapped_niins,
  COUNT(DISTINCT CONCAT(platform_family, '|', niin)) AS distinct_platform_niin_pairs
FROM market_intel_silver.ref_platform_map p
JOIN market_intel_silver.ref_wsdc w
  ON TRIM(CAST(w.wsdc_code AS VARCHAR)) = TRIM(CAST(p.wsdc_code_ref AS VARCHAR))
WHERE p.platform_family IS NOT NULL
  AND w.niin IS NOT NULL;
```
