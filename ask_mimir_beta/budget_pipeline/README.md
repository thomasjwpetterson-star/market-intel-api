# FYDP budget pipeline

This directory owns the reproducible source-to-reference build for Ask Mimir's
FY2027 program outlooks.

1. `ingest_fydp_sources.py` downloads, validates, hashes and archives the
   public component justification books listed in `fydp_source_manifest.json`.
2. `normalize_fydp_justification_books.py` converts first-page P-40 resource
   summaries into non-additive, status-preserving facts for FY2025-FY2031.
3. `validate_fydp_coverage.py` fails a release if a governed source disappears,
   any explicit platform linkage stops matching a budget line, or any mapped
   aircraft family loses published FY2028-FY2031 values.

Example:

```bash
python -m ask_mimir_beta.budget_pipeline.ingest_fydp_sources \
  --download-dir /tmp/mimir-fydp-sources --profile new-account
python -m ask_mimir_beta.budget_pipeline.normalize_fydp_justification_books \
  --input-dir /tmp/mimir-fydp-sources \
  --manifest ask_mimir_beta/budget_pipeline/fydp_source_manifest.json \
  --output-dir /tmp/mimir-fydp-normalized
python -m ask_mimir_beta.budget_pipeline.validate_fydp_coverage \
  --facts /tmp/mimir-fydp-normalized/dod_fydp_budget_facts.parquet
```

The Navy FY2027 BA5 aircraft-modification volume is intentionally listed as a
later source until the official host permits repeatable retrieval or a verified
public archive copy is available. Navy new-aircraft procurement and support
coverage come from BA1-4 and BA6-7 respectively.
