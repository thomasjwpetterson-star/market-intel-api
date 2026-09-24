# Public intelligence serving release

Public company, platform and NSN pages are served from one-row-per-entity
Parquet projections. Expensive joins run once after the daily ETL, not during a
web deploy or crawler request.

## Daily command

Use the orchestrator as the scheduled job command:

```bash
python run_daily_release.py
```

It runs `run_etl.py` and then starts
`materialize_public_intelligence_release.py` in a clean process so the ETL's
large pandas frames cannot overlap the DuckDB materialisation in memory.

The materialiser defaults to 6 GB and four DuckDB threads. They can be changed
for the batch worker without changing the web service:

```bash
PUBLIC_RELEASE_DUCKDB_MEM=6GB
PUBLIC_RELEASE_DUCKDB_THREADS=4
```

## Atomic publication

Release files are uploaded under:

```text
app_cache/public_intelligence/releases/{release_id}/
```

Only after every file has uploaded and passed a size check does the job replace:

```text
app_cache/public_intelligence/current.json
```

The API downloads every referenced file to a temporary path, verifies its size
and SHA-256, and then replaces its local copy. It stages the DuckDB tables and
swaps them in one transaction. A missing or invalid release leaves the existing
request builders in service.

Set `PUBLIC_INTELLIGENCE_POINTER_KEY` to an empty value to disable prebuilt
release loading immediately. No sitemap or URL changes are required for that
rollback.

## Capacity observed in the local full-corpus rehearsal

- 833,524 manifest entities
- 214 MB compressed serving release
- 114 MB for the complete company, platform and NSN payload files
- approximately 16 minutes to build on the local full dataset
- approximately 10 seconds to load and index at a 650 MB DuckDB limit
- warm page-payload point lookups below 2 ms; first API calls in the local
  rehearsal were 7–23 ms including publication metadata

The web process does not build the full page projections. It only builds the
legacy component release when no verified prebuilt release exists, preserving
the previous behaviour during migration or rollback.
