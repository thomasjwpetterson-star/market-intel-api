# PSC-filtered dashboard Risk KPI failure

Investigated 3 November 2026. Fix prepared against API `main` commit
`43f050d77f69563d7f5f6a3e4ad6e9228bc2f4ca`.

## Findings

`get_recompete_kpi` applied `build_summary_where` directly to `v_risk`.
Every nonempty PSC filter added predicates on `psc_code` and
`psc_description`, but the Risk sidecar projects `psc`. The resulting DuckDB
binder exception was caught and returned as `recompete_risk.value = "N/A"`
and `sub_label = "No Data"`. `/api/dashboard/kpis` consequently returned 200.
Spend and contract totals came from a separate successful summary query.

This is the same defect investigated in the chat **Fix psc=Other Risk KPI
failure** on 17 October. The earlier repair and five tests remain in
`risk-kpi-psc-api`; its `main.py` is still an uncommitted local modification.
The earlier chat explicitly said it was undeployed. Current GitHub main still
has the faulty query and fallback, so the evidence supports a missed release.

Render logs independently confirm 30 failures in the 3 November
02:51–03:31 UTC window. The incident report also lists 89 on 29–30 October
UTC; that historical count has not been independently rechecked here.
The user confirmed workspace `tea-d59j9l2li9vc73alp840` and authorized
deployment. The live service is `srv-d59jle1r0fns73frev1g`, serving baseline
commit `43f050d77f69563d7f5f6a3e4ad6e9228bc2f4ca` before this repair.
A single read-only production request, labeled with user agent
`Mimir-KPI-Investigation/2026-11-03`, for `psc=1510` returned 200 and `N/A`.
That response alone does not distinguish a failed calculation from valid
empty data. Actual customer exposure remains unconfirmed.

## Repair

Port the earlier fix to current main. Apply non-PSC filters as before, match
PSC codes against `v_risk.psc`, and resolve description matches to codes
through `v_summary`. Keep values parameterized and wildcard characters
escaped. Use `IN` membership so repeated summary rows cannot multiply Risk
spend. Preserve the award-key/legacy-contract-ID selection and 90-day window.

Log calculation and schema inspection errors with a traceback and raise
HTTP 500 with `{"detail":"Risk KPI calculation failed"}`. A successful
query without matching data continues to return 200 and `No Data`.

The chart's generated **Other** remainder still becomes a literal text
filter. This separate pre-existing selection issue is outside this query
repair. Risk continues to ignore the fiscal-year selection, as before.

## Validation

The earlier regression suite fails against current main for normal PSC
codes/descriptions, Other, combined filters and exception propagation.

The repaired handler passes ten tests using in-memory DuckDB and a real
FastAPI TestClient. Coverage includes HTTP 200/500 behavior, code and
description matching, Other, empty/null PSC, combined filters, escaped input,
valid empty data, schema inspection failures, legacy contract IDs, distinct
award counts and the inclusive 0–90-day completion window.

Separate baseline comparisons confirm unchanged spend and contract totals
across seven filter cases and identical unfiltered, null-PSC and empty-PSC
responses. Syntax compilation and `git diff --check` pass. An optional
comparison against the 16 October pinned parquet snapshot stalled while
reading old local files and was stopped; it is not counted as validation.

Run with the API dependencies and the test-only `httpx` package installed:

```sh
python -m unittest -v test_dashboard_kpi_psc.py
```

## Release status and remaining verification

Prepared on `codex/risk-kpi-psc-recurrence`. The user authorized deployment
on 3 November. Production follows `main` with automatic deployment on
commits; pushing the tested repair will trigger the release. Preserve
concurrent main-branch work and verify labeled PSC requests and corresponding
application/request logs after the deployment becomes live. Check that new
Risk calculation failures cannot appear alongside a 200. Deployment results
will be recorded after the rollout.
