# Ask Mimir release-candidate checks

## Deployment contract

- Deploy the API and website as one coordinated release. PDF downloads now send the saved `request_id` and `response_id`; the API renders the owned, completed answer rather than accepting answer text from the browser.
- ZIP/CSV downloads use the same owned-answer identifiers to authorize the scope, then build the expanded evidence tables only when the customer requests the download. Professional evidence tables retain the 5,000-row-per-CSV ceiling; do not limit the pack to the small set of rows selected for the written answer. A freshly completed answer on the same release reuses its prepared company context, and the completed ZIP is cached against the stable data-release ID so repeat delivery does not rehydrate the internal export context or fail after an app-only restart.
- The website proxy reserves 300 seconds, with a 270-second upstream timeout for evidence exports and 55 seconds for ordinary API requests. Verify the deployed Vercel function supports this duration (Fluid compute, or a compatible paid plan); a 60-second legacy limit can interrupt valid cold-cache downloads.
- Keep one API process (`serve.py` already sets `workers=1`) with the existing persistent disk. The SQLite quota/result store and startup recovery are not a distributed job queue. Do not enable multiple processes or instances against the same store.
- Keep `ASK_MIMIR_ALLOW_TEST_IDENTITIES=0` and configure the trusted proxy secret on both services. Never deploy the local validation identity/secret used in workspace test scripts.
- Default admission is two research workers and eight queued requests. Rejected requests do not reserve allowance. Queue wait is bounded at 900 seconds; this does not shorten report content or reduce model reasoning effort.
- Completed answers are retained for seven days in the durable beta-state database. Refresh/retry re-authorizes each answer and does not charge it again. Unfinished requests interrupted by a process restart are refunded and may be retried using the original request ID/body.
- `/api/ask` is disabled by default. `ASK_MIMIR_ENABLE_SYNC_EVALUATION=1` is for isolated evaluation only, never production admission.
- Evidence caches are disposable, bounded and versioned. Do not clear or replace the durable beta-state database as part of rollout or rollback.
- Audit logs rotate at 20 MB with four backups (plus the active file). New research admission stops before reserving allowance when less than 128 MB of disk headroom remains. Monitor the persistent disk: these safeguards are not a substitute for sizing retained answers and the approximately 6.9 GB data release.
- Item-supplier, capability, product and competitor DLA values now come from canonical transaction records, not the older supplier lookup totals. Legacy precomputed capability/product packs are rebuilt unless they carry the new measure version. Expect extra cold-cache work on the first request.
- Variant-focused answers and CSV exports must retain the same `focus_id`. Their counts and financial totals are computed before display limits; their annual figures use the same variant-specific records. Reviewed Paladin production links also feed company exposure and the broader family view, without assigning mixed-fleet support solely to Paladin.
- Each long-lived DuckDB connection has a unique scratch subdirectory. Do not point connections at one shared spill-file directory or remove another running process's scratch files. The configured scratch root needs free space and operational monitoring as well as the persistent data disk.

## Automated gates

From this directory: `PYTHONPATH=.:tests python -m unittest discover -s tests`.

From the website repository: `node --test tests/ask-mimir-*.test.cjs`, then its production build.

The release-readiness tests cover actual generator dispatch, long-answer follow-ups, complete DuckDB execute/fetch isolation, cache mutation isolation, atomic answer/billing persistence, account-owned recovery/PDF export, admission rejection, full-universe metrics, reviewed SSC links, unavailable-outlook handling, source-link safety and spreadsheet-safe exports.

## Final hosted smoke gates

Before recommending a broader public launch, verify the deployed application revision and data release, then test an authenticated paid account through the website proxy: a detailed report, a genuine follow-up, a new-topic question, refresh during processing, PDF download and the ZIP/CSV evidence pack. Confirm that completed retries do not consume another allowance and that logout/account switching cannot recover the previous account's answers.

Measure cold-cache requests on the actual service memory/CPU allocation before increasing concurrency or public traffic. Local functional tests are not a hosted capacity result. Preserve research quality when addressing latency.
