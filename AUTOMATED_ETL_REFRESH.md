# Automated ETL refresh: first implementation slice

## Outcome

This implementation moves the existing local refresh toward AWS without allowing an automated run to promote itself into production.

The workflow is:

1. Fail closed unless the latest USAspending prime/silver, DLA solicitation, DoD announcement, and SAM daily workflows succeeded within 36 hours, and the primary Athena serving-view chain can be analyzed successfully.
2. Seed an isolated manual control with the current live summary/network versions.
3. Start the existing monolithic manual-control build and the automated core build in parallel, reducing comparison skew from mutable upstream sources.
4. After the automated core build, build `profiles.parquet` in a dependent task so it consumes the same run's `summary.parquet` and `network.parquet`.
5. Require every generated Parquet to expose an ETL run ID, row count, schema fingerprint, content hash, S3 version ID, and timestamp.
6. Publish an immutable main-platform manifest and `mimir/runtime/candidate_manifest.json`.
7. Compare all 18 generated artifacts and deeply scan the 11 highest-risk datasets for schema, row, key-cardinality, null, and numeric aggregate changes. Record the generation lag between each manual/automated pair so live-source drift can be separated from transformation drift.
8. Publish an Ask Mimir candidate using its existing atomic runtime-release mechanism and the same staged objects.
9. Stop. Neither current manifest is changed, no live `app_cache/` object is overwritten, and no Render deploy hook is called.

This makes the first AWS runs true shadow runs rather than production updates.

## Files

- `etl_automation/orchestrator.py` runs one stage or the complete local workflow.
- `etl_automation/release.py` validates exact S3 versions and writes the main candidate manifest.
- `etl_automation/platform_release.py` defines and tests the single root manifest that will bind main Mimir, public intelligence, and Ask Mimir to one ETL run and one rollback pointer.
- `etl_automation/metadata.py` creates the validation metadata attached by `run_etl.py`.
- `Dockerfile.etl` creates the common ECS image.
- `infrastructure/automated_etl_refresh.yaml` provisions Fargate, Step Functions, Scheduler, IAM, logs, an SNS alert topic, an alarm, and a scheduler DLQ.

## Current AWS shadow status (2026-09-23)

The candidate-building workflow is deployed and working in AWS. Manual executions default to the full comparison shadow. The EventBridge Scheduler target is explicitly `{"mode":"candidate"}`, which omits the duplicate manual seed/control and comparison tasks; its schedule `mimir-etl-refresh-daily` remains `DISABLED`. Neither platform has been promoted.

Before the first shadow, the live `global_spend_transactions` Athena view was found invalid because its stored `unit_price_status` width no longer matched its dependency. The transaction and dashboard-master definitions also represented two partially applied feature branches: unit-pricing fields were present in one path and platform-attribution fields in the other. The checked-in SQL now preserves both field families, widens `unit_price_status` to `varchar(64)`, and has been safely applied to the live transaction/master chain. The source, master, summary, and seven direct downstream consumers were probed after repair with unchanged downstream schemas. Exact rollback definitions are pinned at:

```text
s3://a-and-d-intel-lake-newaccount/mimir/change-control/2026-09-23-global-spend-merged-repair-v2/athena-view-repair/manifest.json
```

Three full shadow executions have completed:

- Run `4f9702fe-180a-41e7-b388-118e54d0fc9b` succeeded using the initial sequential control. It found no schema breaks. Four data deltas were attributable to a 34–35 minute mutable-source gap plus the manual profile dependency ordering described below.
- Run `bce5e14e-5533-4b97-9c1a-6d7be8b7d933` succeeded after changing the manual and automated core builds to run in parallel. Fourteen artifacts were `CONTENT_ONLY`, with matching schemas and row counts; the deeply compared members of that group also had matching aggregate/key metrics. There were no schema breaks. The remaining four review items were:
  - `products.parquet`: automated had 1,530 additional rows (+0.134%) across a 79-second output gap.
  - `nsn_cage_reference.parquet`: automated had 78 fewer rows (-0.00045%) across a 104-second output gap.
  - `opportunities.parquet`: row count and schema matched; four NAICS values changed across a 133-second output gap.
  - `profiles.parquet`: row count and schema matched; automated had 320 more contracts and $3,189,003.99 more lifetime spend (+0.0000953%). This same delta occurred in both shadows because the current manual sequence builds profiles from the previously live summary/network files, while the automated dependent phase intentionally uses the freshly generated files from the same run.
- Run `4aa34dd9-f060-4945-981c-8679769b0de1` succeeded in parallel after the transient-retry and workgroup controls were deployed. All 18 schemas matched. Fourteen artifacts again had matching row counts and deep metrics. Across 250-271 seconds of generation skew, `products.parquet` had 847 fewer rows (-0.0740%), `nsn_cage_reference.parquet` had 86 fewer rows (-0.00049%), and `opportunities.parquet` had 112 fewer rows (-0.554%). `profiles.parquet` had the same accepted 320-contract/$3,189,003.99 correction, with an identical row count. No retry was required during this successful run.

The second report is immutable at:

```text
s3://a-and-d-intel-lake-newaccount/mimir/comparisons/bce5e14e-5533-4b97-9c1a-6d7be8b7d933/comparison_report.json
```

The third report is immutable at:

```text
s3://a-and-d-intel-lake-newaccount/mimir/comparisons/4aa34dd9-f060-4945-981c-8679769b0de1/comparison_report.json
```

The parallel run reduced mutable-source comparison skew from roughly 35 minutes to 79–133 seconds. Because both branches execute the same `run_etl.py` image and differ only in isolated output prefixes and phase selection, the remaining product/reference/opportunity differences are input timing effects rather than divergent transform code. They still require a promotion policy: either create a run-scoped input snapshot or accept bounded drift thresholds and compare a candidate to a fixed prior release.

The deployed build-7 image is pinned at digest `sha256:0b1e5fcd56062f13a12eba3ee2a5b5e653e0a971ccba40e0bababe612ad5d433` and has no critical ECR scan findings. It currently has two high findings in OS packages (`zlib` CVE-2026-85091 and `perl` CVE-2026-82560), plus one medium and one low finding. Debian's security tracker lists both findings as unfixed across its current releases as of 2026-09-23, so an `apt upgrade` or base-image switch cannot yet remove them. The specific Perl issue requires formatting attacker-controlled POD documentation, which this task does not do; the zlib issue requires a particular non-blocking `gzwrite` stall path. The task has no ingress and is short-lived, so these are documented residual risks rather than a reason to expose production data. Rebuild and rescan the image when Debian publishes fixed packages; any critical finding remains a hard deployment block.

The profile consistency change is accepted: `profiles.parquet` should use `summary.parquet` and `network.parquet` from the same ETL run. The small, repeatable increase relative to the manual process is therefore expected correction, not a regression.

The mutable-input policy is deliberately conservative. `products.parquet`, `nsn_cage_reference.parquet`, and `opportunities.parquet` are fed by sources that may change while a refresh is running. A schema change or an unexplained delta always blocks promotion. A small timing-correlated delta may be labelled source drift during shadow testing, but it still requires review and cannot authorize an unattended production promotion. Automated promotion should remain disabled until either the inputs are snapshotted for the duration of a run or explicit, tested business thresholds replace this manual gate.

Current decision: **go for scheduled candidate-only builds once retention is in place; no-go for unattended production promotion**. The third representative parallel shadow has passed. Promotion remains blocked until a mutable-input gate is enforceable by the workflow and the main platform consumes an atomic versioned manifest with a tested rollback path.

### Public-page retention invariant (2026-09-24)

An entity missing from one refresh is not a removal instruction. The public release builder now carries every previously published manifest entry into the next staged release, preserves its truthful `last_modified` date, retains the last-known-good release-owned projection modules at entity granularity, rejects duplicate entity keys, and keeps the previous release pointer if staging fails. There is intentionally no automatic age-based, cohort-rank-based, or "not refreshed" de-indexing path.

The prior public manifest and all seven public projections are now persisted as immutable, version-pinned release artifacts. Rehearsal `cold-start-20260924-129c489dbe` rebuilt release `public-intelligence-20260924T005335Z`, downloaded it into an empty Fargate filesystem by exact S3 version ID, and compared every row and schema with the manual baseline. All seven schemas matched exactly, all seven key sets were unique, and the semantic manifest comparison found 3,516 additions, 131,279 changed content fingerprints, 702,245 unchanged fingerprints, and **zero removed published entities**. The row changes are expected refreshed data rather than schema drift; the full `EXCEPT ALL` counts and hashes are retained in `s3://a-and-d-intel-lake-newaccount/mimir/rehearsals/cold-start-20260924-129c489dbe/reports/cold-start-rollback-report.json`.

The same rehearsal verified that the immutable Ask Mimir release references the exact S3 keys, versions, and hashes used by main Mimir for all 12 shared serving artifacts. It then assembled a same-run platform root, promoted it through a rehearsal-only current pointer, and restored the prior immutable root byte-for-byte. Production was not addressed: `mimir/platform/current_manifest.json` remains absent, the existing main and Ask pointers retain their original version IDs, and Scheduler remains disabled in candidate-only mode.

The platform-root contract therefore has both unit and AWS cold-start coverage for same-run validation, mixed-release rejection, candidate-only publication, a one-object pointer update, and exact rollback. Consumer wiring is covered by the rehearsal below; production promotion remains blocked until the mutable-input gate is enforced and a current same-run candidate has passed the consumer smoke suite. The rehearsal proves the release mechanism; it does not authorize an unattended production cutover.

### Atomic consumer rehearsal (2026-09-24)

The three consumers are now wired behind the default-off `MIMIR_USE_PLATFORM_MANIFEST` flag. Main Mimir downloads every main artifact by exact S3 version, verifies its recorded size and hash, and installs the immutable public tables only after their hashes, schemas, and row counts pass. The public API therefore uses the public child referenced by the same root instead of rebuilding a new publication cohort at web-process startup. Ask Mimir resolves its runtime child through the same root and fails closed on a missing component, mutable child pointer, mismatched release ID, or mismatched ETL run. Legacy serving behavior remains the default and no production configuration has been changed.

Fargate task `ce2374c545a648f5a4c2c2ec252b8d1c` cold-started all three consumers from rehearsal pointer version `CBzZaB.0HHre2b1h1CwO7Tw37Msej2HG` and exited successfully. It verified 19 main files, exact declared row counts, 837,040 public manifest entries, publishable company/platform/NSN/award/solicitation pages, public search, 169 Ask Mimir files, and Ask Mimir health. The final run made no Athena queries and reported no live-serving mutations. Evidence is stored at `s3://a-and-d-intel-lake-newaccount/mimir/rehearsals/cold-start-20260924-129c489dbe/reports/atomic-consumer-smoke-report.json` (VersionId `Xu.eZJCR7lCJk5NYO6T0uqWsW09OpYeX`).

The live platform pointer is still absent, the public and Ask current pointers are unchanged, and Scheduler remains disabled. A newer, non-live NSN-enrichment main candidate (`nsn-enrichment-20260924T013900Z-build14`) appeared before that consumer smoke run and contains 21 files. It was intentionally not mixed into the older tested root because no matching Public and Ask children existed at that point. The following rehearsal satisfies that same-run requirement without changing production.

### Aligned 21-file candidate rehearsal (2026-09-24)

The incremental main candidate is now bound to matching Public and Ask children under the isolated prefix `mimir/rehearsals/aligned-platform-20260924-nsn-build14/`. The Ask publisher now receives `ETL_AUTOMATION_RUN_ID` from the orchestrator, so future candidate manifests record their lineage directly. The aligned Ask release `ask-mimir-beta-20260924T075315Z-1bf9be3103fd` pins 171 files and exactly matches main on 14 shared serving artifacts, including both operational NSN sidecars.

The first Public attempt was rejected before a platform root was assembled. It accidentally reused the older manual snapshot as its carry-forward baseline and produced 837,025 entries, 15 fewer than the previously accepted 837,040-entry rehearsal release. No live object was involved. The task-registration tooling now requires an explicit baseline prefix so that an older snapshot cannot be selected silently. Rebuilding from the latest accepted immutable Public release produced `public-intelligence-20260924T081924Z` with 837,795 entries. Exact comparison of all seven tables found identical schemas, unique key sets, 755 added entity keys, and **zero removed entity keys**. Content fingerprints were unchanged for 709,014 entities and updated for 128,026 entities. The isolated platform-pointer promotion and rollback remained byte-for-byte exact. Evidence is stored at `s3://a-and-d-intel-lake-newaccount/mimir/rehearsals/aligned-platform-20260924-nsn-build14/reports/cold-start-rollback-report.json` (VersionId `ruru7jcRUnzEMzeJ6gPZrxgdoZMHoY3.`).

Fargate task `5d000606efc446a5b4740660acfbdbdb` then cold-started Main, Public, and Ask from rehearsal root version `taFCyTrxLUwXTciY1P6rAuu9uQAgZvFQ` and exited successfully. Main loaded 21 version-pinned files and passed DuckDB, geographic, and profile readiness checks. Public returned publishable company, platform, NSN, award, and solicitation samples plus search. Ask verified all 171 files and returned healthy status. The report records no live-serving mutations and is stored at `s3://a-and-d-intel-lake-newaccount/mimir/rehearsals/aligned-platform-20260924-nsn-build14/reports/atomic-consumer-smoke-report.json` (VersionId `27mR4hU47j1IN0ywPtQC8cNCuqrLLhkK`).

The tested image is pinned at digest `sha256:71467beb026991125078ffb6dcad6537b8125309eebabdf22ad8309edd3faed0`. Its scan is complete with no critical findings and the same two documented high, one medium, and one low Debian findings. Scheduler remains disabled, the production platform root remains absent, the existing Ask current pointer remains on VersionId `F41Hf8x7hNITmCMafGtyf4m4w.qgSKKx`, and no deploy hook or live cache path was invoked.

## Measured AWS cost implications

The third parallel shadow confirms the usage-based estimate:

- Athena ran 37 successful queries and scanned 395,804,921,590 bytes. Using AWS's decimal TB pricing convention, this is approximately **$1.98** at $5/TB (before per-query rounding).
- The eight 4-vCPU/16-GB Fargate tasks used 4,813 aggregate task-seconds. CPU, memory, 80 GB/task of chargeable ephemeral storage, and public IPv4 total approximately **$0.33**.
- One main candidate is 6.33 GiB. At S3 Standard's first-tier rate, each candidate retained for a full month is approximately **$0.15/month**. Daily candidates retained for seven days average about 44 GiB, or roughly **$1/month**. Shadow controls double that temporary footprint.
- Step Functions transitions, manifests, S3 requests, CloudWatch log ingestion, and the 204 MiB ECR image are small relative to Athena and should normally remain well below $1/month each at this cadence.

Do not schedule shadow mode as the permanent daily production path: it intentionally runs both the manual and automated build. A production candidate refresh without `seed-manual`, `manual-control`, and `compare` is estimated at **$1.15-$1.25/run**, or approximately **$35-$38/month** when run daily, plus roughly $1-$5/month depending on staging retention. Keeping a full comparison shadow every day would instead be approximately **$69/month** before storage; running one weekly adds roughly $10/month. Athena scan growth is the main cost risk and should have a workgroup byte limit and billing alarm before scheduling.

The scheduled candidate mode now provides that separation. The deployed immutable image uses a dedicated `mimir-etl-refresh` Athena workgroup with CloudWatch query metrics and a 64 GiB per-query scan cutoff. The largest query in the measured shadow scanned 35.9 GiB. A second alarm fires if the workgroup scans more than 500 GB in six hours. The schedule remains disabled pending safe run-aware retention and the explicit decision to begin unattended candidate generation.

The first attempt at the third shadow correctly failed closed when Athena returned a transient `HIVE_S3_THROTTLING`/S3 503 during the network UNLOAD. It did not modify production. Build 6 adds bounded query-level recovery: only known transient S3 throttling/503 and Athena internal failures retry, with at most three attempts and 5-/10-second backoff; partial UNLOAD objects are removed before a retry. SQL, schema, permission, and other deterministic failures still stop immediately.

## Recommended rollout

### 1. Create the image repository, then build and push

The current account has only the unrelated mutable `sam-downloader` and `dibbs-downloader` repositories. Deploy `infrastructure/etl_image_repository.yaml` to create the dedicated `mimir-etl-refresh` repository with immutable tags, scan-on-push, retention of the newest 20 images, and a least-privilege CodeBuild project.

Upload the source bundle to `s3://a-and-d-intel-lake-newaccount/mimir/build-sources/mimir-etl-refresh.zip`, start the `mimir-etl-refresh` CodeBuild project, and pass its digest-pinned image URI to the main CloudFormation stack. The build uses immutable `build-<number>` tags; do not use a mutable `latest` tag for a scheduled data pipeline.

### 2. Deploy with the schedule disabled

The template defaults `ScheduleState` to `DISABLED`. The current account has one default VPC (`vpc-03918a9b62205e952`) and six public subnets, with no private-subnet/NAT path visible. The template therefore defaults `AssignPublicIp` to `ENABLED` and creates a dedicated security group with no ingress and HTTPS-only egress. Use at least two of the public subnets in different availability zones for the first shadow deployment. A later network-hardening pass can move the task into private subnets with VPC endpoints or NAT and set public IP assignment to disabled.

The ECS task role is scoped to the one data bucket. It can read the bucket because Athena queries need access to the underlying tables; writes are limited to ETL temporary data and Mimir/Ask Mimir release prefixes.

### 3. Run three manual shadow executions

Start the Step Functions state machine manually on three representative days. For each run, verify:

- the preflight, manual control, automated build, comparison, and candidate-publication states complete;
- the main candidate includes 21 pinned objects and one ETL run ID for all 20 generated objects, including both DLA operational sidecars;
- `profiles.parquet` is newer than both summary and network;
- Ask Mimir's candidate points to the same run-scoped staging prefix;
- the existing production manifests and `app_cache/` versions did not change;
- duration, Athena scan volume, Fargate memory, and temporary S3 growth are acceptable.
- `mimir/comparisons/<run-id>/comparison_report.json` has no unexplained schema or aggregate deltas.

### 4. Add explicit promotion

Promotion should be a separate state machine or manually approved action. It must:

- revalidate the candidate's exact S3 versions;
- copy or repoint the main platform atomically;
- promote Ask Mimir's existing candidate pointer;
- trigger the two Render deployments;
- smoke-test both services;
- roll back the pointers if either smoke test fails.

The main platform currently reads `app_cache/` directly, so its consumer must be changed to load a versioned release manifest before fully atomic promotion is possible. Until that change lands, do not automate the production cutover.

### 5. Enable the daily schedule

Enable Scheduler only after shadow runs and a tested promotion/rollback path. Keep candidate building and production promotion separately observable even if promotion later becomes automatic.

## Local verification

Run the unit tests without AWS access:

```bash
python3 -m unittest -v test_etl_automation.py
```

The normal candidate-only command uses real AWS data and writes only to its run-scoped staging prefix:

```bash
python3 -m etl_automation.orchestrator --stage all
```

Use `--stage comparison-all` to reproduce the complete one-time manual-control comparison. Individual stages are `preflight`, `seed-manual`, `manual-control`, `core`, `dependents`, `validate-main`, `compare`, and `publish-ask`. Reuse the same `--run-id` and `--run-started-at` for all stages.

## Operational implications

- The laptop is no longer a runtime dependency; AWS credentials come from the ECS task role.
- Failed builds leave the production pointers untouched. Run-scoped staging data can be retained briefly for diagnosis and expired by an S3 lifecycle rule.
- A failed or stale daily upstream blocks the refresh. Monthly DLA procurement and FLIS/reference pathways still require separately agreed freshness windows before they can be made hard gates.
- Fargate and Athena add per-run cost. The dominant variables are Athena bytes scanned, task duration, and 100 GiB task ephemeral storage.
- A single refresh now performs two `run_etl.py` passes. The second is targeted to profiles and reuses every other staged file, trading a small amount of startup time for correct dependency ordering.
- The schedule and promotion are deliberately separate. This adds one rollout step but gives a clean rollback boundary and prevents partial cache generations from becoming live.
- The current broad human/Glue policies can be retired only after all remaining manual ingestion pathways are moved behind dedicated roles; this stack does not change those existing roles.

## DLA operational enrichment candidate (2026-09-24)

The DLA operational release `2026-09-16-1038` has been refreshed into typed, versioned Bronze/Silver candidates and reduced to two application-side lookups: `nsn_supply_state_lookup.parquet` and `nsn_price_summary_lookup.parquet`. The sidecars are 19.4 MB and 9.0 MB respectively, so they preserve the existing DuckDB/Parquet serving model without loading the 15.8 million inventory rows or 16.7 million reorder rows into each application process.

`stage_operational_sidecars` reads the non-production operational candidate manifest, validates each exact S3 version and SHA-256 digest, and copies the objects into the main ETL run's staging prefix with the shared `etl-run-id`. The main release validator then treats them as normal generated artifacts. They are optional inputs to preserve compatibility, but a candidate that declares them cannot silently substitute a different version.

The authenticated NSN profile and Ask Mimir dossier consume the compact files directly. Data Explorer exposes the metrics as additional selectable columns on its existing NIIN/part/CAGE reference view, using a filtered lazy join rather than physically duplicating NIIN values across 17.5 million relationship rows. Public NSN payloads expose only a dated demand/supply teaser, while exact quantities, longer forecast windows and observed-price benchmarks remain conversion-oriented authenticated features. Full operational histories stay partitioned in S3/Glue for Athena-backed evidence and future trend calculations.

The live `app_cache/` prefix and production release pointer remain unchanged. The scheduled candidate job remains disabled; this enrichment does not alter the existing no-go decision for unattended production promotion.
