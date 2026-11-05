# Private MCP accounting recovery

This runbook covers only the new research-session accounting tables in the existing Ask SQLite file. It does not back up Supabase accounts, OAuth, plans, Storage objects, Ask conversations, or the intelligence lake. No backup destination or cloud lifecycle rule is activated by installing this code.

The existing lifecycle monitor runs the 90-day accounting cleanup hourly when the research-session pilot flag is enabled. A fixed completion/failure message contains no customer identifiers. On-access cleanup remains in place. Other Ask retention is unchanged.

## Snapshot and restore

Run from the API repository root on a trusted operator host. Confirm the actual `ASK_MIMIR_BETA_STATE` file from the running service; do not guess a path. Create a private temporary directory with `umask 077`. The utility creates new files with mode 0600 and refuses to overwrite any existing destination.

```sh
python -m ask_mimir_beta.research_recovery snapshot \
  --source "$ASK_MIMIR_BETA_STATE" \
  --destination "$MIMIR_RECOVERY_DIR/accounting.sqlite"

python -m ask_mimir_beta.research_recovery restore \
  --source "$MIMIR_RECOVERY_DIR/accounting.sqlite" \
  --destination "$MIMIR_RECOVERY_DIR/isolated-restored.sqlite" \
  --exclusions "$MIMIR_RECOVERY_DIR/current-exclusions.json"
```

The exclusion file contains `generated_at` as an ISO timestamp with time zone and `owners` as the verified accounting identities whose data must not return. Reconcile it against current account deletion and revocation instructions, generate it within the preceding 24 hours and protect it as customer metadata. An explicit, checked empty list is allowed; omitting the file or using a stale list is rejected.

Restoration preserves successful session charges, ends old active windows and marks pending calls failed. It never replays research jobs. It creates an isolated target; do not replace the live full Ask database with this two-table snapshot. A production recovery needs an approved maintenance window and an explicit merge/reconciliation plan for the existing full state file.

Snapshots use SQLite's consistent read transaction and copy only the fixed accounting tables. They do not accumulate all rows in Python memory. Integrity, ownership references, schema marker and SHA-256 are checked. The report contains counts and checksums, not account identities or research text.

## Encrypted cloud copy

The optional upload command requires an operator-approved existing bucket, its expected AWS account owner and the `customer-accounting-backups/` prefix. All four bucket public-access blocks must be enabled. The command requests SSE-S3 encryption, then verifies the stored encryption, length and checksum metadata. It changes no bucket permissions and grants no public ACL.

```sh
python -m ask_mimir_beta.research_recovery upload \
  --source "$MIMIR_RECOVERY_DIR/accounting.sqlite" \
  --bucket "$MIMIR_BACKUP_BUCKET" \
  --key "$MIMIR_BACKUP_KEY" \
  --expected-owner "$MIMIR_BACKUP_BUCKET_OWNER"
```

Use a unique object key for every snapshot. Approve daily scheduling, least-privilege IAM, failure alerts and actual current/noncurrent object expiry before activation. Verify effective 30-day rotation for the selected bucket; a versioned bucket with both current and noncurrent expiry set to 30 days can retain data longer than 30 days. Do not apply customer-backup expiry to the whole intelligence lake. No cloud backup deletion is performed by this utility.

## Publication gates

The local synthetic accounting drill is a component test, not a complete disaster-recovery certification. Before publication: activate the chosen Supabase recovery path, cover Auth/profile/plan and required Storage data using supported provider mechanisms, protect an off-site copy, reapply deletions/revocations, reconcile subscription state, test an isolated full application restore and measure recovery time. Do not restore over production to perform the drill.

Prefer reusing the existing Pro organisation if it has an unused compute allowance. The connected Supabase account currently exposes only the star Free organisation; the UK Financial Intelligence organisation must be verified before a transfer or new subscription is considered. No subscription was changed by this work.
