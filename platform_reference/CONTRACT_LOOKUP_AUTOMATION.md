# Contract lookup rebuilds

The final reviewed-v7 candidate is followed by a separate `BuildContractLookup`
ECS task. It never runs in Render or changes `mimir/platform/current_manifest.json`.
Existing financial, dual-read and promotion reviews still apply.

The worker consumes the same-run `v7-auto/complete.json` receipt, including the
exact final root and Main manifest versions and hashes. It processes partitions
sequentially with the existing exact streaming layout builder. The logical Main
release ID and all analytical files stay unchanged. Source-bound lookup files,
Main annex, evidence and root are immutable objects under `mimir/releases/`.

The handoff for subsequent validation/promotion is:

```
mimir/rehearsals/weekly-<ETL_RUN_ID>/contract-lookup/complete.json
```

Use its pinned `root` reference, rather than the earlier raw v7 root. A successful
lookup only sets the `contract_lookup` gate. It does not mark the candidate
promotion-ready or satisfy the remaining gates. `release_pointer_control.py`
requires source-bound full-field parity and successful offline handler/load
proof before any new root promotion. Legacy exact-body rollback is unchanged.

## Resource and correctness limits

- One task: 2 vCPU, 4 GB task memory, 3 GB container limit, 50 GB scratch.
- One partition at a time: 512 MB DuckDB, one query thread, 768 MiB process RSS
  cutoff, 15-minute partition deadline, 4 GiB minimum scratch headroom.
- Offline query gate: existing 650 MB shared DuckDB budget, two analytical
  readers and one reserved reader; 1.5 GiB RSS cutoff and 20-minute deadline.
- Stop if active container memory exceeds 2 GiB or a cgroup OOM counter rises.
  RSS checks run every 100 ms. Kernel/container limits provide the final bound;
  polling cannot guarantee that every possible allocation spike is intercepted.
- Whole worker deadline: 75 minutes; workflow task timeout: 90 minutes.
- Every field and row is compared, including floating-point bits and duplicate
  ID selection order. Actual profile outputs, missing IDs and mixed load are
  compared offline. Point p95 must stay within 20% or 50 ms of the original;
  mixed request maximum must remain below 12 seconds.
- Same-run lease prevents overlapping builds. Completed exact-source results
  are reused; a failed attempt leaves the live root and original candidate
  untouched. A failed lease expires after two hours. Do not bypass an active
  lease or raise limits to force a release through a failed guard.

The serving worker count, reader limits, memory budget and crawler policy do not
change. AWS compute/storage for this task is additional, bounded per release.

## Installed binding and rollback

Initial task revision: `mimir-contract-lookup:2` in account 868631722720,
us-east-1. Its immutable source bundle SHA-256 is
`53fba4c6c4278d697ea1628e99d826865ad949f4c217afc8fdb13f2067932137`.
The base image digest is pinned in the task definition. Update the bundle and
test a new revision when builder, handler or serving dependency changes require
it; do not use a mutable image tag or floating code key.

Install or reapply the small additive workflow patch with:

```
python -m platform_reference.install_contract_lookup_workflow \
  --task-definition arn:aws:ecs:us-east-1:868631722720:task-definition/mimir-contract-lookup:2 \
  --journal /absolute/path/new-activation.json
```

The installer verifies the resource envelope, validates ASL, saves the prior
definition and policy, and adds only the exact task ARN to the existing ETL
role's RunTask permission. It preserves independently changed upstream states.
Reapplying the stage is idempotent, with a fresh journal.

To disable future rebuilds, use the same command with `--rollback` and a new
journal path. This removes only the two lookup states, its transition, and its
exact RunTask permission. It refuses to remove another task revision. Do not
restore an old full state-machine definition over newer unrelated work. Running
candidate tasks can finish safely; no production data pointer is involved.
