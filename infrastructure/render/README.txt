MAIN AUTO-DEPLOY FILTER — PREPARED, NOT APPLIED

Target: market-intel-api / srv-d59jle1r0fns73frev1g
Workspace: tea-d59j9l2li9vc73alp840

The live Render Settings page was read on 27 November 2026 around
10:29 UTC / 10:29 PM Fiji. Included Paths and Ignored Paths were empty;
Auto-Deploy was On Commit. No controls were edited.

main-build-filter.json is the proposed narrow PATCH body for the existing
service, not a new Blueprint or service definition. It excludes reviewed
Ask-only files from Main auto-deploys. Main still imports these two Ask files:
  ask_mimir_beta/platform_manifest.py
  ask_mimir_beta/platform_description_mapping.py
Both remain triggers. Other Main paths, mixed Main/Ask commits, and unreviewed
new top-level Ask files/directories continue to trigger Main. Ignoring the whole
Ask directory would be unsafe. Ask beta's existing manual-deploy setting is
unchanged. Memory, CPU, workers, read limits, health checks and release pins are
absent from the PATCH and must not change.

Validation (read-only/local):
  python scripts/render_build_filter.py --check
  python -m pytest -q test_render_build_filter.py

Before applying, obtain deployment/settings approval and wait for the other
chat's current Main release to finish. Re-read remote main, the actual live
deployment and current build filters. Re-run the dependency audit at that source.
If the filters or imports changed, review/regenerate the proposal and capture
the new exact rollback payload. Do not overwrite someone else's new settings.

After approval, update only buildFilter using Render's Update Service endpoint
or the existing service's Settings > Build Filters. Treat a configuration save
as a possible deploy trigger; record deployment IDs and verify readiness.
Never create a replacement service or import ask_mimir_beta/render.yaml, which
describes a historical service and is not the production beta configuration.

Rollback: restore only the prior buildFilter (currently the empty lists in
main-build-filter.rollback.json). Keep unrelated configuration and deployments.
Preserve [skip render] in the operations-only commit when merging these files,
so merely recording this plan does not start another Main preparation build.

Manual deploys and configuration-triggered deploys bypass build filters.
These filters cannot fix the cold-start disk footprint or remove Ask's
persistent-disk deployment gap; those remain separate existing work.

References:
https://render.com/docs/monorepo-support#setting-build-filters
https://render.com/docs/deploys#skipping-an-auto-deploy
