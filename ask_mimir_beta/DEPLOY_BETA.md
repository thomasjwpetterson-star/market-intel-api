# Ask Mimir standalone deployment

Deploy this directory as a separate Render web service. It can remain unlinked
from the main Mimir website while it is being evaluated.

## Render service

- Repository: `market-intel-api`
- Root directory: `ask_mimir_beta`
- Runtime: Python
- Build command: `pip install -r requirements.txt`
- Start command: `python serve.py`
- Instance: 1 CPU / 2 GB RAM (`1c-2g`)
- Health check: `/api/health`
- Persistent disk: 10 GB mounted at `/var/data`

## Secret environment variables

- `OPENAI_API_KEY`: the OpenAI API project key used by Ask Mimir
- `AWS_ACCESS_KEY_ID`: read-only access to the Mimir artifact bucket
- `AWS_SECRET_ACCESS_KEY`: matching AWS secret
- `ASK_MIMIR_ANONYMOUS_SALT`: a long random value, generated once
- `ASK_MIMIR_TRUSTED_PROXY_SECRET`: the same random value configured on the
  main Mimir web application. This allows the main site to pass authenticated
  subscription tiers to Ask Mimir without trusting browser-supplied headers.

The main Mimir deployment should also define:

- `ASK_MIMIR_API_URL=https://ask-mimir-beta.onrender.com`
- `ASK_MIMIR_TRUSTED_PROXY_SECRET`: exactly the same value as Render. The
  existing `MIMIR_EXPORT_PROXY_SECRET` may be reused instead on the main site,
  because the proxy supports it as a fallback.

The remaining non-secret environment variables are documented in
`render.yaml`. The service deliberately forces test identities off and strict
citation validation on at startup. It uses the configured GPT-5.6 model with
high reasoning effort; tier allowances, rather than lower-quality inference,
provide the primary cost control.

The service follows this stable manifest pointer by default:

`ask_mimir/runtime/current_manifest.json`

Each publication first writes an immutable release manifest, including the
exact S3 version ID and SHA-256 hash of every serving file. It updates the
stable pointer only after the complete release has been validated and
published. No Render environment variable changes are required for routine
data refreshes.

For automatic activation after promotion, copy the service's secret Deploy
Hook URL from its Render Settings page and set it as
`ASK_MIMIR_RENDER_DEPLOY_HOOK_URL` in the environment that runs
`publish_runtime_release.py`. Candidate publication does not restart Render;
promotion updates the current pointer and then triggers the restart/deploy.

Routine publications are now staged as a candidate first. Rebuild only the
domains whose data or derived artifacts changed:

```bash
cd /Users/tompetterson/Documents/my-saas-projects/market-intel-api

venv/bin/python ask_mimir_beta/publish_runtime_release.py \
  --profile new-account \
  --only companies,platforms
```

This reuses unchanged objects from `ask_mimir/runtime/current_manifest.json`,
uploads only the selected outputs, verifies every object referenced by the new
manifest, and writes `ask_mimir/runtime/candidate_manifest.json`. It does not
change production.

Supported domains are:

```text
serving-data,references,classification,source-depth,recent-awards,metrics,
core-packs,segments,capabilities,products,companies,platforms,states
```

After candidate checks, promote it without rebuilding:

```bash
ASK_MIMIR_RENDER_DEPLOY_HOOK_URL="<private Render deploy hook>" \
  venv/bin/python ask_mimir_beta/publish_runtime_release.py \
  --profile new-account \
  --promote-candidate
```

Use `--promote` on the first command only when an immediate candidate-to-live
promotion is intentional. Omit `--only` for a complete rebuild. A refreshed
ETL cache should be published with `serving-data` plus every derived domain
that depends on the changed files; ordinary code and prompt changes require a
normal application deployment and no artifact release.

To roll back, set `ASK_MIMIR_FOLLOW_CURRENT_RELEASE=0` and set
`ASK_MIMIR_PINNED_MANIFEST_KEY` to a previously published immutable manifest.

## Release check

After deployment, open the service's private `onrender.com` URL and verify:

1. `/api/health` reports healthy stores.
2. The page has no public navigation link and emits `noindex` metadata.
3. An anonymous visitor can submit one question in a rolling 24-hour window.
4. The evidence trail is visible below the answer.
5. The CSV evidence pack displays a Professional upgrade prompt and does not
   expose a download URL to the anonymous visitor.
