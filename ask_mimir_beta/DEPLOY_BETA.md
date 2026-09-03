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
- `ASK_MIMIR_MANIFEST_KEY`: the exact immutable manifest key printed by
  `publish_runtime_release.py`
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
citation validation on at startup.

Current release manifest:

`ask_mimir/releases/ask-mimir-beta-20260903T194432Z-025c940cbda3/runtime_manifest.json`

## Release check

After deployment, open the service's private `onrender.com` URL and verify:

1. `/api/health` reports healthy stores.
2. The page has no public navigation link and emits `noindex` metadata.
3. An anonymous visitor can submit one question in a rolling 24-hour window.
4. The evidence trail is visible below the answer.
5. The CSV evidence pack displays a Professional upgrade prompt and does not
   expose a download URL to the anonymous visitor.
