# Ask Mimir isolated lab

The lab is not linked to the production website and does not change the production API.

Start it with the existing API environment:

```bash
cd /Users/tompetterson/Documents/ChatGPT/Mimir/ask-mimir-derived-metrics

set -a
source /Users/tompetterson/Documents/my-saas-projects/market-intel-api/.env
set +a

PYTHONPYCACHEPREFIX=/tmp/mimir-pycache \
/Users/tompetterson/Documents/my-saas-projects/market-intel-api/venv/bin/uvicorn \
  lab_api:app --host 127.0.0.1 --port 10100
```

Open `http://127.0.0.1:10100`.

For a fully local interface and evidence-flow test that sends nothing to OpenAI:

```bash
ASK_MIMIR_MOCK=1 \
ASK_MIMIR_ALLOW_TEST_IDENTITIES=1 \
PYTHONPYCACHEPREFIX=/tmp/mimir-pycache \
/Users/tompetterson/Documents/my-saas-projects/market-intel-api/venv/bin/uvicorn \
  lab_api:app --host 127.0.0.1 --port 10100
```

`ASK_MIMIR_ALLOW_TEST_IDENTITIES=1` exposes a local tier selector so each allowance and export
boundary can be exercised without connecting the lab to Supabase or Stripe. Never enable it on a
public deployment.

## Beta access policy

| Access | Queries per UTC day | CSV evidence pack |
|---|---:|---|
| Public preview | 1 | No |
| Logged-in Free | 3 | No |
| Trial | 20 | No |
| Lite | 20 | No |
| Professional | 75 | Yes |
| Enterprise | 200 | Yes |

The policy is enforced by the server. Trial currently follows Lite permissions. Query creation
reserves allowance atomically, and failed model or evidence requests are refunded. Limits reset at
00:00 UTC.

The isolated lab uses SQLite for a single-process test service. A connected deployment must use a
shared Supabase/Postgres quota ledger before more than one web instance is allowed. A trusted
Next.js server route must resolve the authenticated Supabase profile and send the user ID and tier
with `X-Ask-Mimir-Proxy-Secret`. That secret must remain server-only. Anonymous limits currently
use a salted network and browser fingerprint, which is suitable for testing but is not a strong
anti-abuse identity.

The browser creates an asynchronous job at `POST /api/ask/jobs` and polls
`GET /api/ask/jobs/{request_id}`. Progress comes from the active evidence workflow rather than a
simulated timer. `POST /api/feedback` accepts feedback only for a completed response belonging to
the same subject.

Deterministic evidence is cached by tool arguments and active release binding. The service
fingerprints every bound source at startup and refuses to answer if a source changes while it is
running. Before an answer leaves the server, it is checked for unsafe links, local or object-store
paths and internal record identifiers.

Optional overrides:

```bash
export OPENAI_MODEL=gpt-5.6
export ASK_MIMIR_RELEASE_DIR=/absolute/path/to/validation-output
export ASK_MIMIR_TRANSACTIONS=/absolute/path/to/transactions.parquet
```

For an atomically promoted release, set only the release root. The lab resolves both the metric
files and frozen evidence ledger from `active_release.json`:

```bash
export ASK_MIMIR_RELEASE_ROOT=/absolute/path/to/ask-mimir-release-root
```

The OpenAI key is read server-side from `OPENAI_API_KEY` and is never sent to browser code. The lab uses Responses API function tools to search metric scopes, retrieve a versioned calculation and inspect source actions. Unsupported questions should return a declared evidence gap rather than an invented answer.

Real model mode also requires the deliberate outbound-data switch below. Leave it unset for local
mock and interface testing:

```bash
export ASK_MIMIR_ALLOW_EXTERNAL_EVIDENCE=1
```

Optionally retain a local, reconstructable answer bundle for evaluation:

```bash
export ASK_MIMIR_AUDIT_LOG=/absolute/path/to/ask-mimir-audit.jsonl
```

Real model mode transmits the user's question and the tool results selected by the model to the
configured OpenAI API account. Use local mock mode for UI and evidence-contract testing when no
external data transfer is intended.

Run the repeatable evaluation cases against the active lab server:

```bash
PYTHONPATH=. /Users/tompetterson/Documents/my-saas-projects/market-intel-api/venv/bin/python \
  evaluate_lab.py --base-url http://127.0.0.1:10100
```

Run the customer-launch query suite, including Tomahawk routing, platform follow-ups, company/site
resolution and internal-language checks:

```bash
PYTHONPATH=. /Users/tompetterson/Documents/my-saas-projects/market-intel-api/venv/bin/python \
  evaluate_lab.py \
  --base-url http://127.0.0.1:10100 \
  --cases-file launch_eval_cases.json \
  --tier enterprise \
  --subject local-launch-eval
```

Real-model responses include:

- `response_calls`: the number of Responses API calls used by the tool loop;
- `usage`: aggregate token usage across every call, not only the final synthesis;
- `usage_by_response`: the per-call token ledger; and
- `estimated_cost`: a model-rate estimate for the aggregate usage.

The cost estimate is operational metadata rather than billing authority. Pricing is dated in the
response and should be reconciled periodically against the OpenAI API pricing page.

The first workflow-specific benchmark is documented in
`use_cases/OPPORTUNITY_DISCOVERY.md`, with machine-readable cases in
`use_cases/opportunity_discovery_eval_cases.json`.

Build the current analyst-seeded Curtiss-Wright parent and Newtown CAGE contexts with:

```bash
PYTHONPATH=. /Users/tompetterson/Documents/my-saas-projects/market-intel-api/venv/bin/python \
  company_context.py
```

Build the deterministic Newtown opportunity pack after the company contexts with:

```bash
PYTHONPATH=. /Users/tompetterson/Documents/my-saas-projects/market-intel-api/venv/bin/python \
  company_opportunity_candidates.py
```

The opportunity pack separates existing-position expansion, pre-solicitation requirement shaping
and adjacent whitespace. It attaches official event facts, prime awards, budget lines and visible
incumbent subcontract evidence before the model sees the candidates. Program and incumbent values
are never treated as accessible company revenue.

Opportunity answers separate market attractiveness from practical account accessibility.
Proprietary interfaces, long qualification cycles and entrenched qualified incumbents are treated
as normal structural barriers that shape the entry path, not automatic stop conditions. While the
model works, the lab shows staged analysis feedback. Its evidence drawer renders campaign-level
award, authoritative-source, budget and selected subaward-report evidence instead of one raw JSON
payload.

The company tools resolve parent versus site before returning a focused profile, supply-chain or
opportunity-discovery evidence pack. The current Curtiss-Wright benchmark outputs are retained in
`eval-output/company-opportunity-discovery/`. These contexts are evaluation fixtures, not a general
production parent resolver.

Build the deterministic CH-53K supply-chain pack with:

```bash
PYTHONPATH=. /Users/tompetterson/Documents/my-saas-projects/market-intel-api/venv/bin/python \
  build_platform_supply_chain.py
```

The platform supply-chain workflow keeps the platform prime, component-verified suppliers,
reported first-tier relationships, lower-tier/component procurement, direct award recipients and
broader family evidence separate. Broad CH-53 family DLA references are labelled as unconfirmed for
CH-53K, T64 records are excluded from strict totals, and DLA activity is valued once at NIIN grain.
The evidence drawer offers a customer-safe ZIP of separate CSVs without internal report keys.

Build the deterministic missile-program momentum pack with:

```bash
PYTHONPATH=. /Users/tompetterson/Documents/my-saas-projects/market-intel-api/venv/bin/python \
  build_program_momentum.py
```

Generate and retain the governed natural-language benchmark with:

```bash
set -a
source /Users/tompetterson/Documents/my-saas-projects/market-intel-api/.env
set +a
export ASK_MIMIR_ALLOW_EXTERNAL_EVIDENCE=1
export OPENAI_MODEL=gpt-5.6

PYTHONPATH=. /Users/tompetterson/Documents/my-saas-projects/market-intel-api/venv/bin/python \
  run_program_momentum_answer.py
```

The model receives compact calculated lanes and leading examples. The complete awards, reported
supplier records, budget rows and source locators stay in the answer artifacts for drill-down.

Live research follows `web_source_policy.json`: official public records first, then first-party
industry sources, then trusted defense reporting. General web search remains available as a fallback;
the priority list is not a hard allowlist.

Archive the first FY2027 component justification-book set with:

```bash
PYTHONPATH=. /Users/tompetterson/Documents/my-saas-projects/market-intel-api/venv/bin/python \
  budget_pipeline/ingest_fydp_sources.py \
  --download-dir /Users/tompetterson/Downloads/mimir-fydp-fy2027 \
  --profile new-account
```

This validates the PDF signature, records the SHA-256 checksum and source URL, uploads each original
book under the versioned S3 prefix in `budget_pipeline/fydp_source_manifest.json`, and uploads a
resolved manifest beside the documents.

Normalize the archived Exhibit P-40 resource summaries with:

```bash
PYTHONPATH=. /path/to/python \
  budget_pipeline/normalize_fydp_justification_books.py \
  --input-dir /Users/tompetterson/Downloads/mimir-fydp-fy2027 \
  --manifest /Users/tompetterson/Downloads/mimir-fydp-fy2027/fydp_source_manifest.resolved.json \
  --output-dir budget_pipeline/validation-output/fydp
```

The output keeps FY2027 base, OOC and total request separate; retains explicit blanks and
`Continuing` values; and labels FY2028-FY2031 as projections. It reads only first-page P-40 resource
summaries and retains the source PDF, SHA-256, page number and P-1 line for every fact. Upload the
validated parquet beneath `silver/dod_budget/ref_fydp_budget_facts/data/`, then create and validate
the Athena table with the two `athena_*_ref_dod_fydp_budget_facts.sql` files.

Generate the benchmark full CAGE-site answer with:

```bash
PYTHONPATH=. /Users/tompetterson/Documents/my-saas-projects/market-intel-api/venv/bin/python \
  run_company_dossier_answer.py --cage 19645
```

Generate the Army ground-vehicle power competitive-position benchmark with:

```bash
PYTHONPATH=. /Users/tompetterson/Documents/my-saas-projects/market-intel-api/venv/bin/python \
  run_competitive_position_answer.py
```

Generate the Eaton Aerospace observed-competitor benchmark with:

```bash
PYTHONPATH=. /Users/tompetterson/Documents/my-saas-projects/market-intel-api/venv/bin/python \
  run_competitor_discovery_answer.py
```
