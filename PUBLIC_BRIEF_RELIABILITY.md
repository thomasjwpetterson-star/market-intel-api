# Public brief reliability and inference cost safeguards

Local candidate on `codex/public-brief-cost-controls`, based on released API
commit `24e196403a94fc57bc42e094cb16737e1cb64a83`. No production action or new
environment setting is required by this patch. Integration and rollout remain
with the parent task.

## Existing customer flow and risk

The sole browser caller found in the current frontend is
`components/PublicTeaserSearch.tsx:147`. It posts directly to the API after a
Supabase session is found, or after the lead-capture OTP request succeeds.
The browser also remembers `mimir_lead_captured` in localStorage. The API has
no server-side verified-lead gate on this endpoint; the optional bearer header
does not establish authorization. The preceding teaser lookup has a separate
IP limit with a header-presence bypass; it does not bound direct brief calls.

Previously, each valid request rebuilt the evidence and started a new provider
call. The installed provider client defaults to two automatic retries and a
600-second read timeout. A provider failure returned `success: false`, causing
the existing preview component to hide its company result and tables.

## Exact behavior of this candidate

The request and successful response shapes are unchanged. Customers still
receive an immediate preview after the existing lead flow. No new sign-in,
email confirmation, plan gate, customer quota or durable budget is introduced.

Only generated text is cached. The cache key hashes the exact system prompt,
server-derived evidence, model, and generation settings. Identity aliases and
forged browser financial values cannot select a different financial narrative:
the API first resolves its existing server profile. Every request still builds
fresh `deep_data`; changed evidence immediately produces a different text key.

The process-local safeguards are:

| Safeguard | Default behavior |
|---|---|
| Successful text cache | Up to 128 entries, each no longer than 6,000 characters, for one hour |
| Identical simultaneous requests | Share one provider call |
| Distinct simultaneous generations | At most two; excess requests receive the evidence-only summary without an inference queue |
| Provider deadline | 20 seconds, with automatic SDK retries disabled |
| Failed/empty/oversized result | Evidence-only summary and 15-second cooldown before attempting that key again |
| Cancelled waiter | Does not cancel a shared generation needed by another customer; provider work remains time-bounded |

Cached strings and fixed-length hashes have bounded memory; there is no cache
of full tables or unbounded map of IP addresses. Logs contain completion,
capacity and error-class outcomes only, with no prompts, provider diagnostics,
email addresses or user financial payloads.

The commercial tradeoff is deliberate: a busy, slow or failing provider yields
a shorter, deterministic summary of the same loaded evidence while the data
tables remain usable. The summary explicitly describes itself as
“evidence-only.” It does not invent analysis or treat missing records as no
activity. A failed call is not cached for the full success TTL.

This is not a complete abuse-prevention system. Distinct-company requests can
still spend money, and process restarts or additional workers have independent
caches and bounds. Database evidence work occurs before the inference guard
and remains subject to the existing AnyIO/DuckDB limits. Server-verified lead
access, durable quotas and a spending circuit breaker remain separate work.

## Monetary basis correction

The existing prime aggregate is not uniformly a net-obligation measure. The
read-only live Athena definition saved at
`/Users/tompetterson/Documents/ChatGPT/Mimir/outputs/website-audit-2026-10-02/data-evidence/catalog-market_intel_gold-global_spend_transactions.sql`
uses USAspending `federal_action_obligation` at line 101 and DLA
`netprice * order_qty` at line 171; lines 217–224 apply a contract-ID exclusion
to the DLA rows. A particular company need not contain both sources, and the
brief does not receive a source-specific split.

The brief now says **observed prime contract value**, explaining that the
measure can include USAspending net obligations and DLA procurement-line values
depending on the available records. Network ETL maps `flow_amount_capped` to
`subaward_value` (`run_etl.py:1026`), so the second figure is labelled
**Mimir-adjusted reported subcontract value**. These measures have potentially
different observation periods and overlapping coverage; they must not be
added together or presented as company revenue.

This patch changes the brief's prompt, its agency headline and its deterministic
fallback. It does not change any numbers or repair labels elsewhere in the
website, the database or existing materialized public pages. It does not
establish transaction-level completeness or reconcile adjusted amounts back
to individual government reports.

Reference-only company identity and an unavailable subcontract breakdown now
produce “unavailable” evidence rather than a fabricated `$0`. Actual observed
zero remains zero. The fiscal window and separate-measure semantics from the
previous release remain covered by tests.

## Regression checks and manual acceptance

The local suite covers exact evidence-key invalidation, one provider call for
concurrent identical requests while the event loop progresses, bounded memory
and TTL, full-capacity fallback, failed-call cooldown/recovery, provider timeout,
disconnected waiters, result validation, unknown coverage, provider retry
configuration, stable response keys, and fresh deep data despite cached text.
Existing company module isolation, reload, award serving, platform, public
projection/release and automation tests are included in the combined run.

Final local validation passed **86 combined API tests** and **3 existing Ask
context/logging regression tests**. Python compilation and `git diff --check`
also passed. The 86 include all 75 previously verified API tests plus nine
runtime-helper tests and two endpoint regression tests for provider failure
and text reuse with refreshed data.

These tests use dummy credentials and mocked inference/local data; no customer
email, production provider request or production database mutation is needed.

After an approved deployment, a human acceptance check is:

1. On `/tools/cage-code-lookup`, search a known company such as CAGE `81755`.
   Confirm identity, customers and available company data still appear.
2. Using an existing signed-in account, unlock the brief. Confirm the three
   headings and tables remain visible, and the text labels the two monetary
   measures separately. A guest should still see the existing name/work-email
   form; use a real test inbox if checking its email link.
3. Repeat the same company without changing data. Confirm the response has the
   same text and current tables. Provider-call reuse is established by local
   instrumentation tests, not by text similarity alone.
4. Compare another company to verify the correct identity replaces the first.
   For a reference-only record, unavailable financial coverage should remain
   unavailable rather than imply zero company business.

Do not simulate overload or provider failure against production to exercise
fallbacks. Those paths have deterministic local tests. After rollout, compare
readiness, public GET success/latency, and content-free `public_brief` completion
and fallback log counts with the release baseline. Revert the source patch if
the normal preview or tables regress; there is no schema/data change to undo.
