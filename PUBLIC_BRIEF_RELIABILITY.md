# Public brief reliability and inference cost safeguards

Current presentation correction is based on released API commit
`c240fd2976b7f5c801d2e33c4751b5fcb375f0aa`. No production action or new
environment setting is required by this patch. Integration and rollout remain
with the parent task.

## Existing customer flow and risk

The public lookup browser caller is `components/PublicTeaserSearch.tsx`.
It posts directly to the API after a
Supabase session is found, or after the lead-capture OTP request succeeds.
The browser also remembers `mimir_lead_captured` in localStorage. The API has
no server-side verified-lead gate on this endpoint; the optional bearer header
does not establish authorization. The preceding teaser lookup has a separate
IP limit with a header-presence bypass; it does not bound direct brief calls.
The dashboard's protected summary route also forwards to this API, with its
own existing sign-in and entitlement checks. That does not authenticate the
direct public API endpoint.

Previously, each valid request rebuilt the evidence and started a new provider
call. The installed provider client defaults to two automatic retries and a
600-second read timeout. A provider failure returned `success: false`, causing
the existing preview component to hide its company result and tables.

## Exact behavior of this candidate

The request and existing successful response fields are unchanged. Two optional
response fields are added: `methodology`, a short fixed note for the frontend's
collapsed data notes, and `brief_mode` (`generated` or `evidence`). The latter
identifies whether the returned text is the deterministic fallback. Customers still
receive an immediate preview after the existing lead flow. No new sign-in,
email confirmation, plan gate, customer quota or durable budget is introduced.

Only generated text is cached. The cache key hashes the exact system prompt,
server-derived evidence, exact monetary values, model, and generation settings.
Compact currency display does not merge different underlying amounts in the cache.
Identity aliases and
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
tables remain usable. Fallback status is available in `brief_mode`; the brief's
three concise business sections do not carry a methodology paragraph or an
“evidence-only” disclaimer. It does not invent analysis or treat missing records as no
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

The brief says **observed prime contract value**, without attributing a
particular company's total to an unverified source combination. Network ETL maps `flow_amount_capped` to
`subaward_value` (`run_etl.py:1026`), so the second figure is labelled
**tracked subcontract value**, with adjustments explained in the separate
methodology field. These measures have potentially
different observation periods and overlapping coverage; they must not be
added together or presented as company revenue.

This patch changes the brief's prompt, its agency headline and its deterministic
fallback. It does not change any numbers or repair labels elsewhere in the
website, the database or existing materialized public pages. It does not
establish transaction-level completeness or reconcile adjusted amounts back
to individual government reports.

### Follow-up finding from the approved two-request production smoke

The first generated DOMMES brief incorrectly said its value was derived from
both datasets despite an unavailable company source split. The repeated request
returned the same cached text; caching correctly reduced duplicate work but did
not validate that claim. No additional production generations were used to
develop this correction.

The company evidence does not include the general pipeline recipe. Source
datasets and valuation formulas remain excluded from the generated narrative.
Following user feedback, no UNKNOWN/source-composition language or general
methodology paragraph is appended to `ai_brief`. Instead `methodology` contains:
“Based on government contract and subcontract records. Values cover the periods
shown and may overlap; subcontract totals include Mimir adjustments.” Policy
and methodology text participate in the cache key.

A validator rejects generated dataset/formula terminology, including formatting
variants of USAspending, procurement/contract-line values, net price, ordered
quantity and federal action obligation. It permits DLA/Defense Logistics Agency
as an awarding-agency mention. Source-composition prose is also excluded from
the narrative. This
deliberately narrow output boundary avoids trying to infer claim grammar from
phrases such as “derived from”; it also rejects otherwise accurate generated
methodology descriptions because that content belongs outside the business brief.
Rejected output uses the existing fallback and short failure cooldown. It is
not a claim of complete semantic verification or protection against every
possible paraphrase. The 6,000-character narrative limit remains; methodology
is a separate short constant. A deterministic currency formatter converts
large values to readable units (for example `$175.8B`), including expanded
model output, word/spaced units and signed amounts. It does not change numbers
in `deep_data` or establish the truth of arbitrary generated claims.

Reference-only company identity and an unavailable subcontract breakdown now
produce unavailable evidence rather than a fabricated `$0`. Actual observed
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

The combined local suite includes the previously verified API tests, runtime
safeguards, provider failure, fresh data with cached text, rejection of the
captured unsupported claim, compact display, optional metadata, unchanged exact
table values, and cache invalidation when precise amounts round identically.
Existing Ask context/logging regressions and Python compilation are also checked.
Local validation for this presentation correction passed **92 API tests** and
**3 Ask regression tests**, plus Python compilation and `git diff --check`.

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
