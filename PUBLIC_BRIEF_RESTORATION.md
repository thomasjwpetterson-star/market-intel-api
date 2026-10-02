# Exact pre-audit public brief restoration

The user requested the original AI brief, not another revised interpretation.
`generate_unlocked_brief` is restored from
`de56e371cffafcc95338c8470428038043338da1:main.py`, the production baseline before
this conversation's API audit changes.

The original prompt text, request inputs, financial calculations, fixed period
wording, headings, headline, $250,000 contract filter, parts period calculation,
network selection, model, temperature, token limit, raw response text and error
response are restored. The public endpoint again makes its direct OpenAI call
using the original global client's default retry and timeout behavior. It does
not use our brief styles, cache, generated-output validation, methodology
response field or evidence fallback. The added runtime module and its obsolete
tests/fixtures are removed because no other code uses them.

The sole difference within the restored function is that the same six database
calls run through the existing bounded service worker pool. Queries, arguments,
ordering and returned data are identical to the original. This retains event
loop responsiveness while producing the original prompt and response behavior.
The independent company-module and public NSN fallback repairs remain intact.

## Parity receipt and local verification

The original function's exact source, after trimming only end-of-function blank
lines, has SHA-256:

`0c7a2cc96dc594b8ed5fe67eee9410f22777fc61ccd0e758052a04d36ee92fd8`

`test_public_brief_restoration.py` checks that removing only the six worker
wrappers reproduces that checksum. It also verifies raw provider text, uncached
repeat calls, the original provider-failure response, parent query behavior and
the retained shared worker limit. All inference is mocked; no paid generation,
email, account, billing or production data operation is performed by these tests.

Combined affected suite: 21 tests passed with `test_public_brief_restoration`,
`test_api_audit_safeguards`, `test_reload_authorization` and
`test_public_award_serving`. Literal original source includes trailing spaces,
including within prompt strings; those bytes are retained deliberately. The
ordinary whitespace check reports those restored baseline lines.

## Commercial behavior

Repeat public brief requests again make fresh provider requests rather than
using the removed cache or failure cooldown. The original financial phrasing,
percentages and conclusions are restored as explicitly requested. A provider
failure again returns `success: false` rather than substituting a different
brief. This document records source restoration and local verification; live
deployment status is recorded in the conversation's release receipt.
