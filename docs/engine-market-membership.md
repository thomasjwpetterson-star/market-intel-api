# Engine market membership: private integration

This branch adds an optional native market membership path. It is not enabled in
production or attached to the ETL run currently in flight. Existing releases
without `scopeMembership` keep the existing reader path.

Each eligible financial allocation slice keeps its existing signed amount and
full key: source kind, source record, fiscal year, native record and allocation.
The bridge contains approved market/platform associations and evidence, without
amounts. Selection uses `EXISTS` before aggregation, so overlapping associations
and equal-dollar records cannot multiply or collapse financial facts. Partial
allocations stay partial. Primary allocation is unchanged.

An engine record may have a dedicated fighter-family association while its
individual aircraft associations remain shared. These require their own reviewed
extent. Shared aircraft tags do not establish complete dedicated fighter use.

## Existing canonical decisions

`market_scope_facets.py` normalizes existing buyer, platform, applicability,
product technology, technology subject, work type, role and application-domain
fields. It reads the canonical ledger; it does not classify records again.
Existing work subjects and holds must be supplied with exact resolved allocation
keys. A parent-only or native-only wildcard cannot silently widen an extent.

`market_scope_continuity.py` is the local continuity oracle.
`market_scope_build.py` compiles the distributed carry checks, so bulk inventories
can remain in AWS. Dropped mappings, subjects or holds fail the gate. Value-only
changes are allowed without reclassification. Source retirements, identity
transitions, explicit corrections and genuine scope changes require bound
authorities. A record cannot simultaneously be retired and carried through an
alias. Historical dispositions never reinsert old dollars.

The accepted 102-action branch is an additional input to reconcile with the
eligible new Main, not a replacement for newer canonical decisions or a new
overnight gain. Derive inventory counts from selected receipts rather than
hard-coding historical totals.

## Reader and proof contract

`market_scope_detail.py` compiles both annual metrics and whole-selection totals
from the same selected native relation. Whole-period distinct award counts are
not sums of annual or relationship-bucket counts. First-tier reported values and
DLA purchases retain separate source measures and supplier roles. Existing DLA
overlap eligibility must pass before a combined market amount is published.

The trusted, authenticated UI proxy obtains the descriptor from the verified
Market manifest. Its proof binds exact Main, ledger, registry and dictionary
hashes, plus the consumed output table names, catalog versions, physical schema
hashes and versioned output-object inventories. It also binds the selection
policy and declared schemas. Matching table suffixes alone are insufficient.
The API is a trusted proxy endpoint, not an end-user arbitrary-table API.

Public aggregates, private snapshots and Professional requests must bind the
same membership proof and activity policy. The spending cascade remains a
primary-allocation view. The loader rejects partial or mismatched projections;
it does not silently fall back to old totals. API cursors bind the exact inputs,
outputs, policy, filters and award. Native action/stock joins retain source,
award, fiscal year and native identity before financial grouping.

The new path requires an enriched native fact relation. Its party/header adapter
must use the exact Main contract lookup selection rules and source-bound first-
tier identities, not the old small metadata supplement. Missing identities stay
explicit. The private materialization and this raw-source adapter are not yet
wired into the installed producer.

## Tests and release boundary

Local SQL tests execute the generated queries with DuckDB and small synthetic
fixtures. Existing saved F135/Columbia witnesses remain explicitly experimental;
they do not approve a primary repair. Reader tests exercise the actual shared
loader and Professional wrapper using local objects and simulated transport.
These are not real AWS, hosted authentication or full-model serving proof.

After the ETL owner explicitly releases the lane, resolve the new lookup-annex
Main, matching v7 authority, accepted canonical checkpoints and the preserved
102 facets. Reuse existing operations and serialize all heavy work. Build only
private candidate relations, inspect exact current native engine extents and
metadata, run full financial and prior-mapping/subject/hold controls, and compare
every prior reader scope, including existing-platform-only selections.

Then verify actual public/Professional totals, complete pagination, award/NSN/
contractor/platform links, per-source measures and warm old → candidate → old
rollback with the pinned artifacts. Customer sign-in remains deferred by the
user; simulated authentication must stay labelled. A new release requires the
separate source/deployment and promotion checks in AGENTS.md. No live pointer,
installed worker, frozen input, IAM, schedule or deployment is changed here.
