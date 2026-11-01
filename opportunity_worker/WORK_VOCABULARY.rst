Work vocabulary and evidence quality: additive backend interface
===============================================================

Company and opportunity scope now share a versioned vocabulary. Reported PSC,
explicit technology/deliverable concepts and work types remain separate facets.
An observed PSC or topic overlap does not establish qualification, capacity,
technical interchangeability or available work.

The first reference edition uses the official GSA April 2025 workbook, retained
with its URL and SHA-256. Dated historical codes and hierarchy nodes are retained;
the active/reference status is explicit. Its category-management crosswalks,
includes/excludes and parent references are available. The dictionary is pinned
to this reviewed edition, not silently described as a permanently current feed.

Additional topics supplement PSC: 71 work-scope topics, seven separate platform/mission context topics and eight navigation
roots, plus ten independent work types. Examples include radar antennas,
electronic warfare, network operations, aircraft power and fuel systems, with
design/development, integration, production, repair and sustainment kept distinct.
These are an initial controlled vocabulary, not a complete technical ontology.
Platform mentions do not define the delivered work: a proximity switch used on
a submarine is classified as a switch, with submarine context, not shipbuilding.
Market research and business development do not establish engineering R&D.
Work types retain matched source passages; negations are evaluated within clauses.

Immutable logical tables
------------------------

* work_vocabulary_reference: PSC entries, effective dates, hierarchy, category
  crosswalk, includes/excludes and official source provenance.
* work_vocabulary_topics: stable topic_id, parent_id, display label, version.
* work_vocabulary_scopes: deduplicated raw work scopes, normalization status,
  normalized_scope_label, topic_ids_json, work_scope_topic_ids_json,
  context_topic_ids_json, work_types_json and annotation_json.
  The annotation includes matched phrases/passages and source-vocabulary method.
* company_capability_vocabulary: every observed company_capability_history row,
  with the original company_id, capability_id and evidence references retained.
* opportunity_vocabulary: every opportunity episode/version and its scope labels.
* company_match_vocabulary: company and opportunity scope labels for each work
  edge, shared topics, separately shared work-scope/context topics and both work types. This is explanation/retrieval evidence.
* company_match_evidence_reviews: old fit verdict, source financial evidence,
  new evidence verdict and rationale, bound to company/episode/version and policy.
* work_vocabulary_coverage: actual classified, PSC-only, administrative and
  unclassified populations. Unknown scope is not fabricated into a capability.

The worker also archives work_match_source_facts and its exact evidence-key set
under the existing source namespace. These reference existing award/subcontract
facts; they do not overwrite or delete those source records.

Serving contract
----------------

Existing authenticated API endpoints remain backwards compatible. New endpoints:

* GET /opportunity-intelligence/work-vocabulary?psc_prefix=58&limit=100&offset=0
  returns topic navigation, paginated official PSC entries and the current policy.
* GET /opportunity-intelligence/companies/{cage}/capability-vocabulary
  returns source-backed normalized company capabilities with pagination.
* GET /opportunity-intelligence/opportunities?topic=radar&kind=work
  supports technology interest retrieval without selecting a company. Root topics
  such as electronics include their child topics. topic and optional psc prefix
  are validated. Providing company=CAGE:xxxxx additionally applies company fit.

The existing opportunity rows add normalized_scope_label, topic_ids_json,
work_types_json, work_vocabulary_json and vocabulary_version. Keep original
title/scope/PSC, source IDs, episode_id, record_hash and release. Displaying a
normalized label should not conceal the exact work package or configuration.

The website's authenticated /api/opportunities proxy accepts these paths and
topic/psc/psc_prefix query parameters. Existing Supabase principal verification,
private/no-store responses and noindex behavior remain intact. No user preference
schema or company-dashboard/MCP consumer was changed. Persistent interest profiles
without a company are a subsequent consumer addition, not an installed feature.

Evidence hygiene
----------------

Generic kickoff/status/interchange meetings cannot establish a technical
capability. Those selected work matches are rejected and removed from serving
links, with original history and the assessment retained. A negative adjustment
with no retained positive obligations is review context until its original funded
history or independent delivery evidence is found. It is not a claim that the
company never supplied the product. Zero-funded contract vehicles remain distinct
from negative-adjustment evidence and can still prove an explicitly documented
incumbent agreement or eligibility context.

Supply-chain vocabulary uses the supplier work package, not an entire prime
platform description. Programme-scale funding is not assigned to the supplier.
Historical spending, future gross demand and company-addressable value retain
their existing distinct meanings. No revenue uplift or win probability is added. Raw subcontract package amounts
are not additive market spending. An additional source-quality audit found two
implausible reported amounts far above their linked parent award figures; they
require source verification, not a guessed corrected amount.

Refresh and review
------------------

The existing AWS worker runs the vocabulary after market-demand generation on
native refresh, and can rebuild it independently against an immutable release.
Full release gates check coverage, source versions, unique scopes, retained
passages, qualification semantics and the exclusion of rejected evidence.
Publishing remains atomic; failed runs preserve the previous current release.
A standalone rebuild exposed stale local paths for a retained distributed input;
the validator now restores its exact immutable source files and checks their
hashes before registering local validation views. Completed unpublished outputs
can be recovered only against their exact parent release and must pass all
validation again before publication.

The expanded review bank is stratified across company topic/PSC, business role
and scale, with the original eight sites retained for continuity. It is a
deliberate review set, not an accuracy estimate or a claim that all records are
new-business leads. Review both incorrect matches and known relevant omissions.

PSC provenance and controlled projection
----------------------------------------

Supplier-package PSCs inherited from linked prime contracts are context, not a
classification of the supplier deliverable. The opportunity projection keeps
psc_scope, work_psc, parent_prime_psc and parent_prime_psc_label separate. A
reported study PSC on a parent cannot turn a fuel-pump package into study work.
PSC filtering uses work_psc where available. The original opportunity PSC,
source record hash and parent context remain available.

The same projection logic runs during full refreshes and as a controlled update
against an immutable release with the exact current annotation policy. The
controlled update reuses verified annotations, preserves opportunities, company
links and their hashes, and reruns all release gates before atomic publication.
It does not re-infer company qualification or create additional opportunities.
