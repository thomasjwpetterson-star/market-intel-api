Live opportunity intelligence worker
====================================

This is the versioned source of the AWS worker serving the authenticated Mimir
opportunity page and company home through /opportunity-intelligence. The HTTP
adapter is ../opportunity_intelligence; worker-only dependencies are isolated.

The existing Glue job mimir-opportunity-evidence-refresh is configured daily
at 14:30 Pacific/Fiji. The full-market runtime uses Glue 5.0 with two G.2X
workers; infrastructure/runtime.json records its configuration. Frozen Linux
Python 3.11 wheels and their hashes are recorded in RELEASE.json. The trigger
is paused during runtime changes and restored only after successful validation. Source tables are read-only. All derived writes remain under the
existing governance/opportunity_intelligence prefix and versioned oi_* tables.
A release becomes current only after local evidence/financial gates and Athena
readback. Public publication permission is independent of authenticated access.

Every native run refreshes award and subcontract projections, resolves revision
rows before net financial aggregation, rebuilds company work and profiles,
derives new-prime supplier packages from exact parent/program lineage, attaches
historical same-scope/work-mode/buyer comparators, and regenerates company links.
No company sampling or per-company recommendation quota is used.

Values distinguish historical obligations, historical supplier-package ranges,
reference-price scenarios, future demand and company-addressable value. The
last two remain unknown unless their inputs support them. Programme/prime
ceilings are not allocated to a supplier. Eligibility and technical access are
separate from similarity. Subcontract revisions are not summed as new spend.

Tests: PYTHONPATH=opportunity_worker python3 -m pytest opportunity_worker/tests
Rebuild: python3 -m opportunity_engine --from-current --workdir /tmp/mimir-opportunity-engine --refresh --publish
Use the existing AWS account and worker scope. Keep daily schedules and trust
unchanged when updating code. RELEASE.json records the deployed wheel/run.
