Promoted company/NSN catalogue connector — prepared access change
================================================================

The opportunity engine currently holds an archived reference projection derived
from an older local Main catalogue. Main now serves a newer, version-pinned
company/NSN catalogue from its promoted platform release. Reuse that existing
projection rather than creating a competing classification pipeline.

Prepared connector: opportunity_engine/company_references.py.
It checks the promoted schema-2 root, exact immutable Main manifest version and
SHA256, matching platform release, a unique NSN/CAGE entry, and exact object
versions/sizes/ETags. It archives source bytes under the existing opportunity
namespace and derives per-row SHA/row/evidence IDs. Original reference codes,
source statuses, platform attribution and price/date fields remain distinct from
qualification and data rights. Unchanged object pins skip downloads/projection.
A failed upstream check retains the previous complete snapshot with truthful
source health. No Main source or publication pointer is modified.

The existing MimirOpportunityEvidenceWorker role cannot read the promoted Main
manifest or catalogue prefixes. The proposed additional statement grants only
s3:GetObject, s3:GetObjectVersion and s3:GetObjectAttributes for:

* mimir/platform/current_manifest.json
* mimir/releases/mimir-main-platform-*/manifest.json
* governance/platform_reference/candidates/*/output/nsn_cage_reference/*

No new write permissions, role, user, credentials or bucket-wide source reads
are proposed. The existing role's other statements remain unchanged. The JSON
statement must be appended only after explicit approval; preserve the exact old
policy for rollback. The source connector then requires a controlled AWS test
and all ordinary release/readback gates before its data is served. This file
and its source projection are prepared work, not a claim of deployed automation.
