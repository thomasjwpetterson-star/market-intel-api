"""The small, explicit contract for every raw source used by Mimir."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceSpec:
    source_id: str
    role: str
    cadence: str
    status: str
    canonical_owner: str
    downstream: tuple[str, ...]
    promotion: str
    notes: str = ""


SOURCES: dict[str, SourceSpec] = {
    "usaspending-prime-daily": SourceSpec(
        source_id="usaspending-prime-daily",
        role="canonical",
        cadence="daily",
        status="live",
        canonical_owner="USAspending",
        downstream=("global_spend_transactions", "main-mimir", "ask-mimir"),
        promotion="existing Bronze-to-Silver job",
        notes="Rolling 45-day DoD transaction feed; retain unchanged.",
    ),
    "usaspending-contract-archive": SourceSpec(
        source_id="usaspending-contract-archive",
        role="canonical-reconciliation",
        cadence="monthly",
        status="candidate-ready",
        canonical_owner="USAspending",
        downstream=("prime-contracts", "sub-contracts", "company-network"),
        promotion="candidate transform, compatibility gate, then explicit promotion",
        notes=(
            "Official full current/prior-FY bulk download. It contains the complete CSV "
            "shape used by historical loads and closes corrections outside the "
            "daily 45-day window."
        ),
    ),
    "dod-contract-announcements": SourceSpec(
        source_id="dod-contract-announcements",
        role="leading-indicator",
        cadence="daily",
        status="live-protected",
        canonical_owner="US Department of Defense",
        downstream=("ask-mimir",),
        promotion="independent existing release path",
        notes="Must remain independent of Main Mimir refresh success or cadence.",
    ),
    "sam-contract-awards": SourceSpec(
        source_id="sam-contract-awards",
        role="enrichment",
        cadence="monthly",
        status="adapter-required",
        canonical_owner="USAspending",
        downstream=("award-enrichment-candidate",),
        promotion="join coverage/uniqueness gate; never replace USAspending facts",
        notes="SAM Contract Awards is enrichment only and may be delayed for DoD.",
    ),
    "sam-entity-registration": SourceSpec(
        source_id="sam-entity-registration",
        role="reference",
        cadence="monthly",
        status="source-url-required",
        canonical_owner="SAM.gov",
        downstream=("ref_sam_entities", "company-network"),
        promotion="candidate reference table, join regression, explicit promotion",
        notes="Use Public V2 full extract; quarterly is the fallback cadence.",
    ),
    "sam-current-opportunities": SourceSpec(
        source_id="sam-current-opportunities",
        role="canonical",
        cadence="daily",
        status="live",
        canonical_owner="SAM.gov",
        downstream=("opportunities", "main-mimir", "ask-mimir"),
        promotion="existing state machine",
    ),
    "dla-solicitations": SourceSpec(
        source_id="dla-solicitations",
        role="canonical",
        cadence="daily",
        status="live",
        canonical_owner="DLA",
        downstream=("solicitations", "main-mimir", "ask-mimir"),
        promotion="existing Glue job",
    ),
    "dla-bulk-reference": SourceSpec(
        source_id="dla-bulk-reference",
        role="reference-and-history",
        cadence="monthly",
        status="externally-managed-in-progress",
        canonical_owner="DLA",
        downstream=("DLA Silver candidates", "NSN enrichment"),
        promotion="excluded from this change while DLA work is in progress",
    ),
    "budget-documents": SourceSpec(
        source_id="budget-documents",
        role="reference",
        cadence="annual",
        status="manual-backfill",
        canonical_owner="US budget publishers",
        downstream=("program outlook", "ask-mimir"),
        promotion="annual immutable backfill with document-level manifest",
    ),
}
