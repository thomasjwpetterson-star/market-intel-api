# Ask Mimir capability ontology

## Purpose

The capability ontology defines how Ask Mimir translates a technology-market question into public-record evidence. It keeps market boundaries versioned, reviewable and consistent across routing, evidence generation and atomic releases.

The ontology is stored in `capability_ontology.json`. Every governed capability declares:

- A stable capability identifier and customer-facing name.
- Natural-language routing patterns.
- Relevant Federal Supply Classification or Product and Service Codes.
- Whether an entire classification is included or descriptions must also match.
- Any wider adjacent ecosystem that should be shown separately.
- A plain-language definition of the included equipment and activity.

## Evidence boundaries

**Classification-backed market** includes all records within one or more product classifications whose official definitions align with the capability. This provides the strongest repeatable boundary available in the current data.

**Description-confirmed market** uses specified classifications to limit the search area and then requires relevant item or award language. It is suitable for identifying visible suppliers and activity, but not for claiming a complete market universe.

**Hybrid market** combines complete core classifications with description-confirmed records from adjacent classifications. The answer must distinguish the core from the adjacent evidence where their meanings differ.

Some classifications have strong award evidence but sparse DLA item coverage. In those cases, Ask Mimir uses awards and authoritative program sources to describe market structure instead of treating the short item list as the complete supplier base.

## Financial interpretation

Observed DLA procurement is calculated from matched DLA procurement records for the stated fiscal-year window. Prime obligations are shown separately and are derived from matched federal award records. Wider related award values may represent a larger contract containing the capability and are not treated as the value of the component itself.

FLIS approved-source and part-number relationships establish item coverage and source status. They do not inherit the NIIN's financial value. Observed procurement value, contract counts and observation years remain attached to the CAGE site that received the DLA award in the supplier-level procurement ledger.

The ontology does not create total-addressable-market estimates or supplier market shares. Those require a defensible denominator beyond the currently observed public records.

## Release controls

Capability evidence packs are generated before publication and included in the immutable Ask Mimir release. The ontology file and its SHA-256 hash are recorded in the release manifest. A failed rebuild leaves the previous local evidence pack intact, and the production release pointer changes only after all release files have been uploaded and verified.

## Current governed families

- Aircraft actuation and flight controls
- Aircraft braking systems
- Aircraft electrical power generation and management
- Aircraft environmental control and thermal management
- Aircraft landing gear
- Aviation engine fuel controls
- Electro-optical, infrared and night-vision equipment
- Electronic warfare equipment
- Energetic, initiation and ordnance components
- Military antennas and RF equipment
- Military avionics
- Military radar
- Missile and rocket propulsion
- Mission computing and rugged computing
- Tactical communications

Questions outside these families can still use dynamic evidence discovery. Those answers must be presented as bounded searches until the capability has been reviewed and added to the governed ontology.
