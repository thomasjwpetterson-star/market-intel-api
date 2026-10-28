# v7 candidate cache templates

These templates build private candidate artifacts. Supply the following Athena
table placeholders from **version-pinned snapshots of the same reviewed v7
reference release**:

- `__REVIEWED_VISIBILITY_TABLE__`: `canonical_label_mappings_reviewed_overlay.parquet`
- `__REVIEWED_LEGACY_BRIDGE_TABLE__`: `legacy_to_canonical_bridge_reviewed_overlay.parquet`

The hidden-platform set is the union of approved non-platform canonical labels,
reviewed hidden legacy labels, and the retired `HELLFIRE / JAGM` combined label.
The canonical label bridge also uses the pinned v7 legacy table. An unfilled
placeholder must fail the candidate build. Preserve market segment values when
masking platform identity; never infer financial attribution from a segment.

Validate all eleven mapping-dependent Main caches against the union, including
profile rollups. The original legacy bridge alone omits `HEALTHCARE`: v7
classifies it as a market sector with `platform_visible=false` in the canonical
mapping. Keep the four operational NSN sidecars in the Main release manifest.
