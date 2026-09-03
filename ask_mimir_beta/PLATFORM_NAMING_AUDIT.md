# Platform and program naming audit

## Current finding

The Ask Mimir runtime currently contains 581 normalized labels across the loaded transaction,
subcontract, item-platform, item-supplier and rolled-award files. The field contains a mixture of
platforms, programs, weapon systems, engines, enterprise systems and broad modeled categories.
It should therefore not yet be treated as a governed list of physical platforms.

## Confirmed issues

- `COLUMBIA CLASS SSBN` is the correct submarine-class label.
- `COLUMBIA CLASS SSN` also occurs in the source-derived data and is incorrect. The affected
  records must be reassigned upstream before Columbia-class totals are presented as complete.
- Case-only aliases occur, including labels such as `M1 ABRAMS` and `M1 Abrams`.
- `UNMAPPED` and `REVIEW NEEDED` are workflow statuses, not platform names.
- Labels such as `T700`, `J85`, `AFWAY` and `GFEBS` demonstrate that the field also contains
  engines, systems and programs.
- Related labels can carry useful specificity. For example, `M109 PALADIN` and `M109A7 HOWITZER`
  should share a family while retaining their source label or variant, rather than being silently
  collapsed.

## Release safeguards

- Ask Mimir excludes `UNMAPPED` and `REVIEW NEEDED` from platform search.
- Case-only duplicates are collapsed in the search catalogue.
- Common Columbia and misspelled Colombia searches resolve to the correct `COLUMBIA CLASS SSBN`
  label.
- The incorrectly labelled `COLUMBIA CLASS SSN` records are not silently merged into financial
  totals. That correction requires a versioned upstream mapping update and a rebuilt release.

## Required governed model

Create a versioned nomenclature registry with a canonical identifier, display name, entity type,
family or parent identifier, source alias, review status and effective dates. Preserve the original
source label for lineage. Only analyst-approved aliases should alter aggregation; text similarity
can propose candidates but must not merge them automatically.

## Next upstream task

Build and apply the versioned nomenclature registry before the next metric release. Start by
correcting `COLUMBIA CLASS SSN` to the Columbia-class SSBN canonical identifier, removing workflow
statuses from the published dimension, and retaining programs, systems, engines and physical
platforms with explicit entity types. Rebuild the affected Athena views and all dependent Parquet
artifacts atomically, then compare record counts and financial totals before promoting the release.
