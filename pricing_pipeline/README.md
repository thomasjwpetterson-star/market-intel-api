# SAM document-role controls

Arithmetic and source hashes establish what a row says, not whether it is a
procurement price. Government-furnished-property inventories contain unit and
extended costs which must not be treated as executed CLIN prices.

The versioned Glue transform retains non-procurement observations with an explicit
item class, no CLIN, and false price-range/promotion flags. IDs, extraction basis,
amounts and physical provenance stay intact. The parser also labels such source
rows as non-procurement evidence. Other similarly named inventory documents are
held for review rather than asserted to have the exact reviewed value basis.

The legacy parser and governed Glue entrypoint are self-contained. Their document policy
is duplicated intentionally to avoid changing job dependency/configuration or
schedules. An AST parity test ties both copies to the opportunity reader's policy.
The latter filters legacy frozen data before ORDER/LIMIT, and the private builder
retypes retained inventory observations and omits them from procurement-price
notice links. SQL generation distinguishes Athena and DuckDB regex functions.

Source baselines imported here are the independently read production objects:

- Governed transform v12: SHA-256
  `54ebad4815e081bd42107f2155c323c567230279b184842dcbc65e103fd79772`.
- Attachment parser v7: SHA-256
  `37c9fcda4924db1eb98a012c66bb19d39171ab472db1ea4f6b74ed1225de1bc1`.
- Attachment/action evidence transform v2: SHA-256
  `1bedfba3e3e5817692eb3ff80c04c51a40f91d3bc64ad2d7d0b37cfa779c2e9c`.

The source/action transform now recognizes support **services**, rather than
every occurrence of the noun SUPPORT. Its original two noun-only quantity
filters wrongly excluded the physical `THIGH SUPPORT FIERCELY` CLIN 0016 from
the governed output. The original source line already exists in Silver; it is
not a missing publisher record or an obligation. The governed transform retains
the exact reviewed presolicitation schedule as reference evidence, including
this line, without admitting any of its estimates to generic price ranges.

Numeric parser-version selection now accepts semantic suffixes such as
`-v9-source-value-basis`; otherwise a new guard could be outranked by an older
numeric-only parser version. Role/value-basis fields are retained through the
source/action evidence transform; old inputs without those fields remain valid.

`source_document_role` and `source_value_basis` describe economic meaning.
`price_basis` remains the original extraction method and record-identity input.
The reviewed 37-line planning estimate retains its real schedule CLINs and is
reference-only; a separate 19-line equipment list is held with value basis
unestablished, without asserting that it is inventory or an executed purchase.
Unreviewed source lines are explicitly labelled unreviewed, not certified as
executed by elimination. All existing amounts and IDs are preserved by this
reclassification. The recovered CLIN's ID is generated from its original
physical source row and the existing pricing identity rule.

The private enricher rebinds the downstream source view to the corrected copy
before generating notice links. This prevents the source view from bypassing
the new semantic fields. The same policy guards old frozen copies in both
reader adapters, including more than one page of stale autoeligible rows.

A separate source-automation helper searches all description fields without
programme-null/amount-sign exclusions and requires per-native reviewed closure.
It supports local reviewed candidates; alias matches never authorize inclusion.

Run the focused tests from repository root:

```sh
python3 -B -m unittest -v test_sam_pricing_document_role test_market_review_quality test_opportunity_price_document_role test_sam_price_value_basis
```

Deploying source guards, correcting the existing Iceberg records and creating a
versioned private opportunity successor are separate operations. Do not overwrite
immutable release files, delete raw evidence, refresh unrelated DLA/Main price
sidecars, change schedules, or publish an editorial report as part of this repair.
Changing these repository files does not update the independently versioned S3
scripts. A guarded manual data correction and versioned private successor require
fresh input checks, comparison with the current deployment, and live read-back.
