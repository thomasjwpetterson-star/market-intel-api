# SAM document-role controls

Arithmetic and source hashes establish what a row says, not whether it is a
procurement price. Government-furnished-property inventories contain unit and
extended costs which must not be treated as executed CLIN prices.

The versioned Glue transform retains non-procurement observations with an explicit
item class, no CLIN, and false price-range/promotion flags. IDs, extraction basis,
amounts and physical provenance stay intact. The parser also labels such source
rows as non-procurement evidence. Other similarly named inventory documents are
held for review rather than asserted to have the exact reviewed value basis.

The two legacy Glue entrypoints are self-contained. Their small document policy
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

A separate source-automation helper searches all description fields without
programme-null/amount-sign exclusions and requires per-native reviewed closure.
It supports local reviewed candidates; alias matches never authorize inclusion.

Run the focused tests from repository root:

```sh
python3 -B -m unittest -v test_sam_pricing_document_role test_market_review_quality test_opportunity_price_document_role
```

Deploying source guards, correcting the existing Iceberg records and creating a
versioned private opportunity successor are separate operations. Do not overwrite
immutable release files, delete raw evidence, refresh unrelated DLA/Main price
sidecars, change schedules, or publish an editorial report as part of this repair.
