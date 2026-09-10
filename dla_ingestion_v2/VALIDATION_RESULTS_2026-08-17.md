# DLA Snapshot Validation: 2026-08-17

## Source And Run Identity

- Bronze snapshot: `s3://a-and-d-intel-lake-newaccount/bronze/dla/monthly_procurement/source_snapshot_date=2026-08-17/`
- Glue job: `dla_monthly_procurement_snapshot_v2`
- Glue run: `jr_7b99ca57ab4ffd1e8f4e111cdf3d3a99282323a59232fedb5d0443157f36df0f`
- Glue result: `SUCCEEDED`
- Production promotion query: `767f1178-95a3-4d5b-9c45-79bdd8add640`
- Rollback view: `market_intel_silver.view_dla_contract_history_financial_legacy_20260910`

## Reconciliation

| Check | Result |
| --- | ---: |
| Contract-history physical rows | 713,274 |
| Procurement-history physical rows | 713,274 |
| Unique financial lines | 597,099 |
| Distinct financial line IDs | 597,099 |
| Contract-only keys | 0 |
| Procurement-only keys | 0 |
| Parsed quantity failures | 0 |
| Parsed net-price failures | 0 |
| Parsed award-date failures | 0 |
| Award-date coverage | 2026-01-01 to 2026-08-17 |
| Observed procurement value | $6,612,421,074.02 |

## Part-Reference Attribution

| Status | Financial lines | Share of lines | Observed value | Share of value |
| --- | ---: | ---: | ---: | ---: |
| Single reported part reference | 553,486 | 92.70% | $5,549,553,646.26 | 83.93% |
| Multiple reported part references | 43,613 | 7.30% | $1,062,867,427.76 | 16.07% |

The single-part view reproduced every eligible line and its value in every fiscal
year. It does not contain multi-reference financial lines. Financial values in this
view describe an observed DLA procurement association with a uniquely reported part
reference; they do not by themselves identify the manufacturer of the delivered item.

There are 595 financial lines with more than one NSN string in the expanded
contract-history references. Inspection showed that these are alternate FSC prefixes
attached to the same NIIN. The financial NSN is constructed from the procurement
line's FSC and NIIN, while every source-reported NSN remains in the v2 audit table.

## Production Impact

Calendar years 2019 through 2025 retained identical row counts. Calendar 2026 changed
from 111,678 lines and $1.257B through February 17 to 597,099 lines and $6.612B through
August 17.

After the existing USAspending/DLA award-overlap control, the FY2026 DLA lane in
`global_spend_transactions` contains 220,964 rows and $4.752B through August 17. The
NIIN/CAGE fiscal view contains 802,938 lines and $9.743B for FY2026, including the
October-December 2025 portion of the fiscal year.

Post-promotion smoke query: `e0dfc1ab-b30a-47ba-b2b8-da5b10e6041c` (`SUCCEEDED`).
