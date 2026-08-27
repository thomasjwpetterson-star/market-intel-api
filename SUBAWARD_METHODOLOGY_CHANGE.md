# Subaward methodology implementation

## Status

The description-assisted method was promoted to
`market_intel_gold.ref_company_network` on 27 August 2026. The public view keeps
the previous 27-column contract so downstream queries and Parquet schemas do
not change. `network.parquet`, `profiles.parquet`, `geo.parquet` and
`cage_locations.parquet` were rebuilt after promotion.

The source-report audit sidecar and expandable UI history remain staged work;
they are not required by the production financial aggregation and were not
included in this release.

`ref_company_network_v3` is rejected and cannot be selected through
`COMPANY_NETWORK_VIEW`. Validation showed that its same-date action rule
collapsed legitimate Ford-class line detail.

## Customer measure

Reported subcontract value for subawards dated within the selected fiscal-year
period. Values are based on first-tier subcontract reports and should not be
added to prime obligations as though they were separate federal spending.

## Implemented layers

1. `dataset_sub_contracts` remains the complete report-level source history.
2. `ref_company_network_report_history_candidate` selects the latest published
   version of each source report while retaining version counts.
3. `ref_company_network_action_audit_candidate` removes exact business copies.
4. `ref_company_network_curated_actions_candidate` applies entity resolution
   and conservative value controls.
5. `ref_company_network_description_assisted_candidate` separates supported
   re-reports from separately described work.
6. `ref_company_network` exposes the production 27-column contract used by the
   network, company, platform and Data Explorer outputs.
7. `/methodology/subcontracts` documents the measure, source, transformations
   and limitations in customer-facing language.

## Production financial method

`ref_company_network_description_assisted_candidate` is the validated upstream
view used by production. It does not use the rejected v3 same-date rule.
Instead it:

- retains the latest version of the same source report;
- removes business-identical copies;
- keeps separately described line items even when they share a subcontract
  number, subawardee and action date;
- treats reports with the same normalized description, location, subcontract
  number, subawardee and action date as versions of the same reported line,
  retaining the latest source-modified version;
- treats equal-value reports on different dates as suspected re-reports only
  when the normalized description and location also agree;
- does not infer duplicates from amount alone;
- preserves blank-description records by source identity rather than applying
  an aggressive inferred match;
- retains signed negative values as `REPORTED_NEGATIVE_ADJUSTMENT`, without
  asserting that every negative is a federal de-obligation;
- excludes an amount from the adjusted total when its absolute value exceeds
  a known prime-award ceiling, or exceeds $2 billion when no usable ceiling is
  available; and
- keeps every excluded raw report and treatment reason available for audit.

The $2 billion rule is an explicit anomaly guardrail for reports without a
usable prime ceiling. It is not a replacement amount. The current production
rule's invented $100 million fallback and ceiling substitution are not part of
this candidate.

The production promotion was validated with
`audit_description_assisted_financial_impact.sql`,
`audit_description_assisted_platform_impact.sql` and selected relationship
checks before the downstream caches were rebuilt.

### Production impact

Athena audit on 27 August 2026:

| Measure | Current | Description-assisted candidate | Change |
| --- | ---: | ---: | ---: |
| Rows | 877,483 | 911,564 | +34,081 |
| Adjusted reported subcontract value | $691.60B | $558.90B | -$132.70B (-19.19%) |
| Positive adjusted value | $691.60B | $567.33B | -$124.26B |
| Negative reported adjustments | $0 | -$8.43B | -$8.43B |
| Negative rows | 0 | 17,083 | +17,083 |
| Zero-value rows | 0 | 6,030 | +6,030 |
| Excluded anomaly rows | not explicitly excluded | 10,386 | +10,386 |

Change decomposition:

| Component | Change |
| --- | ---: |
| Description-assisted report and line selection, retaining the old positive-value substitution rule | +$23.27B |
| Replacing the old substitution rule with anomaly exclusion | -$147.54B |
| Including signed negative reported adjustments | -$8.43B |
| Net change | -$132.70B |

The reduction is therefore driven primarily by the extreme-value methodology,
not by description-assisted deduplication. The superseded production view contains
1,058 substituted rows contributing $18.37B of adjusted value from $40.31T of
raw reported values. The production method instead excludes 10,386 reports whose value
exceeds a known prime ceiling or the no-ceiling $2B guardrail.

Key-program comparisons:

| Program | Current | Candidate | Change |
| --- | ---: | ---: | ---: |
| AIM-9X | $3.41B | $2.37B | -30.55% |
| AMRAAM | $4.41B | $3.41B | -22.72% |
| Ford-class carrier | $4.26B | $5.68B | +33.34% |
| Sentinel / Minuteman III | $28.14B | $4.46B | -84.14% |
| SM-6 | $1.09B | $0.74B | -32.04% |
| Tactical Tomahawk | $1.46B | $1.17B | -19.51% |

The Ford increase reflects retention of separately described purchase-order
lines. The Sentinel reduction includes removal of an especially consequential
current substitution: a $3.007B raw Honeywell report is currently replaced by
the $13.294B prime ceiling, increasing the displayed value by approximately
$10.29B. The candidate excludes that raw report rather than substituting a
larger value.

## Superseded financial method

The pre-27 August rollback definition:

- discards zero and negative reported amounts before deduplication;
- groups by prime PIID, subcontract number, subcontract parent UEI and amount;
- keeps the latest row in each group;
- replaces values over $2 billion with the prime ceiling, or an invented
  $100 million when no ceiling is available;
- can increase an amount when the prime ceiling is larger than the reported
  subcontract value.

The rejected v3 candidate would have:

- versions records by the authoritative SAM report ID where available;
- removes business-identical re-reports;
- treats same-date alternatives as corrected reports and keeps the latest;
- conservatively treats repeated equal amounts under a numbered subcontract as
  re-reports;
- retains reported zero and negative adjustments;
- bounds extreme signed values to no more than the prime ceiling;
- excludes an absolute value over $2 billion when no prime ceiling is
  available instead of inventing a replacement value;
- retains the raw reported value and source identifiers for audit.

## Rejected v3 financial impact

Athena audit on 26 August 2026:

| Measure | Current | v3 candidate | Change |
| --- | ---: | ---: | ---: |
| Rows | 877,483 | 774,224 | -103,259 |
| Adjusted reported subcontract value | $691.60B | $586.50B | -$105.09B (-15.20%) |
| Negative adjustment rows | 0 | 16,870 | +16,870 |
| Negative adjustment value | $0 | -$11.52B | -$11.52B |

Fiscal-year value changes:

| Fiscal year | Change | Percent |
| --- | ---: | ---: |
| 2019 | -$25.61B | -25.67% |
| 2020 | -$16.86B | -12.17% |
| 2021 | -$38.47B | -21.16% |
| 2022 | -$9.07B | -12.38% |
| 2023 | -$8.19B | -11.25% |
| 2024 | -$5.45B | -7.45% |
| 2025 | -$1.29B | -3.07% |
| 2026 | -$0.15B | -1.47% |

These totals are retained as validation evidence only. They are not production
figures and should not be used to justify the v3 selection rule.

## Other rejected alternative

`ref_company_network_net_change_candidate` is retained only as an audit
experiment. It interpreted later amounts as replacement balances and converted
them to deltas. Samples showed that later reports can be separate modification
amounts, so this method created artificial negative changes and must not be used
for production.
