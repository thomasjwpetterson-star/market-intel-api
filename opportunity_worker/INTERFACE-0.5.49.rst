Explanation version binding — interface addendum
================================================

This extends INTERFACE-0.5.48.rst without changing endpoints or stable IDs.
Quality review reuses a pre-review explanation only when its source record hash
matches the current opportunity version. A changed forecast, quantity, period or
notice uses its newly evaluated explanation while retaining the stable episode ID.
The additional decision_explanation_source_version release gate requires the
source_basis.source_record_hash inside every decision document to equal the
decision row's source_record_hash. New explanations cannot inherit an earlier
source's period, evidence ID or negative-evidence assessment merely because the
episode identifier is unchanged. A regression covers a previously elapsed source
being amended to a current invitation under the same stable ID.
