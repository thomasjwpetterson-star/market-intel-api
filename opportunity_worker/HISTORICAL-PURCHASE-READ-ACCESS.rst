Prepared historical purchase coverage — additional read access
=============================================================

This extends the separately prepared promoted-Main catalogue connector. Neither
connector is deployed until the worker's additional source reads are approved.
No new role, credential, write scope or upstream data change is required.

Additional exact source
-----------------------
s3://a-and-d-intel-lake-newaccount/bronze/dla/contract_history/2025ContractHistory.txt

Required actions: s3:GetObject, s3:GetObjectVersion, s3:GetObjectAttributes. The
reader pins the actual object version and size, verifies the downloaded bytes,
archives the original under the already-owned opportunity source namespace and
records SHA256, original version and original row/PO/line provenance. It never
invents a government download URL for a held file without one.

The source contains public U rows and other security classifications. Only public
U rows are projected. Date/identity/schema failures and conflicting financial
versions abort the new projection. Repeated part mappings count the financial
line once; ambiguous part references stay null. Reported NSN and NIIN must agree;
a different separate FSC is preserved as a source classification discrepancy,
not used to manufacture a replacement NSN.

A missing source permission or failed first validation cannot publish a partial
history. Later retrieval failures retain the last complete source with original
transaction dates and explicit failed-check health.

Historical value semantics
---------------------------
The history also retains already-observed monthly physical purchases when source
snapshots roll forward. Newer current observations take precedence over retained
versions of the same PO/line/date/NIIN/seller. A corrected annual object replaces
older rows from that annual source, including rows withdrawn from its successor.
Financial source provenance remains attached to each row.

Catalogue-sales evidence retains sixteen months so the existing 90-day reporting
lag comparison can use its complete twelve-month window. The separate trailing
sales summary still includes exactly the last twelve months. The product-market
comparison uses its stated period, excludes direct-seller purchases from the
addition and does not annualise missing months. A regression covers August
purchases omitted by a November-start trailing-year filter.

These historical purchases do not create future opportunities or reprice the
current future-demand scenarios. They improve the observed product-market scale
and its three-times coverage comparison. They are not manufacturer revenue,
qualification, available workshare or expected company pipeline.

Activation
-----------
Merge the prepared source-coverage branch only after approval, package a new
worker version and run the native refresh. A native run is necessary to rebuild
company profiles for any newly referenced CAGEs from Main. Validate exact-part
roles, physical-sale uniqueness, aligned periods, raw archives, company examples
and all ordinary release gates before publication. Preserve the existing current
pointer on failure. Restore the daily native schedule after the successful test.
