Monthly demand refresh and company matching — interface addendum
================================================================

This adds to INTERFACE-0.5.46.rst. Existing routes and stable company/episode IDs
remain compatible. No database migration or additional source permissions are
required. The authenticated /opportunities page remains unlinked and noindex.

Company relevance and qualification
----------------------------------
Item demand is joined through exact CAGE/NIIN catalogue references independently
of the historical seller. Where RNVC/RNSC are held, current RNVC 2 references are
required and RNSC B is excluded from the matching route. Old seed links cannot
preserve a match after its reference becomes inactive. The original source codes,
reference evidence and source versions remain inspectable. Manufacturing/design
control, data rights, agency approval, configuration and quote eligibility remain
separate facts; an item association does not establish all of them.

Generic SAM component-family matches without an item identifier, a qualification
invitation or an explicit named source remain review/context, not a demonstrated
replacement route. A source-stated small-acquisition limit prevents a larger
historical comparator from becoming the current ranking value. Such records use
commercial_value_kind=historical_context_source_limit and retain the comparator
only as historical evidence. No numerical legal threshold is inferred.

Published deadlines are checked against the current UTC date/time when served,
as well as at release validation. Source date precision remains explicit; fiscal
solicitation/award planning periods are not response deadlines.

Monthly source and bounded update operation
------------------------------------------
The worker checks the official DLA Electronic Reading Room for one same-edition
set of forecast, stock/backorder, reorder-point, procurement-history and contract
part-history archives. ZIP layout, source headers, financial keys, public-release
flags, dates and physical-line/part reconciliation are validated before a new
monthly source snapshot is installed. Ambiguous part associations are retained as
unresolved, never arbitrarily assigned to a manufacturer's exact part.

Source inputs are immutable under sources/content/sha256=..., with a separate
sources/dla_monthly/current.json pointer. This pointer stages inputs; it does not
publish opportunity results. Parquet hashes and stored raw-archive SHA256s are
rechecked before opportunity publication. A changed source must pass all release
gates before the existing atomic opportunity current pointer advances.

On retrieval failure the last complete monthly snapshot remains available, with
its original edition and business dates. Each source has refresh_health containing
status, attempted_at and source_release. A failed check is not evidence of zero
demand and is not presented as a new monthly release. HTTP/WAF retrieval failures
remain operational coverage gaps even when a complete snapshot is retained.

The existing daily native operation includes this source check. An additional
exclusive --refresh-items operation, Glue --opportunity-refresh-mode=items, can
refresh monthly item demand/prices/stock without repeating the full prime and
subcontract derivation. It preserves the parent's company and non-item source
graph and assessment date; reruns item matches, qualification review, price and
stock contexts, request links, market coverage, events and release validation.
item_refresh_parent identifies the parent; item_refresh_nonitem_graph_preserved
records that preservation. It must not be interpreted as newly refreshed prime
or subcontract evidence.

Consumers
---------
The existing companies/{cage}/overview exposes published demand, source forecasts,
conditional annual-usage outlooks, planned package bands and aligned business
coverage separately. Consumers must retain these distinctions. Company-home
normalisation now carries source bands and annual-usage semantics, and suppresses
historical source-limit comparisons as current opportunity amounts. The ordinary
opportunity feed, evidence routes, stable IDs, saved records and complete-category
CSV remain the integration contract. Company-dashboard layout, MCP deployment and
marketing are outside this change.

Large-prime non-item discovery remains materially incomplete. Generic capability
overlap or a large annual-obligations benchmark must not become invented packages,
qualification, company workshare, win probability or forecast revenue. The three
times scale comparison remains a diagnostic for coverage, not a forecast target.
