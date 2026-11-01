Explicit capability inquiries — qualification interface addendum
================================================================

This extends INTERFACE-0.5.49.rst. An otherwise evidenced company/work match to a
current, explicit agency capability/source inquiry can be served without an NSN
or part identifier. It remains stage=qualification_opening: an invitation to
engage, not proof of production qualification or an invitation to bid.

This exception does not convert OEM-only/traceability inquiries into permission
to manufacture alternatives. Explicit alternate-source qualification, an exact
named source or existing source authority remain separate evidence. Generic
waiver notices without an item/scope entry route remain review-only. The release
gate checks this distinction across all retained company/work matches.

Quality review now stores pre_quality_review_category so later supported source
changes can restore a prospect from review. For legacy rows lacking that field,
the original category is reconstructed from the retained entry route and source
stage; an unestablished downstream entry route remains market context. An earlier
review verdict is not silently sticky after the evidence supports engagement.

The promoted-Main catalogue connector remains separate prepared work on branch
opportunity-promoted-company-catalogue, pending approval of its additional read
paths. This source-inquiry correction requires no new permissions.
