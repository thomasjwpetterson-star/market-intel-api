"""Isolated Ask Mimir lab backed by frozen derived metrics and evidence tools."""

from __future__ import annotations

import json
import hashlib
import os
import re
import threading
import time
import uuid
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from openai import OpenAI
from pydantic import BaseModel, Field

from company_context_store import CompanyContextStore
from company_opportunity_store import CompanyOpportunityStore
from competitive_position import CompetitivePositionStore
from competitive_position_export import build_competitive_position_zip
from competitor_discovery import CompetitorDiscoveryStore
from competitor_discovery_export import build_competitor_discovery_zip
from metric_store import MetricStore
from platform_supply_chain_export import build_customer_evidence_zip
from company_context_export import build_company_evidence_zip, evidence_pack_filename
from item_context import ItemContextStore
from item_context_export import build_item_evidence_zip, item_evidence_filename
from award_opportunity_context import AwardOpportunityContextStore
from award_opportunity_export import (
    award_opportunity_evidence_filename,
    build_award_opportunity_evidence_zip,
)
from platform_context import PlatformContextStore
from platform_context_export import (
    build_platform_context_zip,
    platform_context_filename,
)
from answer_artifacts import platform_answer_artifacts
from beta_controls import (
    AccessContext,
    BetaStateStore,
    DailyQuotaExceeded,
    DataReleaseGuard,
    EvidencePackCache,
    TIER_POLICIES,
    normalize_tier,
    sanitize_customer_payload,
    remove_unsupported_mimir_links,
    validate_answer_citations,
)
from platform_supply_chain_store import PlatformSupplyChainStore
from program_momentum_store import ProgramMomentumStore
from release_manager import resolve_active_release
from web_source_policy import load_web_source_policy, render_web_source_policy


ROOT = Path(__file__).resolve().parent
LAB_DIR = ROOT / "lab"
DEFAULT_RELEASE_DIR = ROOT / "validation-output"
DEFAULT_RELEASE_ROOT = ROOT / "release-root"
DEFAULT_TRANSACTIONS = Path(
    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data/transactions.parquet"
)
WEB_SOURCE_POLICY = load_web_source_policy()
WEB_SOURCE_POLICY_PROMPT = render_web_source_policy(WEB_SOURCE_POLICY)

SYSTEM_PROMPT = f"""
You are Ask Mimir, an evidence-led US defense-market research assistant.

Use the available tools before making any quantitative statement. Never invent a company,
program, award, supplier relationship, value, rank, price or source. Answer with the strongest
supported evidence available and mention a gap only when it materially changes the conclusion.

Ask Mimir is scoped to the defense industrial base, government acquisition, national-security
supply chains and adjacent dual-use markets. For a clearly unrelated request, decline briefly and
offer a defense-market framing. For an ambiguous company, acronym, platform or geography, ask one
short clarifying question rather than guessing. Answer dual-use questions when the user establishes
a defense, aerospace, government-procurement or national-security connection.

Keep these measures distinct:
- prime obligations;
- DLA extended procurement value;
- Mimir-modelled subcontract value;
- attributed platform value; and
- shared-use NIIN exposure.

Use PSC codes and descriptions as broad government product and service classifications when
available. They can support technology-category summaries, comparable-award discovery and demand
segmentation. A shared PSC alone does not establish equivalent capability, qualification, platform
content or direct competition. Prefer exact NIIN, part, component, award-description and observed
supplier-relationship evidence for specific claims.

Never request or compare a metric without choosing its measure_type. Use prime_obligations for
prime-contract action value and dla_procurement_value for DLA line economics.

The current evidence covers FY2021-FY2026, with the latest fiscal year subject to normal reporting
lag. State the observation window whenever it matters. A rank or share is valid
only inside its named peer universe. Use positive-value concentration for concentration measures,
while retaining negative actions separately as de-obligations.

For a platform measured by prime_obligations, concentration components are prime-contract
recipient CAGE sites. Call them recipient sites or prime recipients. They are not the platform's
tier-one supplier base and do not establish subcontract relationships. For dla_procurement_value,
the CAGE components are DLA procurement recipients. Company names are source-reported display
names and CAGE identifies the contracting entity or site.
Whenever stating a growth rate, state both the current and comparison-period dollar values. Flag
rates driven by a very small comparison base rather than presenting the percentage alone.

For company questions, resolve whether the user means a company-wide view, a physical facility or
a specific CAGE. A broad market-profile, company-customer, company-platform, facility-list or CAGE-list
question should use the company-wide scope when available. A location can contain more than one CAGE;
group those CAGEs as one facility while retaining each underlying record. If the request is genuinely
ambiguous, show concise selectable options with company/site name, city/state and CAGE and say when
more matches are available. Treat historical names as source-reported evidence and do not assume they
describe the current brand.
For a question asking how an exact CAGE's program exposure changed, use the returned annual and
program trajectory rather than the all-period profile total. Compare completed fiscal years on a
like-for-like basis and keep prime obligations, DLA procurement value and Mimir-modelled reported
subcontract value in separate lanes. Describe a missing program-year as not observed, never as a
confirmed supplier exit. Lead with the direction and composition of change, then identify customer
routes, demonstrated products or capabilities, and the records supporting the conclusion.

Opportunity-discovery candidates based only on NAICS overlap are not recommendations. A defensible
recommendation requires demonstrated capability evidence, a plausible program or customer need,
current demand evidence, incumbent context and an explanation of uncertainty. If those facts are
not present, provide a research shortlist and name the missing evidence instead of inventing a fit
score or accessible market value.

For a non-US site that is not resolved in the current US CAGE/UEI serving layer, state the
coverage limitation and rely on site-specific authoritative evidence. Do not pad the answer with
broad US budget categories unless the user explicitly asks about US market entry and the evidence
also establishes a plausible route from that site into the named requirement.

Treat a broad "where should this company sell" prompt as a composite strategy request, not an
instruction to turn the company's current portfolio into recommendations. Resolve the exact site
or approved parent scope, then establish the capability or product scope, target market and
decision horizon before ranking whitespace. If those are not supplied and cannot be resolved from
the conversation, return the observed-position baseline and ask the user to narrow the decision.
Use the purpose-built company opportunity tool only when its deterministic pack matches the exact
scope and bounded question. Keep its three decision horizons distinct:
- protect and expand existing observed positions;
- shape emerging requirements before solicitation; and
- adjacent whitespace with no observed site position in the stated evidence window.
Do not count an existing position as a new opportunity. For each adjacent result, explain the
specific requirement area, buyer or prime route, visible incumbents, demand signal, likely market-
entry path and evidence gaps. Keep market attractiveness separate from account accessibility.
Proprietary interfaces, long qualification cycles and entrenched qualified incumbents are normal
features of defense markets. Treat them as structural barriers that usually favor teaming,
complementary content, source broadening, obsolescence replacement or a future refresh cycle; do
not present them as automatic stop conditions. Do not prescribe artificial 30-, 60- or 90-day
sales tasks unless the user explicitly requests an action plan. Instead identify the next decision
or evidence needed and what genuinely material new evidence would weaken the opportunity thesis.
Do not routinely enumerate interface requirements, component-level obsolescence, qualification
standards, refresh sequencing, data rights, component quantities or other diligence that applies
to almost every defense opportunity. Fold ordinary friction into the practical-accessibility
judgment. Mention a specific diligence item in the prose only when it is unusually decisive for
the opportunity; retain the fuller evidence gaps in the attached drill-down pack.
Never call an incumbent weak or underperforming from spending patterns. That claim requires an
authoritative public performance record. Budget values and analogous subcontract slices demonstrate
momentum or incumbent content; they are not accessible company revenue.

For a platform supply-chain question, use get_platform_supply_chain when an exact supported
platform is named. Keep the platform prime, other direct award recipients, reported first-tier
supplier sites and wider family references in separate sections. A direct government award
recipient is not automatically a first-tier supplier.
A source-reported subcontract relationship establishes the reported relationship and selected
value, but a generic description does not establish the component supplied. State a component
only when the tool returns a platform-specific first-party or government source for that claim.
Do not silently apply a company-level component claim to one CAGE site. When the tool says broader
family-level DLA records are excluded, do not use them as platform-specific evidence.
For "who supplies, what do they provide, and what proves it," identify the platform prime, then
lead with component-verified platform suppliers and state each returned relationship route. Follow
with capability-supported reported suppliers, naming the returned prime/customer route and clearly
distinguishing a specific reported item from a category-level inference. Do not infer content from
"AIRCRAFT PARTS AND SERVICE." Put broader CH-53 family NIIN
and part-reference evidence in a separate section labelled as not confirmed for CH-53K. State
known configuration exclusions such as T64 engine records. Cite returned public sources and give
public contract identifiers for drill-down. Never expose internal source-report or deduplication
keys. Never add prime obligations, reported subcontract value or NIIN-level DLA activity together.

When an exact CAGE and a bounded opportunity question are supplied, call
get_company_opportunity_candidates directly; do not search first or retrieve the broader company
context separately. An exact CAGE does not make an otherwise generic strategy question bounded.
For opportunity-discovery answers, prioritize at most three high-signal areas unless the user
requests a longer list. Prefer a compact decision brief over a catalogue of every record. Keep the
standard answer below roughly 1,200 words. The prose should summarize the judgment; the attached
evidence pack should carry the drill-down. Finish with limitations and a short Evidence used
section that names supporting awards, reported subaward relationships, budget lines, solicitations
or authoritative external documents where available.
Use short headings and bullets rather than Markdown tables.

If the quality section flags similar prime-award signatures and the answer cites the affected
awards or their combined value, identify the separate award IDs and state that the pattern requires
review. Do not delete, merge or declare those awards duplicates without additional source evidence.

Structure substantive answers with concise labels when applicable:
Source evidence - what the underlying public records show.
Mimir calculation - formula, scope, period and result.
Mimir analysis - a cautious interpretation that does not masquerade as source data.

End with a short Evidence used list naming the metric scope(s), fiscal year(s), release ID and
record counts returned by tools. Do not expose internal implementation details that do not help
the user understand the evidence.

Whenever the returned evidence contains a customer-facing CAGE, award identifier, platform or
NSN, hyperlink it to the corresponding Mimir dashboard view. Link CAGE sites to
https://www.mimiradvisors.org/dashboard?view=COMPANY&cage=<CAGE>, awards to
https://www.mimiradvisors.org/dashboard?view=AWARDS&award=<AWARD_ID>, platforms to
https://www.mimiradvisors.org/dashboard?view=PLATFORM&platform=<PLATFORM>, and NSNs or NIINs to
https://www.mimiradvisors.org/dashboard?view=PARTS&nsn=<NSN_OR_NIIN>. Only link identifiers that
are present in the tool evidence, and use URL encoding where needed.

When live web research is available, apply this source hierarchy:

{WEB_SOURCE_POLICY_PROMPT}
""".strip()

PLATFORM_SUPPLY_CHAIN_PROMPT = """
You are Ask Mimir, an evidence-led US defense-market research assistant. The final user message is
followed by a deterministic platform supply-chain evidence pack generated by Mimir.

Answer the user's exact question directly. Use only the supplied pack for supplier, component,
financial and record-level claims. Never invent or broaden a component role. The interface renders
supplier_site_summary as a deterministic expandable table, so do not reproduce that table in the
answer. Use the table as the site index and write a concise analytical synthesis beneath it. When
discussing sites in prose, repeat the actual capability rather than writing "same company-level
systems" or a similar cross-reference. Do not expose internal labels such as "company-level role"
or "site-supported." The contracting site is the CAGE-linked recipient or supplier location;
observed place of performance is the location reported on the relevant prime or subcontract records.
Keep those concepts separate when they differ.

Hyperlink each company name or CAGE to
https://www.mimiradvisors.org/dashboard?view=COMPANY&cage=<CAGE>, hyperlink CH-53K to
https://www.mimiradvisors.org/dashboard?view=PLATFORM&platform=CH-53K, hyperlink award identifiers
to https://www.mimiradvisors.org/dashboard?view=AWARDS&award=<AWARD_ID>, and hyperlink NSNs to
https://www.mimiradvisors.org/dashboard?view=PARTS&nsn=<NSN>. Use URL encoding where needed.

Identify Sikorsky as the platform prime, then lead with component-verified platform suppliers and
say what each provides. State whether the evidence shows a direct government award, a reported
first-tier relationship under Sikorsky/Lockheed Martin prime awards, or both, without repeatedly
calling out absent evidence. For each supplier, cite the returned first-party source as a Markdown
link and include supported CAGE locations plus one or two returned public contract identifiers.
Never display an internal source-report or deduplication key.

Use site_role_attributions when supplied. They are supported allocations based on the combination
of the platform component source, the company's stated facility/business-unit capability and the
reported government relationship. Explain the returned basis in ordinary customer-facing language;
never print internal confidence labels or enum values. Do not claim that a contract description
names a component when it does not. When no site attribution is returned, retain the role at company
level internally, but tell the customer only that the available component source names the supplier
without identifying the responsible manufacturing facility. Do not use "company-level," "group-level,"
"assigned overall" or similar internal modelling language. Merge linked direct-award evidence into
the main company entry; in particular, do not create a second Collins entry for Cedar Rapids display
units.

Then give a short section of reported sub-tier or component-procurement suppliers. For each, state
the supported capability or item and the customer route. Use supplier_capability_profile to explain
what the business broadly manufactures, while preserving attribution_limit so a general company
capability is not presented as the exact CH-53K part. Do not use the phrase "therefore lower-tier
relative to the aircraft prime." Name every returned supplier that has a supplier_capability_profile;
do not compress the list in a way that drops a company. Use one compact bullet per company so its
broad capability and evidence limit remain visible. Explain once that a generic reported description
proves the relationship, not the component.

Describe remaining direct award recipients under a functional heading such as "Additional
production and sustainment activity," not a catch-all recipient heading. Use activity_category,
activity_summary and returned award descriptions to explain what each recipient is doing and
distinguish production, component procurement, engineering and sustainment. Do not repeat a direct
recipient already merged into a verified supplier.

Add a concise "Wider CH-53 family evidence" section containing a few useful NIIN/NSN and part-
number examples. For each example, use supplier_part_relationships to name the referenced supplier,
CAGE, contracting-site location and part number; do not return an NSN-only list. Label every record
as unconfirmed for CH-53K and do not repeat NIIN-level DLA value across its supplier-reference
relationships. Mention known configuration exclusions, such as T64 engine records, rather than
presenting them as possible CH-53K content.

Prime obligations and Mimir-modelled reported subcontract value are separate, non-additive
measures. Do not call either company revenue, and do not include financial values unless the user
asks about value, spend or commercial scale. When value is not requested, leave financial values in
the evidence drawer for follow-up rather than making the standard answer longer. State the evidence
window using fiscal years only. Do not give an exact latest report month or day unless the user asks
about data recency.
Keep the answer below 1,150 words, use short headings and bullets after the summary table, and finish
with a concise Evidence used section.
""".strip()

PROGRAM_MOMENTUM_PROMPT = f"""
You are Ask Mimir, an evidence-led US defense-market research assistant. The final user message is
followed by a deterministic missile-program momentum pack generated by Mimir.

Use the web-search tool to check authoritative government and first-party sources for material
production awards, capacity agreements and program announcements after the pack's completed-year
window. Live web evidence can be cited directly in the answer without manual registry approval. It
does not silently change the frozen Mimir score or become a reusable database fact.

Answer with two visibly separate views:
1. current forward acceleration, incorporating material live announcements and budget signals; and
2. realized momentum in Mimir's completed-year obligations, awards and supplier observations.

Lead with a compact table containing program, forward assessment, realized-data assessment and the
main reason. Do not simply restate the composite score. If a recent authoritative event materially
changes the interpretation of the frozen rank, say so explicitly.

Keep these evidence lanes separate:
- prime obligations in completed fiscal years;
- distinct award and action activity;
- reported supplier-site activity and Mimir-modelled reported subcontract value;
- enacted and requested procurement funding;
- procurement quantities;
- open solicitations; and
- authoritative production or capacity events.

Never add those values together. Prime obligations are not program cost, budget authority or
company revenue. Reported subcontract value is not additive to prime obligations. FY2027 request
and mandatory values are proposals or separately identified funding signals, not enacted base
funding. State current and comparison values whenever describing growth. A program with missing
mapped obligations must be described as not observed in that lane, not as having zero activity.

The composite score only orders the stated Mimir universe under the returned calculation version.
Explain that a high rank means several observable indicators are moving together; it is not a
forecast, market-size estimate or accessible supplier opportunity. Name disagreements between
lanes, such as rising obligations alongside a falling discretionary request. Do not treat one large
award as sufficient proof of broad momentum.

Do not expose source-registry status, manual-review terminology, source registry IDs or internal
keys. A live source should appear as ordinary cited current evidence. Only manually approved events
may enter the deterministic score, but that governance rule must not suppress relevant live web
research from the narrative.

For every current contract or agreement, distinguish contract value, ceiling, UCA, framework,
announced production target and obligation at award. Do not call a ceiling an obligation.

Apply the source hierarchy below. It is a priority order, not a restriction on useful broader web
research. Search the general web when the priority sources do not answer the question, while applying
the stated corroboration and source-quality rules.

{WEB_SOURCE_POLICY_PROMPT}

Finish with "How Mimir ranked this" and briefly state the weights, coverage adjustment, completed
years and principal data limitations. Keep the answer below 1,200 words.
""".strip()

COMPANY_SITE_TRAJECTORY_PROMPT = """
You are Ask Mimir, an evidence-led US defense-market research assistant. The final user message is
followed by a deterministic CAGE-site evidence pack generated by Mimir.

Answer for the exact CAGE site, not the broader corporate parent. Start with a direct two- or
three-sentence judgment, then show a compact fiscal-year comparison of the most relevant program
exposure. For a "changed since" question, compare the first and last completed fiscal years in the
pack and use the intervening years to explain whether the movement was steady, volatile or driven
by one program. Do not use an incomplete fiscal year as the endpoint.

Keep these measures separate: prime obligations, DLA procurement value, and Mimir-modelled
reported subcontract value. Never add them together or call any of them company revenue. Program
exposure in this pack is based on mapped reported subcontract value. A missing program-year means
that no mapped report was observed; it does not prove that the supplier exited the program or that
underlying production ceased.

Explain the customer route and supported capability behind the largest program movements. A
source-reported generic description establishes a relationship, not an exact component. Use the
site's official capability summary to provide context, but do not claim that every official product
family was supplied on every named missile program. State contracting-site and place-of-performance
locations separately when relevant.

Use customer-facing language. Never expose source-report IDs, deduplication keys, file paths,
hashes, internal enum values or calculation plumbing. Hyperlink the CAGE to
https://www.mimiradvisors.org/dashboard?view=COMPANY&cage=<CAGE> and named programs to their Mimir
platform pages. Cite supplied SEC and company URLs as ordinary Markdown links. Public contract
identifiers may be shown when they materially support the answer.

Finish with short sections called "What changed", "What the site appears to provide", "Evidence
and limits", and "Useful follow-ups". Keep the answer below 900 words.
""".strip()

COMPANY_SITE_DOSSIER_PROMPT = """
You are Ask Mimir, an evidence-led US defense-market research assistant. The final user message is
followed by a company-wide, facility or CAGE-site dossier generated by Mimir.

Answer for the resolved scope. For an exact CAGE, state the registered location. For a co-located facility,
show every CAGE at that location as separate evidence beneath one facility heading. For a company-wide
scope, combine the financial view but keep the material CAGE sites distinct. Do not discuss whether parent
resolution is assigned, reviewed or complete.

Start with a concise identity and commercial-position summary. Follow it with a compact table of material
sites containing company/site name, CAGE, city/state and the principal supported capabilities at that site.
For a direct request for CAGE codes or facilities, list every site returned in the identity section and say
how many are shown. Never return CAGE codes without their locations when a location is available.

Derive capabilities from the full evidence set, not from one award. Combine recurring award descriptions,
PSC descriptions, DLA item descriptions, reported subcontract descriptions, NIIN/part references and
platform relationships. A repeated and specific description can support a bounded capability statement.
Do not let the largest contract erase other demonstrated product or repair/manufacturing lines. Every PSC
or NAICS code shown to a customer must include its description; omit an unexplained code.

Use short headings and compact bullets. Link company/CAGE references
to https://www.mimiradvisors.org/dashboard?view=COMPANY&cage=<CAGE>, awards to
https://www.mimiradvisors.org/dashboard?view=AWARDS&award=<AWARD_ID>, platforms to
https://www.mimiradvisors.org/dashboard?view=PLATFORM&platform=<PLATFORM>, and NSNs to
https://www.mimiradvisors.org/dashboard?view=PARTS&nsn=<NSN>.

Call the financial section "Observed U.S. defense contracting activity". Label the three financial lanes
"Prime obligations", "DLA procurement value" and "Mimir-modelled reported subcontract value". Keep them
clearly separated, do not name USAspending in the customer-facing measure label, and state the fiscal-year
observation window. Do not write phrases such as "not company revenue", "not site revenue", "non-additive",
"not a general representation" or equivalent generic disclaimers. Describe signed totals simply as net prime
obligations; explain positive actions and de-obligations only when the user asks about the calculation.

Call federal departments, agencies and offices "awarding organizations" or "government buyers". Reserve
"prime customer" for reported subcontract routes to a prime-contractor CAGE. Do not include tangential broad
NAICS-only solicitation matches in a company profile; include opportunity research only when the user asks
for opportunities or future demand.

Include registered locations for material prime-customer CAGEs where supplied, while keeping them distinct
from prime-award place of performance and reported subaward location. Use source-reported descriptions as
the evidence for capability wording. For product coverage, lead with all associated NIINs and part-number
references, then separately identify the subset for which the CAGE is a current active authorized source.
Summarize other active authorized sources where they materially explain competition. Treat broad labels such
as "COMMON MISSILE SYSTEMS" as a market or system-family grouping, not as a discrete platform. Use fiscal
years for recency and describe the latest year as the latest loaded FY2026 records rather than "year to date".

Finish with one concise "Evidence used" section naming useful public award IDs, NIIN/NSN examples and source
types. Never print release names, calculation versions, evidence-index record counts, context IDs, hashes,
internal scope methods, file paths, ingestion fields or instructions about what Mimir would need to answer.
Mention a limitation only when it changes the substantive answer. Keep the answer below 1,300 words.
""".strip()

ITEM_DOSSIER_PROMPT = """
You are Ask Mimir, an evidence-led US defense-market research assistant. The final user message is
followed by a deterministic NSN/NIIN evidence pack generated by Mimir.

Answer for the exact resolved item. Start with a compact identity summary showing NSN, NIIN,
description and FSC. Then cover current active-authorized source sites, other observed DLA
procurement recipients, part-number relationships, item-platform associations and material
contract activity. Use the returned CAGE location for each supplier site when available.

Keep three concepts distinct:
- a source authorized by DLA for procurement;
- an observed DLA procurement recipient for this NIIN during the stated fiscal-year window; and
- any other part-number or CAGE reference relationship.
Use the customer labels "Authorized source (DLA)", "Observed DLA recipient" and "Other reference".
Do not add a paragraph defining snapshot or active-CAGE mechanics unless the user asks about the
methodology. Do not call every reference manufacturer a qualified or current supply source. Explain
technical codes only when they materially affect the interpretation.

DLA item procurement value is the primary item-specific financial measure. Federal obligations on
linked awards provide broader award context for the item. Present them under "Linked award context"
and say that DLA item procurement remains the item-specific measure. Do not discuss adding or not
adding the figures unless the user asks about methodology. Never copy, allocate or sum value across
part-number reference rows. A part number may be shown as observed
on a transaction line, but a reference relationship alone is not a sale. Keep FY2026 partial
activity separate from the completed FY2021-FY2025 view. Call missing fiscal-year activity "not
observed," not zero.

List every returned platform association when the list is reasonably short. If the item maps to
multiple platforms, label it a shared-use item. Keep the explanation customer-facing and concise;
do not add a defensive qualification after the platform list. Description-only platform candidates
must never be presented as confirmed mappings or included in financial totals.

Use a compact supplier-site table with columns for supplier, CAGE/location, relationship status,
observed procurement and example part number. Then summarize the largest contracts and annual
activity. Hyperlink supplier CAGEs to
https://www.mimiradvisors.org/dashboard?view=COMPANY&cage=<CAGE>, platforms to
https://www.mimiradvisors.org/dashboard?view=PLATFORM&platform=<PLATFORM>, awards to
https://www.mimiradvisors.org/dashboard?view=AWARDS&award=<AWARD_ID>, and the NSN to
https://www.mimiradvisors.org/dashboard?view=PARTS&nsn=<NSN>. Do not expose internal award keys,
transaction keys, hashes, local paths or ingestion identifiers.

Finish with a concise "Evidence used" section naming DLA FLIS, DLA contract history, the fiscal
years and the number of supplier, part and contract records in the pack. The downloadable evidence
pack carries the full drill-down. Keep the answer below 1,200 words.
""".strip()

AWARD_OPPORTUNITY_PROMPT = """
You are Ask Mimir, an evidence-led US defense-market research assistant. The final user message is
followed by a deterministic contract or opportunity dossier generated by Mimir.

Resolve the public record first and answer for that exact scope. For a contract, lead with the
recipient, base-award purpose, awarding organization, competition terms, place of performance and
net prime obligations by fiscal year. Use the base-award description for the contract's purpose
and the action descriptions only when explaining modifications. Call the financial measure net
prime obligations over the loaded period, never lifetime value, revenue, contract ceiling or total
program value. FY2026 is partial.

Keep prime obligations and Mimir-modelled reported subcontract value in separate sections and do
not add them. When reported subawards exist, identify the material supplier sites, descriptions and
reported customer route. Do not expose internal award keys, transaction keys, report keys or
deduplication fields. A comparable supplier is useful market context, not evidence that it competed
for the specific completed award.

For an opportunity, summarize the notice, response deadline, requirement, buyer, PSC, NAICS,
set-aside status and contact or public notice link when supplied. Then assess historically relevant
suppliers. Call them likely competitors only when the evidence is strong, and always state that
they are not confirmed bidders. Distinguish possible competitors from possible teaming partners.
Explain every ranking using the returned evidence: same PSC, same NAICS, same service or buying
organization, relevant historical awards, and the nature of the work described. Do not rank on
NAICS alone. Do not infer proposal intent, qualification or bid participation.

Lead the competitive section with DIRECT_PROGRAM_HISTORY records: these identify recipients of
historical awards that explicitly name the program and are the strongest incumbent evidence in the
pack. Keep HIGH_RELEVANCE and MODERATE_RELEVANCE records in a separate wider-market comparison.
For a complex system, source-controlled component or requirement with substantial qualification,
integration, test, security or facility demands, PSC, NAICS and buyer overlap alone never establish
an executable alternative. If no other firm has direct requirement-level evidence, say that no
alternative qualified prime can be established from the available evidence. Put other relevant
firms under broader industrial-base context or possible teaming routes, not likely competitors.
If the response_status is CLOSED, say that immediately and frame the record as a
historical requirement and market signal rather than an open opportunity.

Description-derived platform matches are research leads only unless the dossier explicitly marks
them attributable. They may guide the narrative but must not alter financial attribution. Treat a
sources-sought notice as market research, not a solicitation or funded award.

Hyperlink CAGE sites to https://www.mimiradvisors.org/dashboard?view=COMPANY&cage=<CAGE>, contracts
to https://www.mimiradvisors.org/dashboard?view=AWARDS&award=<CONTRACT_ID>, platforms to
https://www.mimiradvisors.org/dashboard?view=PLATFORM&platform=<PLATFORM>, and use the supplied
SAM.gov URL for an opportunity. Use a compact table for competitors or major suppliers, followed by
short analytical sections. Finish with an Evidence used section and mention the downloadable CSV
evidence pack. Keep the standard answer below 1,200 words.
""".strip()

UNIVERSAL_PLATFORM_PROMPT = f"""
You are Ask Mimir, an evidence-led US defense-market research assistant. The final user message is
followed by a deterministic platform or program dossier generated by Mimir.

Answer for the resolved platform or program. Use a compact summary table first, then cover observed
procurement trajectory, direct award recipients, reported supplier sites, components or capabilities,
major awards, current opportunities and the evidence supporting the most material claims. Keep these
evidence lanes distinct: net prime obligations on directly mapped awards; Mimir-modelled reported
subcontract value; attributed DLA procurement value for single-platform NIINs; and shared-use NIIN
exposure, which is associated with this platform but not allocated to it.

Lead with the time-bounded totals supplied in financial_totals. Every financial value in prose or a
table must carry its fiscal-year period. For direct-award recipient tables,
normally omit rows below 0.5% of the platform's observed prime obligations unless a small row provides
uniquely material technical or program evidence. Do not let incidental travel, meeting or administrative
awards distract from the principal procurement picture. Use the observation window supplied in the
platform scope for reported subcontract values, prime obligations and DLA procurement figures.

Do not call every direct government recipient a prime contractor for the whole platform. Distinguish
the platform prime or system integrator when the evidence establishes it, other direct award
recipients, reported subcontractor sites and DLA item suppliers. A reported subcontract description
supports bounded capability language. An exact component-to-platform claim requires either the
curated_platform_supply_chain layer or an authoritative platform-specific public source. Where no
curated layer exists, say what the reported description shows without upgrading it into a precise
component claim.

Treat a named missile family as the program-wide scope represented by included_platform_records. Do not
silently narrow Tomahawk to one Tactical Tomahawk award or one variant. A follow-up asking for the full
supplier list, largest mapped subcontract positions, supplier-base concentration or important facilities
continues to use the same platform scope unless the user names a different award or platform. For supplier
answers, include CAGE and city/state whenever supplied. Use reported_descriptions to state what each site
appears to provide in concise functional language.

For concentration questions, analyze the reported supplier-site distribution in
reported_supplier_concentration and the component/source evidence separately. Do not use concentration
among direct government award recipients as the answer to supply-base concentration. Express the top-one,
top-five or top-ten supplier share in plain English; do not lead with HHI or effective-recipient-count jargon.

If curated_platform_supply_chain is present, use it as the strongest component-proof layer and keep
broader family references separate. Otherwise, use authoritative government and first-party web
sources to add current program facts and component confirmation where useful. Do not infer unrelated
companies as potential suppliers merely because they share a PSC, NAICS or broad capability.

Hyperlink CAGE sites to https://www.mimiradvisors.org/dashboard?view=COMPANY&cage=<CAGE>, awards to
https://www.mimiradvisors.org/dashboard?view=AWARDS&award=<CONTRACT_ID>, the platform to
https://www.mimiradvisors.org/dashboard?view=PLATFORM&platform=<PLATFORM>, and NSNs to
https://www.mimiradvisors.org/dashboard?view=PARTS&nsn=<NSN>. Use supplied public opportunity URLs.
Never expose internal keys, hashes, file paths or ingestion identifiers.
Never print a calculation version, release name, compatibility statement, internal pack status or phrases
such as "the dossier supplied", "the loaded records" or "the platform pack". Do not tell the customer that
a curated layer was or was not supplied.
Describe the strongest supported evidence directly, without generic defensive disclaimers or lists of data
the pack does not contain.

State the completed fiscal-year window for financial comparisons and identify the partial year
separately. Finish with Evidence used and mention the downloadable evidence pack. Keep the standard
answer below 1,300 words.

When using web research, apply this source hierarchy:
{WEB_SOURCE_POLICY_PROMPT}
""".strip()

ARTICLE_ANALYSIS_PROMPT = f"""
You are Ask Mimir, an evidence-led US defense-market research assistant. The user has supplied a news
article URL or pasted article text and wants the implications for the defense industrial base.

You have one Mimir evidence-retrieval round. Request all independent tools you need in parallel. First verify
the article, then prioritize the exact award, named company and affected site. Only
retrieve a platform, item, competitor or budget context when the article makes it material to the question.
For company-directory searches, query the company or legal name alone and use company_site scope; do not mix
the company name with a product, capability or spelled-out location. Use the returned city/state to select the
relevant CAGE, then retrieve all relevant matching site contexts in parallel when the location has more than
one plausible CAGE. A company search with exactly one site match includes its article_implications context in
the search result, so do not request that context again. When the article concerns an acquired business or a
site whose registered entity may retain a predecessor name, search both the current company and the supported
predecessor or acquired-business name in the same round. Treat that name as a retrieval alias, not automatic
proof that every historical site relationship belongs to the current parent.

Separate what the article reports, what Mimir's observed contracting and supply-chain records show, and the
resulting implications. Treat a contract described by the source as awarded or in production as a firm award.
Call an amount a ceiling, maximum or potential value only when the source does. Do not repeatedly discount an
announced award because its obligation schedule, option structure or delivery profile is not yet public, and
do not replace the announced contract value with an observed-obligations figure.
For award date, instrument type, amount, quantity, system power, place of performance and completion date,
prefer the exact official announcement and the awardee's exact announcement. If sources conflict, report only
the details on which those exact sources agree or explain the conflict briefly. Never import more specific
terms, quantities or dates from a different contract or program. Every such web-sourced detail must carry an
inline Markdown link to the exact supporting page; omit an unsupported detail rather than citing a generic
contracts index or naming an unlinked source.

Name a work or production location only when the article, an official award notice, a first-party source or a
Mimir award place-of-performance field supports it. A registered CAGE location or nearby company site does not
prove that award work occurs there. If the evidence only identifies the closest observed site, say that briefly
without presenting it as the production location.

When Mimir resolves an affected site, prioritize its observed incoming prime-customer routes and reported
downstream subcontract relationships. Name the companies, descriptions, time period and value where available,
while keeping them separate from suppliers explicitly tied to the new award. Omit empty evidence lanes.
Do not add generic lists of possible supplier categories, analogous programs or similar applications. Do not
include budget lines or FYDP material unless the user explicitly asks for budget or future-program analysis.
Do not compare the new award with earlier programs, alternative suppliers or competing systems unless the
article's main implication cannot be explained without that comparison.
Keep the implications to the three to five consequences most directly supported by the award and site evidence.
Do not add generic technology constraints, mission limitations or acquisition caveats merely to balance the
answer. If observed site relationships are available, present the most recent useful examples compactly and
say only that they are existing site relationships rather than suppliers confirmed on the new award.

Do not narrate internal lookup failures, dataset lag, missing curated records, acquisition-system speculation,
or lists of fields that are not available. State a limitation only when it changes the conclusion. Time-bound
every Mimir financial figure and do not add prime, subcontract and DLA measures together.

Use authoritative government and first-party sources for factual confirmation, with reputable defense
trade reporting as secondary evidence. Use only www.mimiradvisors.org for Mimir links: CAGE sites at
https://www.mimiradvisors.org/dashboard?view=COMPANY&cage=<CAGE>, awards at
https://www.mimiradvisors.org/dashboard?view=AWARDS&award=<CONTRACT_ID>, platforms at
https://www.mimiradvisors.org/dashboard?view=PLATFORM&platform=<PLATFORM>, and NSNs at
https://www.mimiradvisors.org/dashboard?view=PARTS&nsn=<NSN>. Link public web claims to their sources.
Never expose internal keys, hashes, release names, calculation versions, file paths or ingestion identifiers.
End with concise implications and an Evidence used section. Keep the standard answer below 800 words.

When using web research, apply this source hierarchy:
{WEB_SOURCE_POLICY_PROMPT}
""".strip()

COMPETITIVE_POSITION_PROMPT = """
You are Ask Mimir, an evidence-led US defense-market research assistant. The final user message is
followed by a deterministic named-market comparison pack.

Answer the user's market-position question directly, but do not manufacture one overall winner
when the evidence lanes measure different things. Lead with a compact table showing the strongest
site-level positions in: reported platform supply-chain evidence; observed DLA item procurement;
and direct power-related prime awards. Keep these lanes separate and do not add their values.

The declared peer universe and named platforms define the scope. Call the result an observed-position
comparison, never market share. Distinguish the platforms present in the reported relationship data
from the smaller set whose subcontract descriptions match the power capability rule. Never imply that
Mimir lacks a platform merely because its descriptions did not match that rule. A source-reported
ELECTRICAL description supports an electrical-systems relationship, but does not prove a specific
power component. Strict item names such as power supply, generator, converter or battery provide more
specific product evidence.

DLA procurement recipients may include manufacturers, distributors and logistics providers. Do not
call the largest DLA recipient the leading manufacturer unless another supplied record proves that
role. Direct awards establish government procurement from that recipient; they do not automatically
establish a subcontract position beneath a vehicle prime. All comparisons remain CAGE-site specific.

For every highlighted site, state the CAGE, location, platforms observed, evidence type and one or
two supporting descriptions or public award identifiers. Hyperlink CAGEs to
https://www.mimiradvisors.org/dashboard?view=COMPANY&cage=<CAGE>, awards to
https://www.mimiradvisors.org/dashboard?view=AWARDS&award=<CONTRACT_ID>, and NSNs to
https://www.mimiradvisors.org/dashboard?view=PARTS&nsn=<NSN>.

Finish with "How the comparison was constructed" and name the four scoring components, completed
fiscal-year window, platform coverage by lane and evidence limitations that materially affect the
ranking. Keep the answer below 1,100 words.
""".strip()

COMPETITOR_DISCOVERY_PROMPT = """
You are Ask Mimir, an evidence-led US defense-market research assistant. The final user message is
followed by a deterministic observed-peer pack for Eaton Aerospace.

Answer who Eaton Aerospace's closest observed US-defense peers are and why. Call the result observed
competitor discovery, not a definitive competitor list or market share. Begin with the resolved Eaton
scope, then provide a compact site-level table containing peer, CAGE/location, exact shared NIINs,
shared active-authorized NIINs, shared observed-procurement NIINs, overlapping capability groups,
shared platforms and the evidence interpretation.

Treat shared active-authorized NIINs as the strongest same-item evidence in the pack. Shared observed
procurement strengthens the evidence that both firms have participated in procurement for the same
items. Broader platform, customer and capability overlap explains adjacency but does not prove that
two companies competed for a particular contract. Comparable award examples share target PSC and
customer context; never describe them as bids against Eaton. Distinguish manufacturers from
distributors, logistics providers, platform OEMs and design authorities when the supplied evidence
allows it. When it does not, say the role is unresolved rather than guessing.

Do not combine affiliated Eaton sites into the peer list. Keep the ranking CAGE-site specific and
identify when two leading sites belong to the same named company. Observed procurement value on
shared NIINs is supporting activity, not revenue or market share.

Hyperlink CAGE sites to https://www.mimiradvisors.org/dashboard?view=COMPANY&cage=<CAGE>, awards to
https://www.mimiradvisors.org/dashboard?view=AWARDS&award=<CONTRACT_ID>, and sample NSNs/NIINs to
https://www.mimiradvisors.org/dashboard?view=PARTS&nsn=<NSN_OR_NIIN>. Do not expose internal keys,
file paths, hashes or ingestion identifiers.

Finish with "How the peer set was constructed", including the observation window, target-site scope,
six score inputs, explicit role limitations and coverage exclusions. Keep the answer below 1,200 words.
""".strip()

TOOLS = [
    {
        "type": "function",
        "name": "get_program_momentum",
        "description": "Return a deterministic multi-signal program-momentum ranking with separate obligations, awards, suppliers, budgets, quantities, solicitations and production-event lanes.",
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {
                "market": {"type": "string", "enum": ["missiles"]},
                "limit": {"type": "integer", "minimum": 1, "maximum": 12}
            },
            "required": ["market", "limit"],
            "additionalProperties": False
        }
    },
    {
        "type": "function",
        "name": "explain_program_momentum",
        "description": "Explain one program's exact momentum components and compare it with the programs immediately above and below in the frozen ranking.",
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {"program_id": {"type": "string"}},
            "required": ["program_id"],
            "additionalProperties": False
        }
    },
    {
        "type": "function",
        "name": "get_platform_supply_chain",
        "description": "Return a deterministic, evidence-layered platform supply-chain pack with separate prime recipients, reported subcontractor sites, verified component roles and drill-down identifiers.",
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {
                "platform_id": {"type": "string"},
                "capability_filter": {"type": ["string", "null"]},
                "supplier_limit": {"type": "integer", "minimum": 1, "maximum": 12},
            },
            "required": ["platform_id", "capability_filter", "supplier_limit"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "search_platform_contexts",
        "description": "Resolve a mapped defense platform or program name without silently choosing an ambiguous match.",
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 20},
            },
            "required": ["query", "limit"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "get_platform_context",
        "description": "Return a universal platform or program dossier with separate award, subcontract, NIIN, supplier, opportunity and evidence lanes.",
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {"platform_id": {"type": "string"}},
            "required": ["platform_id"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "search_company_contexts",
        "description": "Resolve a company question to available parent-wide or CAGE-site contexts before analysing capabilities, supply chains or opportunities.",
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "scope_type": {
                    "type": ["string", "null"],
                    "enum": ["company_parent", "company_site", None],
                },
                "limit": {"type": "integer", "minimum": 1, "maximum": 20},
            },
            "required": ["query", "scope_type", "limit"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "get_company_context",
        "description": "Return a compact, versioned company parent or CAGE-site evidence pack for profile, supply-chain or opportunity-discovery analysis.",
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {
                "scope_type": {
                    "type": "string",
                    "enum": ["company_parent", "company_site"],
                },
                "scope_id": {"type": "string"},
                "focus": {
                    "type": "string",
                    "enum": [
                        "profile",
                        "full_dossier",
                        "supply_chain",
                        "opportunity_discovery",
                        "article_implications",
                    ],
                },
            },
            "required": ["scope_type", "scope_id", "focus"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "get_company_opportunity_candidates",
        "description": "Return a deterministic company or CAGE-site decision pack that separates existing-position expansion, pre-solicitation requirement shaping and adjacent whitespace, with incumbents, budget or event evidence and practical next actions.",
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {
                "scope_type": {
                    "type": "string",
                    "enum": ["company_parent", "company_site"],
                },
                "scope_id": {"type": "string"},
            },
            "required": ["scope_type", "scope_id"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "search_item_contexts",
        "description": "Resolve an exact NSN, NIIN or part number to one or more item records without silently choosing an ambiguous part-number match.",
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 20},
            },
            "required": ["query", "limit"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "get_item_context",
        "description": "Return a deterministic item dossier for one resolved NIIN, including identity, active authorized sources, supplier locations, part references, platform associations and DLA contracts.",
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {"niin": {"type": "string"}},
            "required": ["niin"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "search_award_opportunity_contexts",
        "description": "Resolve a contract, award, solicitation or opportunity identifier without silently choosing an ambiguous text match.",
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 20},
            },
            "required": ["query", "limit"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "get_award_opportunity_context",
        "description": "Return a deterministic dossier for one resolved contract or opportunity, including actions, supplier evidence, comparable awards and evidence-ranked potential competitors.",
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {
                "record_type": {"type": "string", "enum": ["contract", "opportunity"]},
                "record_id": {"type": "string"},
            },
            "required": ["record_type", "record_id"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "search_metric_scopes",
        "description": "Find available Mimir company-site, platform, agency, PSC or NIIN metric scopes by name or identifier.",
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "scope_type": {
                    "type": ["string", "null"],
                    "enum": ["company_site", "platform", "agency", "psc", "niin", None],
                },
                "limit": {"type": "integer", "minimum": 1, "maximum": 20},
            },
            "required": ["query", "scope_type", "limit"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "get_metric_observation",
        "description": "Return a versioned metric observation for one exact scope and fiscal year, including growth, counts, rank, coverage and concentration where supported.",
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {
                "scope_type": {
                    "type": "string",
                    "enum": ["market", "company_site", "platform", "agency", "psc", "niin"],
                },
                "scope_id": {"type": "string"},
                "measure_type": {
                    "type": "string",
                    "enum": ["prime_obligations", "dla_procurement_value"],
                },
                "fiscal_year": {"type": "integer", "minimum": 2021, "maximum": 2026},
            },
            "required": ["scope_type", "scope_id", "measure_type", "fiscal_year"],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "get_metric_evidence",
        "description": "Return source action records supporting one metric scope and fiscal year. Use after a metric observation when the user asks why, how, or for records behind an answer.",
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {
                "scope_type": {
                    "type": "string",
                    "enum": ["market", "company_site", "platform", "agency", "psc", "niin"],
                },
                "scope_id": {"type": "string"},
                "measure_type": {
                    "type": "string",
                    "enum": ["prime_obligations", "dla_procurement_value"],
                },
                "fiscal_year": {"type": "integer", "minimum": 2021, "maximum": 2026},
                "sign": {"type": "string", "enum": ["net", "positive", "negative"]},
                "limit": {"type": "integer", "minimum": 1, "maximum": 20},
            },
            "required": [
                "scope_type",
                "scope_id",
                "measure_type",
                "fiscal_year",
                "sign",
                "limit",
            ],
            "additionalProperties": False,
        },
    },
]


# Promotional GPT-5.6 pricing published by OpenAI on 2026-08-30. Keep the
# token ledger authoritative; this estimate is an operational convenience.
MODEL_PRICING_USD_PER_MTOK = {
    "gpt-5.6": {"input": 4.0, "cached_input": 0.4, "cache_write": 5.0, "output": 20.0},
    "gpt-5.6-sol": {"input": 4.0, "cached_input": 0.4, "cache_write": 5.0, "output": 20.0},
    "gpt-5.6-terra": {"input": 2.0, "cached_input": 0.2, "cache_write": 2.5, "output": 12.0},
    "gpt-5.6-luna": {"input": 0.2, "cached_input": 0.02, "cache_write": 0.25, "output": 1.2},
}


def _usage_dict(response: Any) -> Dict[str, Any] | None:
    return response.usage.model_dump() if response.usage else None


def aggregate_usage(call_usages: List[Dict[str, Any] | None]) -> Dict[str, Any]:
    totals = {
        "input_tokens": 0,
        "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
        "output_tokens": 0,
        "output_tokens_details": {"reasoning_tokens": 0},
        "total_tokens": 0,
    }
    for usage in call_usages:
        if not usage:
            continue
        totals["input_tokens"] += int(usage.get("input_tokens") or 0)
        totals["output_tokens"] += int(usage.get("output_tokens") or 0)
        totals["total_tokens"] += int(usage.get("total_tokens") or 0)
        input_details = usage.get("input_tokens_details") or {}
        output_details = usage.get("output_tokens_details") or {}
        totals["input_tokens_details"]["cached_tokens"] += int(
            input_details.get("cached_tokens") or 0
        )
        totals["input_tokens_details"]["cache_write_tokens"] += int(
            input_details.get("cache_write_tokens") or 0
        )
        totals["output_tokens_details"]["reasoning_tokens"] += int(
            output_details.get("reasoning_tokens") or 0
        )
    return totals


def estimate_usage_cost(model: str, usage: Dict[str, Any]) -> Dict[str, Any] | None:
    rates = MODEL_PRICING_USD_PER_MTOK.get(model)
    if not rates:
        return None
    details = usage.get("input_tokens_details") or {}
    cached = int(details.get("cached_tokens") or 0)
    cache_write = int(details.get("cache_write_tokens") or 0)
    input_tokens = int(usage.get("input_tokens") or 0)
    standard_input = max(input_tokens - cached - cache_write, 0)
    output_tokens = int(usage.get("output_tokens") or 0)
    components = {
        "standard_input_usd": standard_input * rates["input"] / 1_000_000,
        "cached_input_usd": cached * rates["cached_input"] / 1_000_000,
        "cache_write_usd": cache_write * rates["cache_write"] / 1_000_000,
        "output_usd": output_tokens * rates["output"] / 1_000_000,
    }
    return {
        "estimated_total_usd": round(sum(components.values()), 8),
        "components": {key: round(value, 8) for key, value in components.items()},
        "rates_usd_per_million_tokens": rates,
        "pricing_checked_at": "2026-08-30",
    }


def is_supported_platform_supply_chain_request(messages: List[ChatMessage]) -> bool:
    text = " ".join(message.content for message in messages).lower()
    has_platform = "ch-53k" in text or "ch53k" in text
    has_supply_chain_intent = any(
        term in text
        for term in (
            "who supplies",
            "supplier",
            "supply chain",
            "what do they provide",
            "what proves",
            "component",
        )
    )
    return has_platform and has_supply_chain_intent


def is_article_analysis_request(messages: List[ChatMessage]) -> bool:
    text = "\n".join(message.content for message in messages if message.role == "user")
    lowered = text.lower()
    return bool(re.search(r"https?://\S+", text)) or (
        any(term in lowered for term in ("analyse this", "analyze this", "news article", "article text"))
        and len(text) >= 80
    )


def explicit_platform_query(
    messages: List[ChatMessage], store: PlatformContextStore
) -> str | None:
    text = str(messages[-1].content or "").strip()
    intent = text.lower()
    if not any(
        term in intent
        for term in (
            "platform", "program", "programme", "who supplies", "supply chain",
            "supplier", "component", "procurement trajectory", "commercial overview",
        )
    ):
        return None
    mentions = store.mentions(text)
    if len(mentions) == 1:
        return mentions[0]
    guided = re.search(
        r"(?:platform|program|programme)\s*:\s*([^\n?]+)", text, re.IGNORECASE
    )
    if guided:
        return guided.group(1).strip()
    return None


def platform_follow_up_intent(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(
        phrase in lowered
        for phrase in (
            "full list of suppliers",
            "all suppliers",
            "each supplier",
            "major supplier",
            "largest share",
            "largest mapped subcontract",
            "supplier base",
            "how concentrated",
            "which facilities",
            "what do they provide",
            "supporting evidence",
            "underlying contracts",
            "underlying records",
            "export the companies",
            "export the suppliers",
        )
    )


def is_clearly_out_of_domain(messages: List[ChatMessage]) -> bool:
    text = " ".join(message.content for message in messages).lower()
    defense_terms = (
        "defense", "defence", "military", "pentagon", "dod", "contract", "award",
        "supplier", "cage", "nsn", "niin", "platform", "program", "missile",
        "aircraft", "ship", "submarine", "army", "navy", "air force",
    )
    if any(term in text for term in defense_terms):
        return False
    unrelated_phrases = (
        "weather forecast", "football score", "basketball score", "recipe for",
        "write me a poem", "dating advice", "medical diagnosis", "solve this equation",
        "write python code", "javascript tutorial", "holiday itinerary",
    )
    return any(phrase in text for phrase in unrelated_phrases)


def is_program_momentum_request(messages: List[ChatMessage]) -> bool:
    text = " ".join(message.content for message in messages).lower()
    has_market = any(term in text for term in ("missile", "munition", "interceptor"))
    has_momentum = any(
        term in text
        for term in ("accelerat", "momentum", "growing", "growth", "fastest", "rank")
    )
    return has_market and has_momentum


def is_ground_vehicle_power_position_request(messages: List[ChatMessage]) -> bool:
    text = " ".join(message.content for message in messages).lower()
    has_market = any(
        term in text
        for term in (
            "army ground vehicle",
            "ground vehicles",
            "abrams",
            "bradley",
            "stryker",
            "jltv",
            "ampv",
            "m109",
        )
    )
    has_capability = any(
        term in text
        for term in ("power system", "electrical power", "power management")
    )
    has_position = any(
        term in text
        for term in ("strongest", "competitive", "position", "major supplier", "leading")
    )
    return has_market and has_capability and has_position


def is_eaton_competitor_request(messages: List[ChatMessage]) -> bool:
    text = " ".join(message.content for message in messages).lower()
    return "eaton" in text and any(
        term in text for term in ("competitor", "competes", "peer", "compared with")
    )


def company_site_trajectory_cage(messages: List[ChatMessage]) -> str | None:
    text = str(messages[-1].content or "")
    match = re.search(r"\bCAGE\s*[:#-]?\s*([A-Z0-9]{5})\b", text, re.IGNORECASE)
    if not match:
        return None
    intent = text.lower()
    if not any(
        term in intent
        for term in (
            "changed",
            "change",
            "since",
            "exposure",
            "history",
        )
    ):
        return None
    return match.group(1).upper()


def company_site_dossier_cage(messages: List[ChatMessage]) -> str | None:
    text = str(messages[-1].content or "")
    match = re.search(r"\bCAGE\s*[:#-]?\s*([A-Z0-9]{5})\b", text, re.IGNORECASE)
    bare_followup = False
    if not match and len(messages) > 1:
        bare_cage = re.fullmatch(
            r"\s*([A-Z0-9]{5})\s*",
            str(messages[-1].content or ""),
            re.IGNORECASE,
        )
        has_company_prompt = any(
            re.search(
                r"(?:defense\s+supplier|defence\s+supplier|supplier|company)\s*:",
                str(message.content or ""),
                re.IGNORECASE,
            )
            for message in messages[:-1]
        )
        if bare_cage and has_company_prompt:
            match = bare_cage
            bare_followup = True
    if not match:
        return None
    if bare_followup:
        return match.group(1).upper()
    intent = text.lower()
    if not any(
        term in intent
        for term in (
            "everything",
            "tell me about",
            "company profile",
            "site profile",
            "full picture",
            "overview",
        )
    ):
        return None
    return match.group(1).upper()


def explicit_company_name_query(messages: List[ChatMessage]) -> str | None:
    text = str(messages[-1].content or "").strip()
    if re.match(
        r"^(?:who\s+supplies|what\s+does\s+each\s+.+?\s+supplier|"
        r"which\s+.+?\s+suppliers|how\s+concentrated\s+is\s+the\s+.+?\s+supplier\s+base)",
        text,
        re.IGNORECASE,
    ):
        return None
    if re.fullmatch(r"[A-Z0-9]{5}", text, re.IGNORECASE) and any(
        character.isdigit() for character in text
    ):
        return None
    patterns = (
        r"(?:defense\s+supplier|defence\s+supplier|supplier|company)\s*:\s*([^\n?]+)",
        r"what\s+does\s+(.+?)\s+supply(?:\s|\?|$)",
        r"which\s+(?:defense|defence)?\s*platforms\s+does\s+(.+?)\s+support(?:\s|\?|$)",
        r"what\s+does\s+(.+?)(?:'s|’s)\s+.+?\s+facility\s+manufacture(?:\s|\?|$)",
        r"which\s+programs\s+does\s+(.+?)(?:'s|’s)\s+.+?\s+(?:site|facility)\s+support(?:\s|\?|$)",
        r"who\s+are\s+(.+?)(?:['’]s|s)\s+largest\s+(?:defense|defence)\s+customers",
        r"who\s+are\s+(.+?)(?:'s|’s)\s+largest\s+(?:defense|defence)\s+customers",
        r"which\s+prime\s+contractors\s+buy\s+from\s+(.+?)(?:\?|$)",
        r"what\s+are\s+(.+?)(?:'s|’s)\s+largest\s+visible\s+(?:defense|defence)\s+positions",
        r"what\s+are\s+(.+?)(?:['’]s|s)\s+largest\s+visible\s+(?:defense|defence)\s+positions",
        r"how\s+has\s+(.+?)(?:'s|’s)\s+(?:defense|defence)\s+activity\s+changed",
        r"how\s+has\s+(.+?)(?:['’]s|s)\s+(?:defense|defence)\s+activity\s+changed",
        r"which\s+cage\s+codes\s+are\s+associated\s+with\s+(.+?)(?:\?|$)",
        r"which\s+facilities\s+belong\s+to\s+(.+?)(?:\s+and\s+what|\?|$)",
        r"what\s+evidence\s+do\s+you\s+have\s+that\s+(.+?)\s+supplies\s+",
        r"(?:give\s+me\s+)?(?:a\s+)?concise\s+(?:defense|defence)[-\s]+market\s+profile\s+of\s+(.+?)(?:\?|$)",
        r"(?:tell\s+me\s+about|what\s+about|how\s+about)\s+(.+?)(?:\?|$)",
    )
    match = next(
        (candidate for pattern in patterns if (candidate := re.search(pattern, text, re.IGNORECASE))),
        None,
    )
    if match:
        query = match.group(1).strip().rstrip(".?")
    else:
        lowered = text.lower().strip(" .?")
        short_scope_reply = (
            len(text.split()) <= 5
            or lowered in {"parent wide", "parent-wide", "company wide", "company-wide", "corporation wide", "corporation-wide"}
        )
        if not short_scope_reply:
            return None
        if len(text.split()) >= 2 and "wide" not in lowered:
            query = text.strip().rstrip(".?")
            if query:
                return query
        prior_query = None
        for prior_index in range(len(messages) - 2, -1, -1):
            if messages[prior_index].role != "user":
                continue
            prior_query = explicit_company_name_query(messages[: prior_index + 1])
            if prior_query:
                break
        if not prior_query:
            return None
        query = prior_query if "wide" in lowered else f"{prior_query} {text}"
    if query.lower() in {"it", "them", "they", "their", "this company", "this site"}:
        return None
    if re.match(r"^CAGE\b", query, re.IGNORECASE) or (
        re.fullmatch(r"[A-Z0-9]{5}", query, re.IGNORECASE)
        and any(character.isdigit() for character in query)
    ):
        return None
    return query or None


def company_wide_intent(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(
        phrase in lowered
        for phrase in (
            "parent-wide",
            "parent wide",
            "company-wide",
            "company wide",
            "corporation-wide",
            "corporation wide",
            "into the us defence market",
            "into the us defense market",
            "largest defence customers",
            "largest defense customers",
            "largest visible defence positions",
            "largest visible defense positions",
            "which cage codes",
            "which facilities belong",
            "concise defence-market profile",
            "concise defense-market profile",
        )
    )


def company_follow_up_intent(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(
        term in lowered
        for term in (
            "them",
            "their",
            "this company",
            "this site",
            "what do they",
            "how has",
            "largest customers",
            "prime contractors buy",
            "platforms",
            "programs",
            "capabilities",
            "awards",
            "contracts",
        )
    )


def explicit_item_query(messages: List[ChatMessage]) -> str | None:
    text = " ".join(message.content for message in messages)
    intent = text.lower()
    if not any(term in intent for term in ("nsn", "niin", "part number", "part no", "item")):
        return None
    identifier_patterns = (
        r"\bNSN\s*[:#-]?\s*([0-9][0-9\s-]{10,20}[0-9])\b",
        r"\bNIIN\s*[:#-]?\s*([0-9][0-9\s-]{6,14}[0-9])\b",
        r"\bPART\s+(?:NUMBER|NO\.?|#)\s*[:#-]?\s*([A-Z0-9][A-Z0-9./_-]{2,39})\b",
    )
    for pattern in identifier_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    compact_nsn = re.search(r"\b\d{13}\b", re.sub(r"(?<=\d)[ -](?=\d)", "", text))
    return compact_nsn.group(0) if compact_nsn else None


def explicit_award_or_opportunity_query(messages: List[ChatMessage]) -> str | None:
    text = " ".join(message.content for message in messages)
    intent = text.lower()
    if not any(term in intent for term in ("contract", "award", "solicitation", "opportunity", "notice")):
        return None
    patterns = (
        r"\b(?:CONTRACT|AWARD|SOLICITATION|OPPORTUNITY|NOTICE)(?:\s+(?:NUMBER|NO\.?|ID))?\s*[:#]?\s*([A-Z0-9][A-Z0-9._/-]{4,})",
        r"\b((?:PANRSA|FA|W91|W31|N00|HQ|SPE)[A-Z0-9._/-]{5,})\b",
    )
    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        candidates = [match.rstrip(".,;:)") for match in matches if re.search(r"\d", match)]
        if candidates:
            return candidates[-1]
    return None


class ChatMessage(BaseModel):
    role: str = Field(pattern="^(user|assistant)$")
    content: str = Field(min_length=1, max_length=12000)


class ActiveScope(BaseModel):
    scope_type: str = Field(pattern="^(company_parent|company_site|platform)$")
    scope_id: str = Field(min_length=1, max_length=200)
    scope_name: Optional[str] = Field(default=None, max_length=300)
    resolved_cages: List[str] = Field(default_factory=list, max_length=250)
    group_kind: Optional[str] = Field(default=None, max_length=80)


class AskRequest(BaseModel):
    messages: List[ChatMessage] = Field(min_length=1, max_length=20)
    active_scope: Optional[ActiveScope] = None


class FeedbackRequest(BaseModel):
    response_id: str = Field(min_length=1, max_length=200)
    request_id: Optional[str] = Field(default=None, max_length=200)
    rating: str = Field(pattern="^(accurate|incomplete|wrong_entity|unsupported)$")
    reason: Optional[str] = Field(default=None, max_length=2000)


class LabRuntime:
    def __init__(self) -> None:
        release_root = os.getenv("ASK_MIMIR_RELEASE_ROOT")
        if release_root:
            release_dir, transactions = resolve_active_release(Path(release_root))
        elif os.getenv("ASK_MIMIR_RELEASE_DIR"):
            release_dir = Path(os.environ["ASK_MIMIR_RELEASE_DIR"])
            transactions = Path(
                os.getenv("ASK_MIMIR_TRANSACTIONS", str(DEFAULT_TRANSACTIONS))
            )
        elif (DEFAULT_RELEASE_ROOT / "active_release.json").exists():
            release_dir, transactions = resolve_active_release(DEFAULT_RELEASE_ROOT)
        else:
            release_dir = DEFAULT_RELEASE_DIR
            transactions = Path(os.getenv("ASK_MIMIR_TRANSACTIONS", str(DEFAULT_TRANSACTIONS)))
        if not (release_dir / "manifest.json").exists():
            raise RuntimeError(f"Ask Mimir release was not found: {release_dir}")
        if not transactions.exists():
            raise RuntimeError(f"Ask Mimir evidence ledger was not found: {transactions}")
        self.store = MetricStore(release_dir, transactions)
        company_context_dir = Path(
            os.getenv(
                "ASK_MIMIR_COMPANY_CONTEXT_DIR",
                str(ROOT / "validation-output" / "company-context"),
            )
        )
        self.company_contexts = CompanyContextStore(company_context_dir)
        self.item_contexts = ItemContextStore(
            Path(
                os.getenv(
                    "ASK_MIMIR_DATA_ROOT",
                    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data",
                )
            )
        )
        self.award_opportunities = AwardOpportunityContextStore(
            Path(
                os.getenv(
                    "ASK_MIMIR_DATA_ROOT",
                    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data",
                )
            )
        )
        self.platform_contexts = PlatformContextStore(
            Path(
                os.getenv(
                    "ASK_MIMIR_DATA_ROOT",
                    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data",
                )
            )
        )
        opportunity_dir = Path(
            os.getenv(
                "ASK_MIMIR_COMPANY_OPPORTUNITY_DIR",
                str(ROOT / "validation-output" / "company-opportunities"),
            )
        )
        self.company_opportunities = CompanyOpportunityStore(opportunity_dir)
        platform_supply_chain_dir = Path(
            os.getenv(
                "ASK_MIMIR_PLATFORM_SUPPLY_CHAIN_DIR",
                str(ROOT / "validation-output" / "platform-supply-chains"),
            )
        )
        self.platform_supply_chains = PlatformSupplyChainStore(
            platform_supply_chain_dir
        )
        self.program_momentum = ProgramMomentumStore(
            Path(
                os.getenv(
                    "ASK_MIMIR_PROGRAM_MOMENTUM_PACK",
                    str(ROOT / "validation-output" / "program-momentum" / "missile-program-momentum.json"),
                )
            )
        )
        self.competitive_position = CompetitivePositionStore(
            Path(
                os.getenv(
                    "ASK_MIMIR_DATA_ROOT",
                    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data",
                )
            )
        )
        self.competitor_discovery = CompetitorDiscoveryStore(
            Path(
                os.getenv(
                    "ASK_MIMIR_DATA_ROOT",
                    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data",
                )
            )
        )
        release_sources = {
            self.store.primitives,
            self.store.components_file,
            self.store.transactions,
            *self.item_contexts.paths.values(),
            *self.award_opportunities.paths.values(),
            *self.platform_contexts.paths.values(),
            self.company_contexts.context_dir / "manifest.json",
            *self.company_contexts.directory_sources,
            self.company_opportunities.opportunity_dir / "manifest.json",
            self.platform_supply_chains.pack_dir / "manifest.json",
            self.program_momentum.pack_path,
            self.competitive_position.definitions_path,
            *self.competitive_position.paths.values(),
            self.competitor_discovery.definitions_path,
            *self.competitor_discovery.paths.values(),
        }
        self.release_guard = DataReleaseGuard(
            str(self.store.manifest["release_id"]), release_sources
        )
        self.evidence_cache = EvidencePackCache(
            Path(
                os.getenv(
                    "ASK_MIMIR_CACHE_DIR",
                    str(ROOT / ".runtime" / "evidence-cache"),
                )
            ),
            ttl_seconds=int(os.getenv("ASK_MIMIR_CACHE_TTL_SECONDS", "86400")),
        )
        self.beta_state = BetaStateStore(
            Path(
                os.getenv(
                    "ASK_MIMIR_BETA_STATE",
                    str(ROOT / ".runtime" / "beta-state.sqlite3"),
                )
            )
        )
        self.lock = threading.Lock()
        self.model = os.getenv("OPENAI_MODEL", "gpt-5.6")
        self.mock_mode = os.getenv("ASK_MIMIR_MOCK", "0") == "1"
        self.external_evidence_allowed = (
            os.getenv("ASK_MIMIR_ALLOW_EXTERNAL_EVIDENCE", "0") == "1"
        )
        self.reasoning_effort = os.getenv("OPENAI_REASONING_EFFORT", "medium")
        self.max_output_tokens = int(os.getenv("OPENAI_MAX_OUTPUT_TOKENS", "10000"))
        self.max_evidence_records = min(
            max(int(os.getenv("ASK_MIMIR_MAX_EVIDENCE_RECORDS", "20")), 5),
            100,
        )
        audit_path = os.getenv("ASK_MIMIR_AUDIT_LOG")
        self.audit_log = Path(audit_path).resolve() if audit_path else None

    def write_audit_record(self, record: Dict[str, Any]) -> None:
        if self.audit_log is None:
            return
        self.audit_log.parent.mkdir(parents=True, exist_ok=True)
        with self.lock:
            with self.audit_log.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(record, default=str) + "\n")

    def search_scopes(self, query: str, scope_type: str | None, limit: int) -> Dict[str, Any]:
        clean_query = str(query).strip()
        if not clean_query:
            return {"matches": []}
        where = ["(UPPER(scope_name) LIKE UPPER(?) OR UPPER(scope_id) LIKE UPPER(?))"]
        params: List[Any] = [f"%{clean_query}%", f"%{clean_query}%"]
        if scope_type:
            where.append("scope_type = ?")
            params.append(scope_type)
        params.append(min(max(int(limit), 1), 20))
        with self.lock:
            result = self.store.connection.execute(
                f"""
                SELECT
                    scope_type,
                    scope_id,
                    measure_type,
                    MAX(scope_name) AS scope_name,
                    MIN(fiscal_year) AS first_fiscal_year,
                    MAX(fiscal_year) AS last_fiscal_year,
                    SUM(CASE WHEN fiscal_year = 2025 THEN net_value_usd ELSE 0 END) AS fy2025_net_value_usd
                FROM read_parquet(?)
                WHERE {' AND '.join(where)}
                GROUP BY scope_type, scope_id, measure_type
                ORDER BY ABS(fy2025_net_value_usd) DESC, scope_name ASC
                LIMIT ?
                """,
                [str(self.store.primitives), *params],
            )
            columns = [description[0] for description in result.description]
            matches = [dict(zip(columns, row)) for row in result.fetchall()]
        for match in matches:
            match.pop("fy2025_net_value_usd", None)
        return {
            "release_id": self.store.manifest["release_id"],
            "query": clean_query,
            "matches": matches,
        }

    def call_tool(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        self.release_guard.assert_unchanged()
        cacheable = {
            "get_program_momentum",
            "explain_program_momentum",
            "get_platform_supply_chain",
            "get_platform_context",
            "get_company_context",
            "get_company_opportunity_candidates",
            "get_item_context",
            "get_award_opportunity_context",
            "get_metric_observation",
            "get_competitive_position",
            "get_competitor_discovery",
        }
        cache_key = None
        if name in cacheable:
            cache_key = self.evidence_cache.cache_key(
                self.release_guard.release_binding_id, name, arguments
            )
            cached = self.evidence_cache.get(cache_key)
            if cached is not None:
                return cached

        result: Dict[str, Any]
        if name == "get_program_momentum":
            result = self.program_momentum.get(**arguments)
        elif name == "explain_program_momentum":
            result = self.program_momentum.explain(**arguments)
        elif name == "get_platform_supply_chain":
            result = self.platform_supply_chains.get(**arguments)
        elif name == "search_platform_contexts":
            return self.platform_contexts.search(**arguments)
        elif name == "get_platform_context":
            result = self.platform_contexts.answer_projection(**arguments)
        elif name == "search_company_contexts":
            return self.company_contexts.search(**arguments)
        elif name == "get_company_context":
            result = self.company_contexts.get(**arguments)
        elif name == "get_company_opportunity_candidates":
            result = self.company_opportunities.get(**arguments)
        elif name == "search_item_contexts":
            return self.item_contexts.search(**arguments)
        elif name == "get_item_context":
            result = self.item_contexts.answer_projection(**arguments)
        elif name == "search_award_opportunity_contexts":
            return self.award_opportunities.search(**arguments)
        elif name == "get_award_opportunity_context":
            result = self.award_opportunities.answer_projection(**arguments)
        elif name == "search_metric_scopes":
            return self.search_scopes(**arguments)
        elif name == "get_metric_observation":
            with self.lock:
                result = self.store.observation(
                    arguments["scope_type"],
                    arguments["scope_id"],
                    arguments["measure_type"],
                    arguments["fiscal_year"],
                )
        elif name == "get_competitive_position":
            result = self.competitive_position.get(**arguments)
        elif name == "get_competitor_discovery":
            result = self.competitor_discovery.get(**arguments)
        elif name == "get_metric_evidence":
            with self.lock:
                return self.store.evidence(
                    arguments["scope_type"],
                    arguments["scope_id"],
                    arguments["fiscal_year"],
                    arguments["measure_type"],
                    sign=arguments["sign"],
                    limit=arguments["limit"],
                )
        else:
            raise ValueError(f"unsupported tool: {name}")
        if cache_key is not None:
            self.evidence_cache.set(cache_key, result)
        return result

    def mock_answer(self, request: AskRequest) -> Dict[str, Any]:
        question = request.messages[-1].content
        if is_eaton_competitor_request(request.messages):
            arguments = {"target_id": "eaton_aerospace", "limit": 10}
            pack = self.call_tool("get_competitor_discovery", arguments)
            peers = pack["observed_peers"][:5]
            return {
                "answer": (
                    "**Closest observed peers**\n\n"
                    + "\n".join(
                        f"- [{row['supplier_name']}](https://www.mimiradvisors.org/dashboard?view=COMPANY&cage={row['cage']}) "
                        f"(CAGE {row['cage']}): {row['shared_exact_niin_count']} shared NIINs; "
                        f"{row['shared_active_authorized_niin_count']} shared active-authorized NIINs."
                        for row in peers
                    )
                    + "\n\n**How the peer set was constructed**\n\n"
                    "Affiliates and government or standards reference records are excluded. "
                    "The result is an observed peer ranking, not market share."
                ),
                "response_id": "local-competitor-discovery-mock",
                "model": "local-evidence-mock",
                "release_id": self.store.manifest["release_id"],
                "answer_artifacts": {
                    "competitor_discovery": pack,
                    "evidence_pack": {
                        "format": "zip",
                        "download_url": "/api/evidence/competitor-discovery/eaton_aerospace.zip",
                    },
                },
                "tool_trace": [
                    {"tool": "get_competitor_discovery", "arguments": arguments, "result": pack}
                ],
            }
        if is_ground_vehicle_power_position_request(request.messages):
            arguments = {"market_id": "army_ground_vehicle_power", "limit": 8}
            pack = self.call_tool("get_competitive_position", arguments)
            reported = pack["reported_supply_chain_position"][:3]
            dla = pack["dla_item_procurement_position"][:3]
            return {
                "answer": (
                    "**Observed position, not market share**\n\n"
                    "The current evidence does not support one combined winner. Reported supplier "
                    "relationships exist across all six declared vehicle families, but only JLTV "
                    "contains subcontract descriptions matching the current power/electrical rule. "
                    "The DLA item lane contains power-item evidence across five families.\n\n"
                    "**Reported platform supply-chain evidence**\n\n"
                    + "\n".join(
                        f"- [{row['supplier_name']}](https://www.mimiradvisors.org/dashboard?view=COMPANY&cage={row['cage']}) "
                        f"(CAGE {row['cage']}): {', '.join(row['evidence_descriptions'][:2])}."
                        for row in reported
                    )
                    + "\n\n**Observed DLA item procurement**\n\n"
                    + "\n".join(
                        f"- [{row['supplier_name']}](https://www.mimiradvisors.org/dashboard?view=COMPANY&cage={row['cage']}) "
                        f"(CAGE {row['cage']}): {', '.join(row['item_descriptions'][:2])}."
                        for row in dla
                    )
                    + "\n\n**How the comparison was constructed**\n\n"
                    "Each lane ranks CAGE sites using platform breadth, completed-year persistence, "
                    "award breadth and log-scaled value inside the declared evidence universe."
                ),
                "response_id": "local-competitive-position-mock",
                "model": "local-evidence-mock",
                "release_id": self.store.manifest["release_id"],
                "answer_artifacts": {
                    "competitive_position": pack,
                    "evidence_pack": {
                        "format": "zip",
                        "download_url": "/api/evidence/competitive-position/army_ground_vehicle_power.zip",
                    },
                },
                "tool_trace": [
                    {"tool": "get_competitive_position", "arguments": arguments, "result": pack}
                ],
            }
        if "ch-53k" in question.lower() or "ch53k" in question.lower():
            arguments = {
                "platform_id": "CH-53K",
                "capability_filter": None,
                "supplier_limit": 10,
            }
            pack = self.call_tool("get_platform_supply_chain", arguments)
            verified = pack["component_verified_suppliers"]
            prime = pack["platform_prime_contractors"][0]
            examples = "; ".join(
                f"{row['display_name']}: {', '.join(row['component_roles'][:2])}"
                for row in verified
            )
            return {
                "answer": (
                    "**Platform prime**\n\n"
                    f"{prime['supplier_name']} at CAGE {prime['cage']}.\n\n"
                    "**Component-verified suppliers**\n\n"
                    f"{examples}.\n\n"
                    "**Reported first-tier evidence**\n\n"
                    "Each component claim above has a platform-specific first-party source. "
                    "Specific subcontract descriptions can support a bounded capability inference; "
                    "generic descriptions support only the reported first-tier relationship.\n\n"
                    "**Wider CH-53 family**\n\n"
                    "The evidence drawer includes NIIN, NSN and part-number references for the wider "
                    "CH-53 family. They are explicitly unconfirmed for CH-53K, and T64 engine records "
                    "are excluded from strict CH-53K totals.\n\n"
                    "**Coverage**\n\n"
                    f"The current pack contains {pack['coverage']['resolved_supplier_site_count']:,} "
                    "resolved reported supplier sites. Prime obligations and modelled reported "
                    "subcontract value remain separate, non-additive measures."
                ),
                "response_id": "local-platform-supply-chain-mock",
                "model": "local-evidence-mock",
                "release_id": self.store.manifest["release_id"],
                "answer_artifacts": platform_answer_artifacts(pack),
                "tool_trace": [
                    {
                        "tool": "get_platform_supply_chain",
                        "arguments": arguments,
                        "result": pack,
                    }
                ],
            }
        query = "F-35" if "f-35" in question.lower() else question[:80]
        search_args = {"query": query, "scope_type": None, "limit": 5}
        search_result = self.call_tool("search_metric_scopes", search_args)
        matches = search_result.get("matches") or []
        trace = [
            {"tool": "search_metric_scopes", "arguments": search_args, "result": search_result}
        ]
        if not matches:
            return {
                "answer": (
                    "**Evidence gap**\n\n"
                    "The current frozen metric release does not contain a matching scope. "
                    "No quantitative answer should be produced until the relevant scope or source is added."
                ),
                "response_id": "local-mock-no-match",
                "model": "local-evidence-mock",
                "release_id": self.store.manifest["release_id"],
                "tool_trace": trace,
            }

        selected = matches[0]
        observation_args = {
            "scope_type": selected["scope_type"],
            "scope_id": selected["scope_id"],
            "measure_type": selected["measure_type"],
            "fiscal_year": int(self.store.manifest["analysis_fy"]),
        }
        evidence_args = {**observation_args, "sign": "net", "limit": 5}
        observation = self.call_tool("get_metric_observation", observation_args)
        evidence = self.call_tool("get_metric_evidence", evidence_args)
        trace.extend(
            [
                {
                    "tool": "get_metric_observation",
                    "arguments": observation_args,
                    "result": observation,
                },
                {"tool": "get_metric_evidence", "arguments": evidence_args, "result": evidence},
            ]
        )
        metrics = observation["metrics"]
        measure_label = {
            "prime_obligations": "Net prime obligations",
            "dla_procurement_value": "Net DLA procurement value",
        }[observation["measure_type"]]
        growth = metrics.get("growth_yoy_pct")
        growth_text = "not calculable" if growth is None else f"{growth:,.1f}%"
        top_components = metrics.get("concentration", {}).get("top_components", [])[:3]
        drivers = ", ".join(
            f"{row['component_name']} ({row['share_pct']:,.1f}% of positive value)"
            for row in top_components
        ) or "not available in this metric scope"
        answer = (
            f"**Source evidence**\n\n"
            f"The FY{observation['analysis_fy']} release contains {metrics['distinct_actions']:,} "
            f"actions across {metrics['distinct_awards']:,} awards for {observation['scope_name']}.\n\n"
            f"**Mimir calculation**\n\n"
            f"{measure_label} was ${metrics['net_value_usd']:,.0f}, a {growth_text} change from "
            f"FY{metrics['comparison_fy']}. Positive value was ${metrics['positive_value_usd']:,.0f}; "
            f"de-obligations were ${metrics['deobligation_value_usd']:,.0f}.\n\n"
            f"**Mimir analysis**\n\n"
            f"The largest positive-value recipient sites in this scope were {drivers}. This local mock "
            f"tests the evidence contract and does not call an external language model.\n\n"
            f"**Evidence used**\n\n"
            f"- {observation['scope_type']} scope `{observation['scope_id']}`\n"
            f"- Measure `{observation['measure_type']}`\n"
            f"- FY{observation['analysis_fy']}\n"
            f"- Release `{observation['release_id']}`\n"
            f"- {evidence['total_records']:,} supporting records; {len(evidence['records'])} shown"
        )
        return {
            "answer": answer,
            "response_id": "local-evidence-mock",
            "model": "local-evidence-mock",
            "release_id": self.store.manifest["release_id"],
            "tool_trace": trace,
        }


runtime = LabRuntime()
app = FastAPI(title="Ask Mimir", docs_url="/api/docs", redoc_url=None)
app.mount("/assets", StaticFiles(directory=LAB_DIR / "assets"), name="assets")


def workflow_for_request(request: AskRequest) -> str:
    if is_article_analysis_request(request.messages):
        return "news_article_implications"
    if is_eaton_competitor_request(request.messages):
        return "competitor_discovery"
    if is_ground_vehicle_power_position_request(request.messages):
        return "defined_market_competitive_position"
    if explicit_award_or_opportunity_query(request.messages):
        return "contract_or_opportunity"
    if explicit_item_query(request.messages):
        return "item_intelligence"
    if explicit_platform_query(request.messages, runtime.platform_contexts):
        return "platform_intelligence"
    if (
        request.active_scope
        and request.active_scope.scope_type == "platform"
        and platform_follow_up_intent(request.messages[-1].content)
    ):
        return "platform_intelligence"
    if company_site_dossier_cage(request.messages):
        return "company_site_intelligence"
    if explicit_company_name_query(request.messages):
        return "company_site_intelligence"
    if (
        request.active_scope
        and request.active_scope.scope_type in {"company_parent", "company_site"}
        and company_follow_up_intent(request.messages[-1].content)
    ):
        return "company_site_intelligence"
    if company_site_trajectory_cage(request.messages):
        return "company_site_trajectory"
    if is_program_momentum_request(request.messages):
        return "program_momentum"
    if is_clearly_out_of_domain(request.messages):
        return "out_of_domain"
    return "general_defense_research"


def access_from_request(request: Request) -> AccessContext:
    requested_tier = normalize_tier(request.headers.get("x-ask-mimir-tier"))
    requested_subject = str(request.headers.get("x-ask-mimir-subject") or "").strip()
    proxy_secret = os.getenv("ASK_MIMIR_TRUSTED_PROXY_SECRET")
    trusted_proxy = bool(
        proxy_secret
        and request.headers.get("x-ask-mimir-proxy-secret") == proxy_secret
    )
    test_identities = os.getenv("ASK_MIMIR_ALLOW_TEST_IDENTITIES", "0") == "1"
    if (trusted_proxy or test_identities) and requested_subject:
        return AccessContext(
            subject_id=f"user:{requested_subject[:160]}",
            tier=requested_tier,
            authenticated=requested_tier != "public",
        )
    if requested_subject and requested_tier != "public":
        raise HTTPException(
            status_code=401,
            detail=(
                "Signed-in Ask Mimir access could not be verified. The website and "
                "Ask Mimir proxy configuration do not match."
            ),
        )

    client_host = request.client.host if request.client else "unknown"
    user_agent = request.headers.get("user-agent", "unknown")
    salt = os.getenv("ASK_MIMIR_ANONYMOUS_SALT", "ask-mimir-local-beta")
    fingerprint = hashlib.sha256(
        f"{salt}|{client_host}|{user_agent}".encode()
    ).hexdigest()[:24]
    return AccessContext(
        subject_id=f"anonymous:{fingerprint}", tier="public", authenticated=False
    )


def require_evidence_download(request: Request) -> AccessContext:
    access = access_from_request(request)
    if not access.policy.can_download_evidence:
        raise HTTPException(
            status_code=403,
            detail="CSV evidence-pack downloads are available on Professional and Enterprise.",
        )
    return access


def sanitize_answer_text(answer: str) -> str:
    """Last-line guard against customer-facing runtime and identity plumbing."""
    blocked_line_patterns = (
        r"^\s*Release\s*:",
        r"^\s*Calculation version\s*:",
        r"^\s*Evidence-index records\s*:",
        r"^\s*(?:The\s+)?platform supply-chain pack (?:was|is) unavailable",
        r"^\s*Comparable .* in the dossier",
        r"^\s*Current parent resolution is not assigned",
        r"^\s*No current parent is resolved",
    )
    lines = [
        line
        for line in str(answer or "").splitlines()
        if not any(re.search(pattern, line, re.IGNORECASE) for pattern in blocked_line_patterns)
    ]
    cleaned = "\n".join(lines)
    cleaned = re.sub(
        r"\s*This is contract-action value, not [^.]+\.",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\s*These measures are non-additive[^.]*\.",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\s*(?:This answer is|The answer is)?\s*compatible with (?:the )?[^.]*?dossier[^.]*\.",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\s*(?:under|from|in)\s+release\s+`?[-A-Za-z0-9_.]+`?\.?",
        ".",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\bthe dossier\b", "the evidence", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bthe loaded records\b", "the available records", cleaned, flags=re.IGNORECASE)
    return re.sub(r"\n{3,}", "\n\n", cleaned).strip()


def finalize_customer_result(
    result: Dict[str, Any], access: AccessContext, request_id: str
) -> Dict[str, Any]:
    result = {**result, "answer": sanitize_answer_text(str(result.get("answer") or ""))}
    initial_validation = validate_answer_citations(
        str(result.get("answer") or ""), result.get("tool_trace") or []
    )
    removed_links = initial_validation.get("unsupported_mimir_links", [])
    if removed_links:
        result = {
            **result,
            "answer": remove_unsupported_mimir_links(
                str(result.get("answer") or ""), initial_validation
            ),
        }
    citation_validation = validate_answer_citations(
        str(result.get("answer") or ""), result.get("tool_trace") or []
    )
    if removed_links:
        citation_validation["removed_unsupported_mimir_links"] = removed_links
        citation_validation["warnings"] = [
            *citation_validation.get("warnings", []),
            "Unverified Mimir drill-down links were rendered as text.",
        ]
    if (
        citation_validation["status"] == "fail"
        and os.getenv("ASK_MIMIR_STRICT_CITATIONS", "1") == "1"
    ):
        raise RuntimeError(
            "The answer failed the outbound citation and internal-identifier check."
        )

    safe_result = sanitize_customer_payload(result)
    artifacts = safe_result.get("answer_artifacts") or {}
    if not access.policy.can_download_evidence and artifacts.get("evidence_pack"):
        artifacts["evidence_pack"] = {
            "format": artifacts["evidence_pack"].get("format", "zip"),
            "locked": True,
            "required_tier": "professional",
            "upgrade_url": os.getenv(
                "ASK_MIMIR_UPGRADE_URL",
                "https://www.mimiradvisors.org/dashboard?upgrade=professional",
            ),
        }
    safe_result["answer_artifacts"] = artifacts
    safe_result["request_id"] = request_id
    safe_result["release_binding_id"] = runtime.release_guard.release_binding_id
    safe_result["citation_validation"] = citation_validation
    safe_result["access"] = access.public_dict(
        runtime.beta_state.used_today(access.subject_id)
    )
    return safe_result


NON_BILLABLE_RESPONSE_IDS = frozenset(
    {
        "company-site-disambiguation",
        "award-opportunity-disambiguation",
        "item-disambiguation",
        "platform-disambiguation",
        "out-of-domain",
    }
)


def result_counts_toward_quota(result: Dict[str, Any]) -> bool:
    return str(result.get("response_id") or "") not in NON_BILLABLE_RESPONSE_IDS


class AskJobManager:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.jobs: Dict[str, Dict[str, Any]] = {}

    def create(self, request: AskRequest, access: AccessContext) -> Dict[str, Any]:
        request_id = str(uuid.uuid4())
        workflow = workflow_for_request(request)
        used = runtime.beta_state.reserve(
            request_id,
            access,
            runtime.release_guard.release_binding_id,
            workflow,
        )
        job = {
            "request_id": request_id,
            "subject_id": access.subject_id,
            "status": "queued",
            "workflow": workflow,
            "stage": "Queued",
            "detail": "Preparing the evidence workspace",
            "percent": 2,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "result": None,
            "error": None,
            "access": access.public_dict(used),
        }
        with self.lock:
            self.jobs[request_id] = job
        thread = threading.Thread(
            target=self._run,
            args=(request_id, request, access),
            daemon=True,
        )
        thread.start()
        return self.public_job(job)

    def update(self, request_id: str, stage: str, detail: str, percent: int) -> None:
        with self.lock:
            job = self.jobs[request_id]
            job.update(
                {
                    "status": "running",
                    "stage": stage,
                    "detail": detail,
                    "percent": min(max(int(percent), 0), 99),
                }
            )

    def _run(self, request_id: str, request: AskRequest, access: AccessContext) -> None:
        runtime.beta_state.mark_running(request_id)
        try:
            runtime.release_guard.assert_unchanged()
            result = generate_answer(
                request,
                progress=lambda stage, detail, percent: self.update(
                    request_id, stage, detail, percent
                ),
            )
            self.update(
                request_id,
                "Validating the answer",
                "Checking citations, drill-down links and customer-safe evidence",
                92,
            )
            customer_result = finalize_customer_result(result, access, request_id)
            cost = (result.get("estimated_cost") or {}).get("estimated_total_usd")
            runtime.beta_state.complete(
                request_id,
                latency_ms=result.get("latency_ms"),
                estimated_cost_usd=cost,
                billable=result_counts_toward_quota(result),
            )
            customer_result["access"] = access.public_dict(
                runtime.beta_state.used_today(access.subject_id)
            )
            with self.lock:
                self.jobs[request_id].update(
                    {
                        "status": "completed",
                        "stage": "Answer ready",
                        "detail": "The evidence and citations have been checked",
                        "percent": 100,
                        "result": customer_result,
                        "completed_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
        except Exception as exc:
            runtime.beta_state.fail(request_id, refund=True)
            detail = exc.detail if isinstance(exc, HTTPException) else str(exc)
            with self.lock:
                self.jobs[request_id].update(
                    {
                        "status": "failed",
                        "stage": "Request could not be completed",
                        "detail": detail,
                        "percent": 100,
                        "error": detail,
                    }
                )

    def get(self, request_id: str, access: AccessContext) -> Dict[str, Any]:
        with self.lock:
            job = self.jobs.get(request_id)
            if not job or job["subject_id"] != access.subject_id:
                raise KeyError(request_id)
            return self.public_job(job)

    @staticmethod
    def public_job(job: Dict[str, Any]) -> Dict[str, Any]:
        return {key: value for key, value in job.items() if key != "subject_id"}


job_manager = AskJobManager()


@app.get("/")
def index() -> FileResponse:
    return FileResponse(LAB_DIR / "index.html")


@app.get("/app.js")
def app_js() -> FileResponse:
    return FileResponse(LAB_DIR / "app.js", media_type="application/javascript")


@app.get("/styles.css")
def styles() -> FileResponse:
    return FileResponse(LAB_DIR / "styles.css", media_type="text/css")


@app.get("/api/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "release_id": runtime.store.manifest["release_id"],
        "analysis_fy": runtime.store.manifest["analysis_fy"],
        "model": runtime.model,
        "reasoning_effort": runtime.reasoning_effort,
        "openai_configured": bool(os.getenv("OPENAI_API_KEY")),
        "external_evidence_allowed": runtime.external_evidence_allowed,
        "mock_mode": runtime.mock_mode,
        "release_binding_id": runtime.release_guard.release_binding_id,
        "test_identities_enabled": os.getenv("ASK_MIMIR_ALLOW_TEST_IDENTITIES", "0") == "1",
        "trusted_proxy_configured": bool(os.getenv("ASK_MIMIR_TRUSTED_PROXY_SECRET")),
    }


@app.get("/api/beta/policy")
def beta_policy(request: Request) -> Dict[str, Any]:
    access = access_from_request(request)
    return {
        "tiers": {
            tier: {
                "display_name": policy.display_name,
                "queries_per_utc_day": policy.queries_per_utc_day,
                "can_download_evidence": policy.can_download_evidence,
            }
            for tier, policy in TIER_POLICIES.items()
        },
        "current_access": access.public_dict(
            runtime.beta_state.used_today(access.subject_id)
        ),
        "test_identities_enabled": os.getenv("ASK_MIMIR_ALLOW_TEST_IDENTITIES", "0") == "1",
    }


@app.post("/api/ask/jobs", status_code=202)
def create_ask_job(payload: AskRequest, request: Request) -> Dict[str, Any]:
    access = access_from_request(request)
    try:
        return job_manager.create(payload, access)
    except DailyQuotaExceeded as exc:
        raise HTTPException(
            status_code=429,
            detail={
                "message": str(exc),
                "access": access.public_dict(
                    runtime.beta_state.used_today(access.subject_id)
                ),
            },
        ) from exc


@app.get("/api/ask/jobs/{request_id}")
def get_ask_job(request_id: str, request: Request) -> Dict[str, Any]:
    try:
        return job_manager.get(request_id, access_from_request(request))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Ask Mimir job was not found.") from exc


@app.post("/api/feedback", status_code=201)
def add_answer_feedback(payload: FeedbackRequest, request: Request) -> Dict[str, Any]:
    access = access_from_request(request)
    if not payload.request_id:
        raise HTTPException(status_code=400, detail="A completed Ask Mimir request is required.")
    try:
        job = job_manager.get(payload.request_id, access)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Ask Mimir response was not found.") from exc
    expected_response_id = (job.get("result") or {}).get("response_id")
    if job.get("status") != "completed" or expected_response_id != payload.response_id:
        raise HTTPException(status_code=409, detail="Feedback does not match the completed response.")
    feedback_id = str(uuid.uuid4())
    runtime.beta_state.add_feedback(
        feedback_id=feedback_id,
        response_id=payload.response_id,
        request_id=payload.request_id,
        access=access,
        rating=payload.rating,
        reason=payload.reason,
        release_binding_id=runtime.release_guard.release_binding_id,
    )
    return {"feedback_id": feedback_id, "status": "recorded"}


@app.get("/api/evidence/platform-supply-chain/{platform_id}.zip")
def platform_supply_chain_evidence_export(platform_id: str, request: Request) -> StreamingResponse:
    require_evidence_download(request)
    try:
        payload = build_customer_evidence_zip(
            platform_id,
            runtime.platform_supply_chains.pack_dir,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    filename = f"mimir-{platform_id.lower()}-supply-chain-evidence.zip"
    return StreamingResponse(
        BytesIO(payload),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/evidence/platform.zip")
def universal_platform_evidence_export(platform_id: str, request: Request) -> StreamingResponse:
    require_evidence_download(request)
    try:
        context = runtime.platform_contexts.get_export_context(platform_id, limit=5000)
        payload = build_platform_context_zip(context)
    except (KeyError, ValueError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return StreamingResponse(
        BytesIO(payload),
        media_type="application/zip",
        headers={
            "Content-Disposition": (
                f'attachment; filename="{platform_context_filename(context)}"'
            )
        },
    )


@app.get("/api/evidence/company/{scope_type}/{scope_id}.zip")
def company_evidence_export(scope_type: str, scope_id: str, request: Request) -> StreamingResponse:
    require_evidence_download(request)
    if scope_type not in {"company_site", "company_parent"}:
        raise HTTPException(status_code=400, detail="Unsupported company scope type")
    try:
        context = runtime.company_contexts.get_raw(scope_type, scope_id)
        payload = build_company_evidence_zip(
            scope_type,
            scope_id,
            runtime.company_contexts.context_dir,
            context=context,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return StreamingResponse(
        BytesIO(payload),
        media_type="application/zip",
        headers={
            "Content-Disposition": (
                f'attachment; filename="{evidence_pack_filename(scope_type, scope_id)}"'
            )
        },
    )


@app.get("/api/evidence/item/{niin}.zip")
def item_evidence_export(niin: str, request: Request) -> StreamingResponse:
    require_evidence_download(request)
    try:
        context = runtime.item_contexts.get(niin)
        payload = build_item_evidence_zip(context)
    except (KeyError, ValueError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return StreamingResponse(
        BytesIO(payload),
        media_type="application/zip",
        headers={
            "Content-Disposition": (
                f'attachment; filename="{item_evidence_filename(context)}"'
            )
        },
    )


@app.get("/api/evidence/competitive-position/{market_id}.zip")
def competitive_position_evidence_export(market_id: str, request: Request) -> StreamingResponse:
    require_evidence_download(request)
    try:
        pack = runtime.competitive_position.get(market_id, limit=30)
        payload = build_competitive_position_zip(pack)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return StreamingResponse(
        BytesIO(payload),
        media_type="application/zip",
        headers={
            "Content-Disposition": (
                f'attachment; filename="mimir-{market_id}-competitive-position.zip"'
            )
        },
    )


@app.get("/api/evidence/competitor-discovery/{target_id}.zip")
def competitor_discovery_evidence_export(target_id: str, request: Request) -> StreamingResponse:
    require_evidence_download(request)
    try:
        pack = runtime.competitor_discovery.get(target_id, limit=30)
        payload = build_competitor_discovery_zip(pack)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return StreamingResponse(
        BytesIO(payload),
        media_type="application/zip",
        headers={
            "Content-Disposition": (
                f'attachment; filename="mimir-{target_id}-observed-peers.zip"'
            )
        },
    )


@app.get("/api/evidence/{record_type}/{record_id}.zip")
def award_opportunity_evidence_export(record_type: str, record_id: str, request: Request) -> StreamingResponse:
    require_evidence_download(request)
    try:
        context = runtime.award_opportunities.get(record_type, record_id)
        payload = build_award_opportunity_evidence_zip(context)
    except (KeyError, ValueError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return StreamingResponse(
        BytesIO(payload),
        media_type="application/zip",
        headers={
            "Content-Disposition": (
                f'attachment; filename="{award_opportunity_evidence_filename(context)}"'
            )
        },
    )


def emit_progress(
    callback: Callable[[str, str, int], None] | None,
    stage: str,
    detail: str,
    percent: int,
) -> None:
    if callback:
        callback(stage, detail, percent)


def complete_response_text(response: Any) -> str:
    if getattr(response, "status", None) == "incomplete":
        details = getattr(response, "incomplete_details", None)
        reason = getattr(details, "reason", None) or "response limit"
        raise HTTPException(
            status_code=502,
            detail=(
                "The model reached its response limit before completing the answer "
                f"({reason}). Your allowance has been restored; please retry."
            ),
        )
    answer = str(getattr(response, "output_text", "") or "").strip()
    if not answer:
        fragments: List[str] = []
        for item in getattr(response, "output", []) or []:
            if getattr(item, "type", None) != "message":
                continue
            for content in getattr(item, "content", []) or []:
                if getattr(content, "type", None) != "output_text":
                    continue
                fragment = str(getattr(content, "text", "") or "").strip()
                if fragment:
                    fragments.append(fragment)
        answer = "\n\n".join(fragments).strip()
    if not answer:
        raise HTTPException(status_code=502, detail="The model returned no answer.")
    return answer


def finalize_response(
    client: OpenAI,
    response: Any,
    instructions: str,
    input_items: List[Any],
    call_usages: List[Dict[str, Any] | None],
    progress: Callable[[str, str, int], None] | None,
    max_output_tokens: int,
) -> tuple[Any, str]:
    """Return model text, forcing a no-tools synthesis when evidence gathering ends silently."""
    try:
        return response, complete_response_text(response)
    except HTTPException as exc:
        if exc.status_code != 502 or exc.detail != "The model returned no answer.":
            raise

    emit_progress(
        progress,
        "Writing the answer",
        "Synthesizing the evidence already gathered",
        88,
    )
    synthesis_input = list(input_items)
    output = list(getattr(response, "output", []) or [])
    if not any(getattr(item, "type", None) == "function_call" for item in output):
        synthesis_input.extend(output)
    synthesis_input.append(
        {
            "role": "user",
            "content": (
                "Return the final answer now using the evidence already gathered. "
                "Do not request or call another tool. Answer the user's original question directly."
            ),
        }
    )
    final_response = client.responses.create(
        model=runtime.model,
        instructions=instructions,
        input=synthesis_input,
        reasoning={"effort": runtime.reasoning_effort},
        max_output_tokens=max_output_tokens,
        store=False,
    )
    call_usages.append(_usage_dict(final_response))
    return final_response, complete_response_text(final_response)


def generate_answer(
    request: AskRequest,
    progress: Callable[[str, str, int], None] | None = None,
) -> Dict[str, Any]:
    emit_progress(
        progress,
        "Resolving the question",
        "Identifying the entity, record and analytical scope",
        12,
    )
    if runtime.mock_mode:
        return runtime.mock_answer(request)
    started = time.perf_counter()
    resolved_company_scope: Dict[str, Any] | None = None
    company_query = explicit_company_name_query(request.messages)
    if (
        not company_query
        and request.active_scope
        and request.active_scope.scope_type in {"company_parent", "company_site"}
        and company_follow_up_intent(request.messages[-1].content)
    ):
        resolved_company_scope = request.active_scope.model_dump()
        if (
            resolved_company_scope.get("scope_type") == "company_parent"
            and resolved_company_scope.get("resolved_cages")
        ):
            runtime.company_contexts.register_group(
                scope_id=str(resolved_company_scope["scope_id"]),
                scope_name=str(
                    resolved_company_scope.get("scope_name")
                    or resolved_company_scope["scope_id"]
                ),
                cages=list(resolved_company_scope["resolved_cages"]),
                group_kind=str(
                    resolved_company_scope.get("group_kind")
                    or "observed_company_group"
                ),
            )
    if company_query:
        search_arguments = {
            "query": company_query,
            "scope_type": None,
            "limit": 50,
        }
        resolution = runtime.call_tool("search_company_contexts", search_arguments)
        search_trace = {
            "tool": "search_company_contexts",
            "arguments": search_arguments,
            "result": resolution,
        }
        matches = list(resolution.get("matches", []))
        latest_question = str(request.messages[-1].content or "").upper()
        location_matches = [
            row
            for row in matches
            if row.get("scope_type") == "company_site"
            and any(
                str(value or "").strip().upper() in latest_question
                for value in (row.get("city"), row.get("state"))
                if len(str(value or "").strip()) >= 3
            )
        ]
        site_specific = bool(
            re.search(r"\b(?:SITE|FACILITY|LOCATION|CAGE)\b", latest_question)
        )
        parent_matches = [
            row for row in matches if row.get("scope_type") == "company_parent"
        ]
        location_parent_matches = [
            row
            for row in parent_matches
            if any(
                str(value or "").strip().upper() in latest_question
                for value in (row.get("city"), row.get("state"))
                if len(str(value or "").strip()) >= 3
            )
        ]
        if len(location_matches) == 1:
            resolved_company_scope = location_matches[0]
        elif len(location_matches) > 1 and len(location_parent_matches) == 1:
            resolved_company_scope = location_parent_matches[0]
        elif company_wide_intent(latest_question) and len(parent_matches) == 1:
            resolved_company_scope = parent_matches[0]
        elif len(matches) == 1:
            resolved_company_scope = matches[0]
        elif resolution.get("requires_disambiguation"):
            options = "\n".join(
                f"- {option}"
                for option in resolution.get("disambiguation_options", [])
            )
            return {
                "answer": (
                    f"Which {company_query} site did you mean?\n\n"
                    f"{options}\n\n"
                    + (
                        f"Showing six of {resolution.get('total_site_matches')} matching sites. "
                        if resolution.get("has_more_site_matches")
                        else ""
                    )
                    + "Choose an option, or type another CAGE code or location."
                ),
                "response_id": "company-site-disambiguation",
                "model": "deterministic-resolution",
                "release_id": runtime.store.manifest["release_id"],
                "tool_trace": [search_trace],
                "answer_artifacts": {"company_resolution": resolution},
                "latency_ms": round((time.perf_counter() - started) * 1000, 1),
                "response_calls": 0,
                "usage": None,
                "usage_by_response": [],
                "estimated_cost": None,
            }
    if not os.getenv("OPENAI_API_KEY"):
        raise HTTPException(
            status_code=503,
            detail="OPENAI_API_KEY is not configured for the isolated Ask Mimir lab.",
        )
    if not runtime.external_evidence_allowed:
        raise HTTPException(
            status_code=503,
            detail=(
                "Real model mode is locked. Set ASK_MIMIR_ALLOW_EXTERNAL_EVIDENCE=1 "
                "only after approving transmission of selected metric evidence."
            ),
        )
    input_items: List[Any] = [
        {"role": message.role, "content": message.content} for message in request.messages
    ]
    client = OpenAI()
    if is_clearly_out_of_domain(request.messages):
        return {
            "answer": (
                "Ask Mimir is focused on the defense industrial base, government acquisition and "
                "national-security supply chains. I cannot help with that request here, but I can "
                "help if you reframe it around a defense company, program, component, award or market."
            ),
            "response_id": "out-of-domain",
            "model": "deterministic-scope-guard",
            "release_id": runtime.store.manifest["release_id"],
            "tool_trace": [],
            "answer_artifacts": {},
            "latency_ms": round((time.perf_counter() - started) * 1000, 1),
            "response_calls": 0,
            "usage": None,
            "usage_by_response": [],
            "estimated_cost": None,
        }
    if is_article_analysis_request(request.messages):
        emit_progress(progress, "Reading the article", "Verifying the report and resolving the entities it names", 24)
        article_tools = [*TOOLS, {"type": "web_search", "search_context_size": "low"}]
        response = client.responses.create(
            model=runtime.model,
            instructions=ARTICLE_ANALYSIS_PROMPT,
            input=input_items,
            tools=article_tools,
            tool_choice="auto",
            parallel_tool_calls=True,
            reasoning={"effort": runtime.reasoning_effort},
            max_output_tokens=min(runtime.max_output_tokens, 6000),
            store=False,
        )
        call_usages: List[Dict[str, Any] | None] = [_usage_dict(response)]
        trace: List[Dict[str, Any]] = []
        evidence_records_remaining = runtime.max_evidence_records
        for _ in range(1):
            calls = [item for item in response.output if item.type == "function_call"]
            if not calls:
                break
            input_items.extend(response.output)
            outputs = []
            for call in calls:
                arguments = json.loads(call.arguments)
                emit_progress(
                    progress,
                    "Connecting Mimir evidence",
                    f"Checking {call.name.replace('_', ' ')}",
                    min(42 + len(trace) * 6, 76),
                )
                try:
                    if call.name == "get_metric_evidence":
                        if evidence_records_remaining <= 0:
                            raise ValueError("the per-answer supporting-record limit has been reached")
                        arguments["limit"] = min(
                            int(arguments.get("limit", 10)), evidence_records_remaining
                        )
                    tool_result = runtime.call_tool(call.name, arguments)
                    if call.name == "search_company_contexts":
                        matches = tool_result.get("matches", [])
                        if (
                            len(matches) == 1
                            and matches[0].get("scope_type") == "company_site"
                        ):
                            tool_result = {
                                **tool_result,
                                "resolved_context": runtime.call_tool(
                                    "get_company_context",
                                    {
                                        "scope_type": "company_site",
                                        "scope_id": matches[0]["scope_id"],
                                        "focus": "article_implications",
                                    },
                                ),
                            }
                    if call.name == "get_metric_evidence":
                        evidence_records_remaining -= len(tool_result.get("records", []))
                    trace.append({"tool": call.name, "arguments": arguments, "result": tool_result})
                    output = json.dumps(tool_result, default=str)
                except Exception as exc:
                    error = {"error": str(exc), "tool": call.name, "arguments": arguments}
                    trace.append(error)
                    output = json.dumps(error)
                outputs.append({"type": "function_call_output", "call_id": call.call_id, "output": output})
            input_items.extend(outputs)
            emit_progress(progress, "Assessing the implications", "Reconciling the article with Mimir's records", 80)
            response = client.responses.create(
                model=runtime.model,
                instructions=ARTICLE_ANALYSIS_PROMPT,
                input=input_items,
                tools=article_tools,
                tool_choice="auto",
                parallel_tool_calls=True,
                reasoning={"effort": runtime.reasoning_effort},
                max_output_tokens=min(runtime.max_output_tokens, 6000),
                store=False,
            )
            call_usages.append(_usage_dict(response))
        response, answer_text = finalize_response(
            client,
            response,
            ARTICLE_ANALYSIS_PROMPT,
            input_items,
            call_usages,
            progress,
            min(runtime.max_output_tokens, 6000),
        )
        usage = aggregate_usage(call_usages)
        result = {
            "answer": answer_text,
            "response_id": response.id,
            "model": runtime.model,
            "release_id": runtime.store.manifest["release_id"],
            "answer_artifacts": {"article_analysis": True},
            "tool_trace": trace,
            "latency_ms": round((time.perf_counter() - started) * 1000, 1),
            "response_calls": len(call_usages),
            "usage": usage,
            "usage_by_response": call_usages,
            "estimated_cost": estimate_usage_cost(runtime.model, usage),
        }
        runtime.write_audit_record(
            {
                "run_id": str(uuid.uuid4()),
                "recorded_at": datetime.now(timezone.utc).isoformat(),
                "request_messages": [message.model_dump() for message in request.messages],
                **result,
            }
        )
        return result
    if is_eaton_competitor_request(request.messages):
        arguments = {"target_id": "eaton_aerospace", "limit": 15}
        emit_progress(
            progress,
            "Resolving Eaton Aerospace",
            "Separating target CAGE sites from affiliates and unrelated names",
            24,
        )
        pack = runtime.call_tool("get_competitor_discovery", arguments)
        emit_progress(
            progress,
            "Constructing the peer set",
            "Comparing exact items, authorized sources, procurement, platforms and customers",
            52,
        )
        trace = [
            {"tool": "get_competitor_discovery", "arguments": arguments, "result": pack}
        ]
        input_items.append(
            {
                "role": "user",
                "content": "MIMIR OBSERVED-COMPETITOR DISCOVERY PACK\n" + json.dumps(pack, default=str),
            }
        )
        emit_progress(
            progress,
            "Testing competitor evidence",
            "Keeping exact-item evidence separate from broader market adjacency",
            70,
        )
        response = client.responses.create(
            model=runtime.model,
            instructions=COMPETITOR_DISCOVERY_PROMPT,
            input=input_items,
            reasoning={"effort": runtime.reasoning_effort},
            max_output_tokens=min(runtime.max_output_tokens, 9000),
            store=False,
        )
        answer_text = complete_response_text(response)
        usage = aggregate_usage([_usage_dict(response)])
        result = {
            "answer": answer_text,
            "response_id": response.id,
            "model": runtime.model,
            "release_id": runtime.store.manifest["release_id"],
            "answer_artifacts": {
                "competitor_discovery": pack,
                "evidence_pack": {
                    "format": "zip",
                    "download_url": "/api/evidence/competitor-discovery/eaton_aerospace.zip",
                },
            },
            "tool_trace": trace,
            "latency_ms": round((time.perf_counter() - started) * 1000, 1),
            "response_calls": 1,
            "usage": usage,
            "usage_by_response": [_usage_dict(response)],
            "estimated_cost": estimate_usage_cost(runtime.model, usage),
        }
        runtime.write_audit_record(
            {
                "run_id": str(uuid.uuid4()),
                "recorded_at": datetime.now(timezone.utc).isoformat(),
                "request_messages": [message.model_dump() for message in request.messages],
                **result,
            }
        )
        return result
    if is_ground_vehicle_power_position_request(request.messages):
        arguments = {"market_id": "army_ground_vehicle_power", "limit": 15}
        emit_progress(
            progress,
            "Defining the comparison market",
            "Fixing the capability scope and Army ground-vehicle peer universe",
            24,
        )
        pack = runtime.call_tool("get_competitive_position", arguments)
        emit_progress(
            progress,
            "Comparing supplier positions",
            "Separating reported supply-chain, DLA item and direct-award evidence",
            52,
        )
        trace = [
            {"tool": "get_competitive_position", "arguments": arguments, "result": pack}
        ]
        input_items.append(
            {
                "role": "user",
                "content": "MIMIR DEFINED-MARKET COMPETITIVE-POSITION PACK\n" + json.dumps(pack, default=str),
            }
        )
        emit_progress(
            progress,
            "Preparing the comparison",
            "Testing the ranking against evidence coverage and supplier role",
            70,
        )
        response = client.responses.create(
            model=runtime.model,
            instructions=COMPETITIVE_POSITION_PROMPT,
            input=input_items,
            reasoning={"effort": runtime.reasoning_effort},
            max_output_tokens=min(runtime.max_output_tokens, 9000),
            store=False,
        )
        answer_text = complete_response_text(response)
        usage = aggregate_usage([_usage_dict(response)])
        result = {
            "answer": answer_text,
            "response_id": response.id,
            "model": runtime.model,
            "release_id": runtime.store.manifest["release_id"],
            "answer_artifacts": {
                "competitive_position": pack,
                "evidence_pack": {
                    "format": "zip",
                    "download_url": "/api/evidence/competitive-position/army_ground_vehicle_power.zip",
                },
            },
            "tool_trace": trace,
            "latency_ms": round((time.perf_counter() - started) * 1000, 1),
            "response_calls": 1,
            "usage": usage,
            "usage_by_response": [_usage_dict(response)],
            "estimated_cost": estimate_usage_cost(runtime.model, usage),
        }
        runtime.write_audit_record(
            {
                "run_id": str(uuid.uuid4()),
                "recorded_at": datetime.now(timezone.utc).isoformat(),
                "request_messages": [message.model_dump() for message in request.messages],
                **result,
            }
        )
        return result
    award_query = explicit_award_or_opportunity_query(request.messages)
    if award_query:
        emit_progress(progress, "Resolving the public record", "Matching the award or opportunity identifier", 24)
        search_arguments = {"query": award_query, "limit": 12}
        resolution = runtime.call_tool("search_award_opportunity_contexts", search_arguments)
        search_trace = {
            "tool": "search_award_opportunity_contexts",
            "arguments": search_arguments,
            "result": resolution,
        }
        if resolution.get("requires_disambiguation"):
            options = "\n".join(f"- {row['option_label']}" for row in resolution.get("matches", []))
            return {
                "answer": "I found more than one relevant public record. Which one do you mean?\n\n" + options,
                "response_id": "award-opportunity-disambiguation",
                "model": "deterministic-resolution",
                "release_id": runtime.store.manifest["release_id"],
                "tool_trace": [search_trace],
                "answer_artifacts": {"record_resolution": resolution},
                "latency_ms": round((time.perf_counter() - started) * 1000, 1),
                "response_calls": 0,
                "usage": None,
                "usage_by_response": [],
                "estimated_cost": None,
            }
        resolved = resolution.get("resolved")
        if resolved:
            arguments = {
                "record_type": resolved["record_type"],
                "record_id": resolved["record_id"],
            }
            pack = runtime.call_tool("get_award_opportunity_context", arguments)
            emit_progress(progress, "Assembling contract evidence", "Loading actions, suppliers and comparable public records", 48)
            trace = [search_trace, {"tool": "get_award_opportunity_context", "arguments": arguments, "result": pack}]
            input_items.append(
                {
                    "role": "user",
                    "content": "MIMIR CONTRACT OR OPPORTUNITY DOSSIER\n" + json.dumps(pack, default=str),
                }
            )
            emit_progress(progress, "Preparing the answer", "Analysing the bounded evidence and writing the brief", 68)
            response = client.responses.create(
                model=runtime.model,
                instructions=AWARD_OPPORTUNITY_PROMPT,
                input=input_items,
                reasoning={"effort": runtime.reasoning_effort},
                max_output_tokens=min(runtime.max_output_tokens, 9000),
                store=False,
            )
            call_usages = [_usage_dict(response)]
            response, answer_text = finalize_response(
                client,
                response,
                AWARD_OPPORTUNITY_PROMPT,
                input_items,
                call_usages,
                progress,
                min(runtime.max_output_tokens, 9000),
            )
            usage = aggregate_usage(call_usages)
            record_type = resolved["record_type"]
            record_id = resolved["record_id"]
            result = {
                "answer": answer_text,
                "response_id": response.id,
                "model": runtime.model,
                "release_id": runtime.store.manifest["release_id"],
                "answer_artifacts": {
                    f"{record_type}_dossier": pack,
                    "evidence_pack": {
                        "format": "zip",
                        "download_url": f"/api/evidence/{record_type}/{record_id}.zip",
                    },
                },
                "tool_trace": trace,
                "latency_ms": round((time.perf_counter() - started) * 1000, 1),
                "response_calls": len(call_usages),
                "usage": usage,
                "usage_by_response": call_usages,
                "estimated_cost": estimate_usage_cost(runtime.model, usage),
            }
            runtime.write_audit_record(
                {
                    "run_id": str(uuid.uuid4()),
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                    "request_messages": [message.model_dump() for message in request.messages],
                    **result,
                }
            )
            return result

    item_query = explicit_item_query(request.messages)
    if item_query:
        emit_progress(progress, "Resolving the item", "Matching the NSN, NIIN or part number", 24)
        search_arguments = {"query": item_query, "limit": 20}
        resolution = runtime.call_tool("search_item_contexts", search_arguments)
        search_trace = {
            "tool": "search_item_contexts",
            "arguments": search_arguments,
            "result": resolution,
        }
        if resolution.get("requires_disambiguation"):
            options = "\n".join(
                f"- {row['option_label']}"
                for row in resolution.get("matches", [])
            )
            return {
                "answer": (
                    "That part number maps to more than one item. Which one do you mean?\n\n"
                    + options
                ),
                "response_id": "item-disambiguation",
                "model": "deterministic-resolution",
                "release_id": runtime.store.manifest["release_id"],
                "tool_trace": [search_trace],
                "answer_artifacts": {"item_resolution": resolution},
                "latency_ms": round((time.perf_counter() - started) * 1000, 1),
                "response_calls": 0,
                "usage": None,
                "usage_by_response": [],
                "estimated_cost": None,
            }
        resolved_niin = resolution.get("resolved_niin")
        if resolved_niin:
            arguments = {"niin": resolved_niin}
            pack = runtime.call_tool("get_item_context", arguments)
            emit_progress(progress, "Assembling item evidence", "Loading sources, procurement, platforms and contracts", 48)
            trace = [
                search_trace,
                {"tool": "get_item_context", "arguments": arguments, "result": pack},
            ]
            input_items.append(
                {
                    "role": "user",
                    "content": "MIMIR ITEM DOSSIER\n" + json.dumps(pack, default=str),
                }
            )
            emit_progress(progress, "Preparing the answer", "Comparing source status and observed procurement", 68)
            response = client.responses.create(
                model=runtime.model,
                instructions=ITEM_DOSSIER_PROMPT,
                input=input_items,
                reasoning={"effort": runtime.reasoning_effort},
                max_output_tokens=min(runtime.max_output_tokens, 9000),
                store=False,
            )
            call_usages = [_usage_dict(response)]
            response, answer_text = finalize_response(
                client,
                response,
                ITEM_DOSSIER_PROMPT,
                input_items,
                call_usages,
                progress,
                min(runtime.max_output_tokens, 10000),
            )
            usage = aggregate_usage(call_usages)
            result = {
                "answer": answer_text,
                "response_id": response.id,
                "model": runtime.model,
                "release_id": runtime.store.manifest["release_id"],
                "answer_artifacts": {
                    "item_dossier": pack,
                    "evidence_pack": {
                        "format": "zip",
                        "download_url": f"/api/evidence/item/{resolved_niin}.zip",
                    },
                },
                "tool_trace": trace,
                "latency_ms": round((time.perf_counter() - started) * 1000, 1),
                "response_calls": len(call_usages),
                "usage": usage,
                "usage_by_response": call_usages,
                "estimated_cost": estimate_usage_cost(runtime.model, usage),
            }
            runtime.write_audit_record(
                {
                    "run_id": str(uuid.uuid4()),
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                    "request_messages": [message.model_dump() for message in request.messages],
                    **result,
                }
            )
            return result

    platform_query = explicit_platform_query(request.messages, runtime.platform_contexts)
    if (
        not platform_query
        and request.active_scope
        and request.active_scope.scope_type == "platform"
        and platform_follow_up_intent(request.messages[-1].content)
    ):
        platform_query = request.active_scope.scope_id
    if platform_query:
        emit_progress(progress, "Resolving the platform", "Matching the platform or program universe", 24)
        search_arguments = {"query": platform_query, "limit": 15}
        resolution = runtime.call_tool("search_platform_contexts", search_arguments)
        search_trace = {
            "tool": "search_platform_contexts",
            "arguments": search_arguments,
            "result": resolution,
        }
        if resolution.get("requires_disambiguation"):
            options = "\n".join(
                f"- {row['option_label']}" for row in resolution.get("matches", [])
            )
            return {
                "answer": "I found more than one mapped platform or program. Which one do you mean?\n\n" + options,
                "response_id": "platform-disambiguation",
                "model": "deterministic-resolution",
                "release_id": runtime.store.manifest["release_id"],
                "tool_trace": [search_trace],
                "answer_artifacts": {"platform_resolution": resolution},
                "latency_ms": round((time.perf_counter() - started) * 1000, 1),
                "response_calls": 0,
                "usage": None,
                "usage_by_response": [],
                "estimated_cost": None,
            }
        resolved_platform = resolution.get("resolved_platform_id")
        if resolved_platform:
            latest_platform_question = str(request.messages[-1].content or "")
            supplier_limit = 180 if platform_follow_up_intent(latest_platform_question) else 50
            arguments = {
                "platform_id": resolved_platform,
                "supplier_limit": supplier_limit,
            }
            pack = runtime.call_tool("get_platform_context", arguments)
            emit_progress(progress, "Assembling platform evidence", "Separating awards, suppliers, components and opportunities", 46)
            trace = [
                search_trace,
                {"tool": "get_platform_context", "arguments": arguments, "result": pack},
            ]
            if resolved_platform.upper() == "CH-53K":
                curated_arguments = {
                    "platform_id": "CH-53K",
                    "capability_filter": None,
                    "supplier_limit": 12,
                }
                curated = runtime.call_tool("get_platform_supply_chain", curated_arguments)
                pack["curated_platform_supply_chain"] = curated
                trace.append(
                    {
                        "tool": "get_platform_supply_chain",
                        "arguments": curated_arguments,
                        "result": curated,
                    }
                )
            input_items.append(
                {
                    "role": "user",
                    "content": "MIMIR UNIVERSAL PLATFORM OR PROGRAM DOSSIER\n" + json.dumps(pack, default=str),
                }
            )
            emit_progress(progress, "Preparing the answer", "Checking current sources and synthesizing the platform brief", 66)
            response = client.responses.create(
                model=runtime.model,
                instructions=UNIVERSAL_PLATFORM_PROMPT,
                input=input_items,
                tools=[{"type": "web_search", "search_context_size": "low"}],
                reasoning={"effort": runtime.reasoning_effort},
                max_output_tokens=min(runtime.max_output_tokens, 10000),
                store=False,
            )
            call_usages = [_usage_dict(response)]
            response, answer_text = finalize_response(
                client,
                response,
                UNIVERSAL_PLATFORM_PROMPT,
                input_items,
                call_usages,
                progress,
                min(runtime.max_output_tokens, 10000),
            )
            usage = aggregate_usage(call_usages)
            result = {
                "answer": answer_text,
                "response_id": response.id,
                "model": runtime.model,
                "release_id": runtime.store.manifest["release_id"],
                "answer_artifacts": {
                    "platform_dossier": pack,
                    "evidence_pack": {
                        "format": "zip",
                        "download_url": f"/api/evidence/platform.zip?platform_id={resolved_platform}",
                    },
                },
                "tool_trace": trace,
                "latency_ms": round((time.perf_counter() - started) * 1000, 1),
                "response_calls": len(call_usages),
                "usage": usage,
                "usage_by_response": call_usages,
                "estimated_cost": estimate_usage_cost(runtime.model, usage),
            }
            runtime.write_audit_record(
                {
                    "run_id": str(uuid.uuid4()),
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                    "request_messages": [message.model_dump() for message in request.messages],
                    **result,
                }
            )
            return result

    if is_supported_platform_supply_chain_request(request.messages):
        arguments = {
            "platform_id": "CH-53K",
            "capability_filter": None,
            "supplier_limit": 12,
        }
        pack = runtime.call_tool("get_platform_supply_chain", arguments)
        emit_progress(progress, "Assembling supply-chain evidence", "Separating verified components from reported relationships", 48)
        trace = [
            {
                "tool": "get_platform_supply_chain",
                "arguments": arguments,
                "result": pack,
            }
        ]
        input_items.append(
            {
                "role": "user",
                "content": (
                    "MIMIR PLATFORM SUPPLY-CHAIN EVIDENCE PACK\n"
                    + json.dumps(pack, default=str)
                ),
            }
        )
        emit_progress(progress, "Preparing the answer", "Writing the supplier and component assessment", 68)
        response = client.responses.create(
            model=runtime.model,
            instructions=PLATFORM_SUPPLY_CHAIN_PROMPT,
            input=input_items,
            reasoning={"effort": runtime.reasoning_effort},
            max_output_tokens=min(runtime.max_output_tokens, 9000),
            store=False,
        )
        answer_text = complete_response_text(response)
        usage = aggregate_usage([_usage_dict(response)])
        result = {
            "answer": answer_text,
            "response_id": response.id,
            "model": runtime.model,
            "release_id": runtime.store.manifest["release_id"],
            "answer_artifacts": platform_answer_artifacts(pack),
            "tool_trace": trace,
            "latency_ms": round((time.perf_counter() - started) * 1000, 1),
            "response_calls": 1,
            "usage": usage,
            "usage_by_response": [_usage_dict(response)],
            "estimated_cost": estimate_usage_cost(runtime.model, usage),
        }
        runtime.write_audit_record(
            {
                "run_id": str(uuid.uuid4()),
                "recorded_at": datetime.now(timezone.utc).isoformat(),
                "request_messages": [
                    message.model_dump() for message in request.messages
                ],
                **result,
            }
        )
        return result

    dossier_cage = company_site_dossier_cage(request.messages)
    dossier_scope_type = (
        str(resolved_company_scope.get("scope_type"))
        if resolved_company_scope
        else "company_site"
    )
    dossier_scope_id = (
        str(resolved_company_scope.get("scope_id"))
        if resolved_company_scope
        else dossier_cage
    )
    if dossier_scope_id:
        arguments = {
            "scope_type": dossier_scope_type,
            "scope_id": dossier_scope_id,
            "focus": "full_dossier",
        }
        try:
            pack = runtime.call_tool("get_company_context", arguments)
        except KeyError:
            pack = None
        if pack:
            emit_progress(progress, "Assembling company evidence", "Loading activity, customers, suppliers, items and locations", 48)
            trace = [
                {
                    "tool": "get_company_context",
                    "arguments": arguments,
                    "result": pack,
                }
            ]
            input_items.append(
                {
                    "role": "user",
                    "content": (
                        "MIMIR FULL CAGE-SITE DOSSIER\n"
                        + json.dumps(pack, default=str)
                    ),
                }
            )
            emit_progress(progress, "Preparing the answer", "Building the site-level commercial picture", 68)
            response = client.responses.create(
                model=runtime.model,
                instructions=COMPANY_SITE_DOSSIER_PROMPT,
                input=input_items,
                reasoning={"effort": runtime.reasoning_effort},
                max_output_tokens=min(runtime.max_output_tokens, 9000),
                store=False,
            )
            call_usages = [_usage_dict(response)]
            response, answer_text = finalize_response(
                client,
                response,
                COMPANY_SITE_DOSSIER_PROMPT,
                input_items,
                call_usages,
                progress,
                min(runtime.max_output_tokens, 9000),
            )
            usage = aggregate_usage(call_usages)
            result = {
                "answer": answer_text,
                "response_id": response.id,
                "model": runtime.model,
                "release_id": runtime.store.manifest["release_id"],
                "answer_artifacts": {
                    "company_site_dossier": pack,
                    "evidence_pack": {
                        "format": "zip",
                        "download_url": (
                            f"/api/evidence/company/{dossier_scope_type}/{dossier_scope_id}.zip"
                        ),
                    },
                },
                "tool_trace": trace,
                "latency_ms": round((time.perf_counter() - started) * 1000, 1),
                "response_calls": len(call_usages),
                "usage": usage,
                "usage_by_response": call_usages,
                "estimated_cost": estimate_usage_cost(runtime.model, usage),
            }
            runtime.write_audit_record(
                {
                    "run_id": str(uuid.uuid4()),
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                    "request_messages": [
                        message.model_dump() for message in request.messages
                    ],
                    **result,
                }
            )
            return result

    trajectory_cage = company_site_trajectory_cage(request.messages)
    if trajectory_cage:
        arguments = {
            "scope_type": "company_site",
            "scope_id": trajectory_cage,
            "focus": "profile",
        }
        try:
            pack = runtime.call_tool("get_company_context", arguments)
        except KeyError:
            pack = None
        if pack and pack.get("missile_program_trajectory"):
            emit_progress(progress, "Assembling trajectory evidence", "Comparing completed fiscal years and customer routes", 48)
            trace = [
                {
                    "tool": "get_company_context",
                    "arguments": arguments,
                    "result": pack,
                }
            ]
            input_items.append(
                {
                    "role": "user",
                    "content": (
                        "MIMIR CAGE-SITE TRAJECTORY EVIDENCE PACK\n"
                        + json.dumps(pack, default=str)
                    ),
                }
            )
            emit_progress(progress, "Preparing the answer", "Explaining the observed changes and supporting records", 68)
            response = client.responses.create(
                model=runtime.model,
                instructions=COMPANY_SITE_TRAJECTORY_PROMPT,
                input=input_items,
                reasoning={"effort": runtime.reasoning_effort},
                max_output_tokens=min(runtime.max_output_tokens, 8000),
                store=False,
            )
            answer_text = complete_response_text(response)
            usage = aggregate_usage([_usage_dict(response)])
            result = {
                "answer": answer_text,
                "response_id": response.id,
                "model": runtime.model,
                "release_id": runtime.store.manifest["release_id"],
                "answer_artifacts": {
                    "company_site_context": pack,
                    "evidence_pack": {
                        "format": "zip",
                        "download_url": (
                            f"/api/evidence/company/company_site/{trajectory_cage}.zip"
                        ),
                    },
                },
                "tool_trace": trace,
                "latency_ms": round((time.perf_counter() - started) * 1000, 1),
                "response_calls": 1,
                "usage": usage,
                "usage_by_response": [_usage_dict(response)],
                "estimated_cost": estimate_usage_cost(runtime.model, usage),
            }
            runtime.write_audit_record(
                {
                    "run_id": str(uuid.uuid4()),
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                    "request_messages": [
                        message.model_dump() for message in request.messages
                    ],
                    **result,
                }
            )
            return result

    if is_program_momentum_request(request.messages):
        arguments = {"market": "missiles", "limit": 10}
        pack = runtime.call_tool("get_program_momentum", arguments)
        emit_progress(progress, "Assembling momentum signals", "Separating obligations, budgets, suppliers and production events", 46)
        trace = [{"tool": "get_program_momentum", "arguments": arguments, "result": pack}]
        input_items.append(
            {
                "role": "user",
                "content": "MIMIR MISSILE PROGRAM MOMENTUM PACK\n" + json.dumps(pack, default=str),
            }
        )
        emit_progress(progress, "Checking the forward view", "Reviewing current authoritative sources before ranking", 64)
        response = client.responses.create(
            model=runtime.model,
            instructions=PROGRAM_MOMENTUM_PROMPT,
            input=input_items,
            tools=[
                {
                    "type": "web_search",
                    "search_context_size": "low",
                }
            ],
            reasoning={"effort": runtime.reasoning_effort},
            max_output_tokens=min(runtime.max_output_tokens, 10000),
            store=False,
        )
        answer_text = complete_response_text(response)
        usage = aggregate_usage([_usage_dict(response)])
        result = {
            "answer": answer_text,
            "response_id": response.id,
            "model": runtime.model,
            "release_id": runtime.store.manifest["release_id"],
            "answer_artifacts": {
                "program_momentum_ranking": runtime.program_momentum.full_programs(
                    limit=arguments["limit"]
                )
            },
            "tool_trace": trace,
            "latency_ms": round((time.perf_counter() - started) * 1000, 1),
            "response_calls": 1,
            "usage": usage,
            "usage_by_response": [_usage_dict(response)],
            "estimated_cost": estimate_usage_cost(runtime.model, usage),
        }
        runtime.write_audit_record(
            {
                "run_id": str(uuid.uuid4()),
                "recorded_at": datetime.now(timezone.utc).isoformat(),
                "request_messages": [message.model_dump() for message in request.messages],
                **result,
            }
        )
        return result

    emit_progress(progress, "Selecting evidence", "Choosing the relevant Mimir calculations and records", 34)
    response = client.responses.create(
        model=runtime.model,
        instructions=SYSTEM_PROMPT,
        input=input_items,
        tools=TOOLS,
        tool_choice="auto",
        parallel_tool_calls=False,
        reasoning={"effort": runtime.reasoning_effort},
        max_output_tokens=runtime.max_output_tokens,
        store=False,
    )
    call_usages: List[Dict[str, Any] | None] = [_usage_dict(response)]
    trace: List[Dict[str, Any]] = []
    evidence_records_remaining = runtime.max_evidence_records
    for _ in range(8):
        calls = [item for item in response.output if item.type == "function_call"]
        if not calls:
            break
        input_items.extend(response.output)
        outputs = []
        for call in calls:
            arguments = json.loads(call.arguments)
            emit_progress(
                progress,
                "Building the evidence trail",
                f"Running {call.name.replace('_', ' ')}",
                min(48 + len(trace) * 6, 76),
            )
            try:
                if call.name == "get_metric_evidence":
                    if evidence_records_remaining <= 0:
                        raise ValueError("the per-answer supporting-record limit has been reached")
                    arguments["limit"] = min(
                        int(arguments.get("limit", 10)), evidence_records_remaining
                    )
                result = runtime.call_tool(call.name, arguments)
                if call.name == "get_metric_evidence":
                    evidence_records_remaining -= len(result.get("records", []))
                trace.append({"tool": call.name, "arguments": arguments, "result": result})
                output = json.dumps(result, default=str)
            except Exception as exc:
                error = {"error": str(exc), "tool": call.name, "arguments": arguments}
                trace.append(error)
                output = json.dumps(error)
            outputs.append(
                {
                    "type": "function_call_output",
                    "call_id": call.call_id,
                    "output": output,
                }
            )
        input_items.extend(outputs)
        emit_progress(progress, "Testing the answer", "Reconciling the retrieved evidence before synthesis", 78)
        response = client.responses.create(
            model=runtime.model,
            instructions=SYSTEM_PROMPT,
            input=input_items,
            tools=TOOLS,
            tool_choice="auto",
            parallel_tool_calls=False,
            reasoning={"effort": runtime.reasoning_effort},
            max_output_tokens=runtime.max_output_tokens,
            store=False,
        )
        call_usages.append(_usage_dict(response))
    response, answer_text = finalize_response(
        client,
        response,
        SYSTEM_PROMPT,
        input_items,
        call_usages,
        progress,
        runtime.max_output_tokens,
    )
    total_usage = aggregate_usage(call_usages)
    result = {
        "answer": answer_text,
        "response_id": response.id,
        "model": runtime.model,
        "release_id": runtime.store.manifest["release_id"],
        "tool_trace": trace,
        "latency_ms": round((time.perf_counter() - started) * 1000, 1),
        "response_calls": len(call_usages),
        "usage": total_usage,
        "usage_by_response": call_usages,
        "estimated_cost": estimate_usage_cost(runtime.model, total_usage),
    }
    runtime.write_audit_record(
        {
            "run_id": str(uuid.uuid4()),
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "request_messages": [message.model_dump() for message in request.messages],
            **result,
        }
    )
    return result


@app.post("/api/ask")
def ask_direct(payload: AskRequest, request: Request) -> Dict[str, Any]:
    """Backward-compatible synchronous endpoint used by the evaluation runner."""
    access = access_from_request(request)
    request_id = str(uuid.uuid4())
    try:
        runtime.beta_state.reserve(
            request_id,
            access,
            runtime.release_guard.release_binding_id,
            workflow_for_request(payload),
        )
    except DailyQuotaExceeded as exc:
        raise HTTPException(
            status_code=429,
            detail={
                "message": str(exc),
                "access": access.public_dict(
                    runtime.beta_state.used_today(access.subject_id)
                ),
            },
        ) from exc
    runtime.beta_state.mark_running(request_id)
    try:
        result = generate_answer(payload)
        customer_result = finalize_customer_result(result, access, request_id)
        runtime.beta_state.complete(
            request_id,
            latency_ms=result.get("latency_ms"),
            estimated_cost_usd=(result.get("estimated_cost") or {}).get(
                "estimated_total_usd"
            ),
            billable=result_counts_toward_quota(result),
        )
        customer_result["access"] = access.public_dict(
            runtime.beta_state.used_today(access.subject_id)
        )
        return customer_result
    except Exception:
        runtime.beta_state.fail(request_id, refund=True)
        raise
