# Ask Mimir beta acceptance catalogue

This catalogue defines the behaviours Ask Mimir must support before a public beta. It is a capability test, not a list of phrases to hard-code. Each theme must pass with reasonable paraphrases, spelling variations and follow-up questions.

## Release gates

- [ ] No question silently resolves to the wrong company, site, platform, item, award or opportunity.
- [ ] Ambiguous requests return clickable choices with company/site name, location and CAGE where applicable.
- [ ] A follow-up can retain the current subject, change to a new subject or broaden/narrow scope without becoming trapped in the prior answer.
- [ ] Every financial value states the relevant fiscal-year or date window.
- [ ] Prime obligations, reported subcontract value, DLA procurement value, budget values and announced contract values remain distinct.
- [ ] Every material factual claim has a usable Mimir or public-record evidence route.
- [ ] No internal release IDs, file paths, SQL, source keys, dossier terminology or implementation notes appear in customer answers.
- [ ] Failed or refused questions do not consume quota.
- [ ] Guest, free, trial, Lite, Professional and Enterprise permissions match policy.
- [ ] Every submitted question, outcome, tier, latency and user feedback can be reviewed privately.
- [ ] The current data release loads atomically and every evidence pack uses the same release.
- [ ] Desktop and mobile flows support question entry, clarification, follow-up, evidence inspection and a return to the start screen.

## 1. Entity resolution and conversation

- [ ] Exact CAGE: `Tell me about CAGE 19645.`
- [ ] Exact legal name: `Tell me about Honeywell International Inc.`
- [ ] Trading name: `Tell me about Aerojet Rocketdyne.`
- [ ] Parent scope: `Tell me about Lockheed Martin's US defense business.`
- [ ] Site scope: `What does Honeywell Clearwater supply?`
- [ ] Location plus company: `Tell me about Boeing in Mesa, Arizona.`
- [ ] Co-located or related CAGEs: `Are Honeywell CAGEs 09128 and 0BFA5 the same facility?`
- [ ] Ambiguous company: `Tell me about Ontic.`
- [ ] Misspelling: `Tell me about Northrup Grumman.`
- [ ] Platform alias: `Who supplies Tactical Tomahawk?`
- [ ] Platform family: `Who supplies the Tomahawk family?`
- [ ] Variant: `How does LRASM differ from JASSM in the supplier data?`
- [ ] NIIN: `Tell me everything about NIIN 000000042.`
- [ ] NSN: `Who supplies NSN 1005-00-000-0042?`
- [ ] Part number: `Find defense records for part number UK 60A890216.`
- [ ] Award: `Tell me about contract N0001923F2616.`
- [ ] Opportunity: `Tell me about solicitation N00019-25-S-0001.`
- [ ] Public URL: submit a government or reputable defense-news article URL with no additional text.
- [ ] Pronoun follow-up: `Who are its largest prime customers?`
- [ ] Scope change: after Clearwater, ask `Now do Honeywell Phoenix.`
- [ ] Broadening: after one CAGE, ask `Now show the whole parent company.`
- [ ] Narrowing: after a parent answer, ask `Only show the Clearwater site.`
- [ ] New topic: after a company answer, ask `Who supplies the Virginia class?`

## 2. Company and site intelligence

- [ ] `What does Honeywell supply into the US defense market?`
- [ ] `Which defense platforms does Honeywell support?`
- [ ] `What does Honeywell's Clearwater facility manufacture or repair?`
- [ ] `Which programs does Aerojet Rocketdyne's East Camden site support?`
- [ ] `Who are this site's largest government customers?`
- [ ] `Which prime contractors buy from this site?`
- [ ] `Who supplies this site?`
- [ ] `What are Boeing St. Louis's largest observed defense positions?`
- [ ] `How has CAGE 19645's activity changed since FY2021?`
- [ ] `List every CAGE associated with Boeing, with location.`
- [ ] `Which facilities belong to Northrop Grumman and what does each do?`
- [ ] `What evidence shows that L3Harris supplies the C-130?`
- [ ] `Give me a concise defense-market profile of AeroVironment.`
- [ ] `Separate this company's prime-contract and subcontract activity.`
- [ ] `Show its leading PSCs and NAICS codes with descriptions.`
- [ ] `Which NIINs is this site authorized to supply?`
- [ ] `Which NIINs show observed procurement activity for this site?`
- [ ] `How dependent is this site on its largest prime customer?`
- [ ] `Which company sites share this capability?`
- [ ] `What changed after this company was acquired?`

Required answer behaviour:

- Site rows remain separate and include location and CAGE.
- Parent-wide totals use a governed parent-to-site hierarchy, not a loose company-name search.
- Site capabilities use multiple evidence lanes where available, rather than one convenient contract description.
- PSC and NAICS codes include descriptions.
- Prime customers and awards link to the relevant Mimir view.

## 3. Platform, program and system intelligence

- [ ] `Who supplies the CH-53K, what do they provide, and what proves it?`
- [ ] `Who supplies the Tomahawk missile?`
- [ ] `What does each major Tomahawk supplier provide?`
- [ ] `Give me the full Tomahawk supplier list.`
- [ ] `Show only propulsion suppliers on JASSM.`
- [ ] `Which CH-53K suppliers are in Connecticut?`
- [ ] `Which suppliers have the largest mapped Tomahawk subcontract positions?`
- [ ] `How concentrated is the Tomahawk supply chain below the prime?`
- [ ] `Which facilities appear most important to Tomahawk production?`
- [ ] `Compare the Tomahawk and JASSM supplier bases.`
- [ ] `Which suppliers appear on both Columbia and Virginia class submarines?`
- [ ] `Which components on this platform have multiple authorized sources?`
- [ ] `Which platform items are shared with other programs?`
- [ ] `Which suppliers entered or disappeared from this program since FY2021?`
- [ ] `Show the prime awards, reported subcontracts and item evidence separately.`
- [ ] `What recent solicitations relate to this program?`
- [ ] `Give me a commercial overview of the F-35, not a general program history.`

Required answer behaviour:

- The answer distinguishes platform prime recipients, reported first-tier suppliers and the wider item/supplier network.
- Supplier rows include company/site, location, CAGE, supplied capability and evidence.
- Family-wide evidence is identified separately from variant-specific evidence.
- Trivial or tangential awards do not crowd out the material supply-chain answer.
- Supplier-value totals state both the time window and the number of records/sites represented.

## 4. NIIN, NSN and part-number intelligence

- [ ] `Who are the authorized sources for this NIIN?`
- [ ] `Which suppliers have actually received DLA procurement for it?`
- [ ] `Show authorized and observed sources side by side.`
- [ ] `What part numbers are associated with this NIIN?`
- [ ] `Which NIIN does this part number belong to?`
- [ ] `How has its observed unit price changed since FY2021?`
- [ ] `How many suppliers were active in each fiscal year?`
- [ ] `Show its largest and most recent DLA awards.`
- [ ] `Which platforms use this item?`
- [ ] `Is this a shared-use item across multiple platforms?`
- [ ] `What do its RNCC, RNVC and RNSC codes mean?`
- [ ] `Are any listed CAGEs inactive or superseded?`
- [ ] `Which source relationships are reference-only rather than procurement-authorized?`
- [ ] `Show me the public records supporting this supplier relationship.`

Required answer behaviour:

- The NSN is formatted from FSC plus NIIN only where FSC is available.
- NIIN-level financials are never repeated and summed across part-number references.
- Platform associations remain many-to-many and do not arbitrarily allocate full item value to one platform.
- Authorized-source status and observed procurement activity are not conflated.

## 5. Contract and opportunity intelligence

- [ ] `Tell me everything important about contract N0001923F2616.`
- [ ] `What was the original award purpose?`
- [ ] `What did the latest contract action change?`
- [ ] `Show the action history and net obligations by fiscal year.`
- [ ] `Who received the award and where was the work performed?`
- [ ] `Which platform, PSC and NAICS are associated with it?`
- [ ] `What reported subcontracts sit beneath this award?`
- [ ] `How significant is this award relative to the recipient's recent activity?`
- [ ] `How significant is it relative to this program's recent activity?`
- [ ] `Tell me everything important about this solicitation.`
- [ ] `When is it due, who is the customer and what capability is sought?`
- [ ] `Which incumbent or comparable suppliers are relevant?`
- [ ] `Is this likely to require an established qualified source?`
- [ ] `Find similar prior awards.`
- [ ] `Did this earlier solicitation become an award?`
- [ ] `Show open opportunities related to this company's capabilities.`

Required answer behaviour:

- Base award description and action description are not silently substituted for one another.
- Action dates omit irrelevant timestamps in customer presentation.
- Candidate competitors are labelled by the evidence supporting relevance; shared NAICS alone is insufficient.
- Opportunity links open the exact public notice where available.

## 6. Market and program momentum

- [ ] `Which missile programs are accelerating?`
- [ ] `Which Army vehicle programs have grown fastest over the last three completed fiscal years?`
- [ ] `Which programs materially accelerated in FY2025 and FY2026?`
- [ ] `Why is PAC-3 ranked above the next program?`
- [ ] `Separate obligation growth, award activity, supplier activity, solicitations, budgets and production announcements.`
- [ ] `Which programs have growing budgets but limited observed award momentum?`
- [ ] `Which programs show production announcements ahead of obligation growth?`
- [ ] `Which supplier categories are most exposed to the JASSM/LRASM ramp?`
- [ ] `Which programs have expanding supplier participation?`
- [ ] `Which programs are growing while supplier participation contracts?`
- [ ] `Compare completed-year growth with the current partial fiscal year.`
- [ ] `Show the budget and FYDP evidence behind this forward view.`

Required answer behaviour:

- Completed fiscal years and partial current-year observations are visibly separated.
- Announced ceilings, requested budgets, enacted funding and obligations are not treated as equivalent.
- Rankings explain their calculation and link to underlying evidence without exposing internal implementation fields.

## 7. Competitive position and supplier discovery

- [ ] `Who has the strongest observed position in power systems across Army ground vehicles?`
- [ ] `Compare L3Harris and Collins Aerospace on military helicopters.`
- [ ] `Who has the strongest observed position in F-35 avionics?`
- [ ] `Who are Eaton Aerospace's closest observed competitors, and why?`
- [ ] `Find suppliers of missile antennas.`
- [ ] `Find companies similar to this supplier.`
- [ ] `Which companies overlap most with this supplier by products and programs?`
- [ ] `Which suppliers appear to be gaining position in tactical communications?`
- [ ] `Which suppliers added new platform positions since FY2021?`
- [ ] `Where does RTX have relatively little observed content compared with its capabilities?`
- [ ] `Which aircraft use competitor products similar to products this company supplies elsewhere?`
- [ ] `Which programs could be relevant to a supplier of ruggedized power conversion equipment?`
- [ ] `Who currently supplies those requirements?`

Required answer behaviour:

- Competition is derived from product, component, program, customer, award and NIIN overlap rather than NAICS similarity alone.
- “Share” is used only where the denominator is defensible; otherwise the answer uses mapped value, rank, awards, programs or supplier activity.
- Whitespace is framed as evidence-backed adjacency, not a claim that a sale is accessible or qualification is easy.

## 8. Current events and external evidence

- [ ] Paste a DoD contract announcement and ask `What does this mean?`
- [ ] Paste a reputable defense-news URL with no accompanying instruction.
- [ ] `Which suppliers may benefit from this production increase?`
- [ ] `Which site is most likely to perform this work?`
- [ ] `How significant is the announced value relative to historical activity?`
- [ ] `Do current budgets and FYDP plans support the announcement?`
- [ ] `Are there relevant prior R&D, prototype or OTA awards?`
- [ ] `Which existing site suppliers could be relevant?`
- [ ] `What evidence remains unconfirmed?`

Required answer behaviour:

- The article itself is retrieved and cited, not inferred from its URL slug.
- Government and company primary sources take precedence for the award facts.
- Well-supported inference is clearly phrased as inference without smothering the answer in caveats.
- External evidence enriches Mimir records and does not silently overwrite them.

## 9. Evidence, provenance and exports

- [ ] `Where did this number come from?`
- [ ] `Show the underlying contracts and public records.`
- [ ] `Why was this supplier included?`
- [ ] `Explain how this subcontract value was modelled.`
- [ ] `Show the original report and the selected revision.`
- [ ] `Which evidence is platform-specific and which is family-wide?`
- [ ] `Export the companies and supporting evidence.`
- [ ] `Export every supplier in this answer.`
- [ ] `Give me a CSV of awards supporting this conclusion.`

Required answer behaviour:

- Customer evidence uses understandable public identifiers and direct links, not internal record keys.
- Evidence drawers state `showing X of Y` and do not imply that a preview is the full universe.
- Professional evidence exports include the complete bounded result up to the documented row allowance.
- Guest/free/Lite users can inspect on-screen evidence but cannot download gated evidence packs.
- Export metadata records the subject, period, active filters, measure definitions and generation date.

## 10. Data-quality edge cases

- [ ] Negative contract actions remain de-obligations rather than disappearing.
- [ ] Subaward revisions do not create duplicate reported value.
- [ ] A zeroed or corrected latest subaward report follows the documented revision method.
- [ ] Reported subcontract value above a stale prime-action ceiling is not silently converted to zero.
- [ ] DLA sales lines are deduplicated using their source identifiers.
- [ ] Part-number references do not multiply NIIN financials.
- [ ] Multi-platform item associations do not multiply platform financial totals.
- [ ] Contract identity uses the full available award identity where PIID alone is ambiguous.
- [ ] Recipient address and place of performance are labelled and used separately.
- [ ] Missing CAGE resolution is shown as `No CAGE found`, not assigned by name guesswork.
- [ ] Parent ownership changes do not rewrite historic site identity.
- [ ] Source snapshot and Mimir release dates can be inspected internally for every answer.

## 11. Access, quota and security

- [ ] Guest receives the public daily allowance across repeated browser requests.
- [ ] Logged-in free, trial, Lite, Professional and Enterprise users receive the configured allowance.
- [ ] Signed-in identity is recognized on `/ask-mimir` on desktop and mobile.
- [ ] A user cannot raise their tier with client headers, cookies or query parameters.
- [ ] Concurrent submissions cannot exceed quota through a race condition.
- [ ] A failed model call refunds the query where policy says it should.
- [ ] Trial and Lite cannot download evidence or CSV exports.
- [ ] Professional export row and monthly limits are enforced server-side.
- [ ] Prompt or evidence content is not exposed in public analytics.
- [ ] Feedback is attached to the correct answer and user/guest subject.
- [ ] Admin and audit-log access is restricted.

## 12. Reliability and experience

- [ ] Progress text reflects real stages and the elapsed timer never resets mid-query.
- [ ] Render restart responses retry safely without double charging.
- [ ] A model no-answer response becomes a useful retry or clarification, not a blank result.
- [ ] Clarification choices are clickable and preserve the original question.
- [ ] A user can start a new question and return to the Ask Mimir opening screen.
- [ ] Company, platform, award, opportunity, NSN and evidence links open the correct destination.
- [ ] Tables fit mobile screens with usable horizontal scrolling and no hidden controls.
- [ ] Long answers are complete rather than cut off after a section heading.
- [ ] Cached answers are bound to the data release and cannot mix old and new evidence.
- [ ] The service remains usable during a release publication and switches only after the complete manifest is available.

## 13. Out-of-domain and unsafe requests

- [ ] A clearly unrelated question is redirected briefly to supported defense-industrial-base analysis.
- [ ] A mixed question answers the relevant defense component and identifies what is outside scope.
- [ ] The model does not invent coverage for foreign, commercial or classified activity absent from the evidence.
- [ ] Requests for classified, export-controlled or proprietary uploads receive an appropriate warning.
- [ ] Prompt injection in a pasted article cannot reveal secrets, system prompts, internal paths or other users' queries.
- [ ] A user cannot use evidence export endpoints to enumerate unrestricted underlying datasets.

## Recommended pass thresholds

- Resolver: 100% of known-identity tests resolve correctly; ambiguous identities never resolve silently.
- Evidence: at least 95% of material factual claims have the expected evidence route; zero fabricated citations.
- Financials: 100% of displayed values carry the correct measure and period.
- Language: zero internal identifiers, SQL, file paths or release labels in customer answers.
- Reliability: at least 98% successful completion excluding deliberate quota and validation rejections.
- Latency for in-depth answers: median under 60 seconds and 90th percentile under 120 seconds.
- Tracking: 100% of submitted questions record an outcome; completed API jobs record latency and estimated cost.
- Regression: all automated launch evaluations and deterministic unit tests pass against the release candidate.

## Beta decision

The service is ready for a public beta only when all release gates pass, every section has at least one automated test and the remaining manual cases have no severity-one or severity-two failures. Lower-severity answer-quality gaps may enter beta only if they are recorded, measurable and do not create a wrong identity, wrong financial claim or unsupported supplier relationship.
