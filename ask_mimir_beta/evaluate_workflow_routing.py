"""Exercise Ask Mimir workflow routing without calling a language model."""

from __future__ import annotations

from collections import Counter

from capability_discovery import resolve_capability
from geographic_market import resolve_state
from lab_api import (
    ActiveScope,
    AskRequest,
    ChatMessage,
    explicit_award_or_opportunity_query,
    explicit_company_name_query,
    explicit_item_query,
    explicit_platform_query,
    runtime,
    workflow_for_request,
)
from market_record_search import resolve_market_record_search
from market_segment import resolve_market_segment


ROUTING_CASES = {
    "platform_intelligence": [
        "Show me F-16 suppliers",
        "Who supplies the F-35?",
        "F-16 suppliers",
        "Suppliers to F-16",
        "Give me F-35 vendors",
        "Which companies support the F-35?",
        "Show me firms involved in F-16",
        "Who builds the F-35?",
        "F-16 production outlook",
        "Map the Stryker supply chain",
        "Tell me about Tomahawk",
        "Give me an overview of AMRAAM",
        "Who benefits from increased SM-6 production?",
        "What components go into the CH-53K?",
        "Which subcontractors support HIMARS?",
        "Show the PAC-3 MSE industrial base",
        "How is B-52 modernization progressing?",
        "What does Raytheon provide on AMRAAM?",
        "Which facilities support Tomahawk?",
        "Who are the suppliers for F-16?",
        "Who supplies Patriot?",
        "Show the supplier base for the Virginia-class submarine.",
        "What firms work on Abrams?",
        "Tomahawk industrial base",
        "What is happening with CH-53K production?",
        "What does Lockheed Martin provide on F-35?",
        "List the main vendors supporting PAC-3 MSE.",
        "Map the industrial footprint behind SM-6.",
        "Show suppliers for F16",
        "Who supplies CH53K?",
        "PAC3 MSE suppliers",
        "SM6 production outlook",
        "Virginia class supply chain",
        "Tell me about the Abrams program.",
        "Map AH64 suppliers.",
        "What vendors support P8A?",
        "Who makes F35?",
        "Who manufactures Patriot?",
    ],
    "company_site_intelligence": [
        "Tell me everything about this defense supplier: Honeywell",
        "Give me an overview of Honeywell's US defense business.",
        "What does Honeywell supply into the US defence market?",
        "Which defence platforms does Honeywell support?",
        "What defence work is carried out at Moog facilities?",
        "Which CAGE codes are associated with Boeing?",
        "Who are Curtiss-Wright's largest defence customers?",
        "What are Parker Hannifin's largest visible defence positions?",
        "How has Ducommun's defence activity changed since FY2021?",
        "Which facilities belong to TransDigm and what does each one do?",
        "Give me an overview of Lockheed Martin's Camden, Arkansas facility.",
        "What does Honeywell's Clearwater facility manufacture?",
        "Which programs does Aerojet Rocketdyne's East Camden site support?",
        "Show me Boeing facilities",
        "Moog US defense business",
        "Moog Blacksburg",
        "Honeywell Clearwater capabilities",
        "Who buys from Moog?",
        "Who supplies Honeywell?",
        "What does CAGE 19645 do?",
        "CAGE 19645",
        "19645",
        "Tell me about Curtiss-Wright",
        "Profile RTX's observed US defence activity.",
        "Give me a concise defence-market profile of AeroVironment.",
        "What platforms does BAE Systems support?",
        "List Northrop Grumman sites.",
        "Raytheon Tucson",
        "Lockheed Martin Orlando",
        "Parker Hannifin company-wide",
        "Which suppliers does Boeing buy from?",
        "What evidence shows Honeywell supplies F-16?",
        "Tell me about Curtiss Wright",
        "BAE Systems York defence activity",
        "L3Harris Tewkesbury",
        "What does Moog make for defense?",
        "Who are Boeing's main defense customers?",
        "Show all TransDigm CAGE codes.",
        "Give me Honeywell company wide.",
        "What does CAGE 09128 manufacture?",
    ],
    "company_site_trajectory": [
        "How has CAGE 19645's missile-program exposure changed since FY2021?",
    ],
    "item_intelligence": [
        "Tell me everything about this NSN, NIIN or part number: 1560-00-817-5790",
        "Tell me about NSN 1680-01-579-4366",
        "Who supplies NSN 1560-00-817-5790?",
        "Show suppliers for NIIN 008175790",
        "What is NIIN 015794366?",
        "Price history for NSN 5305-01-579-4366",
        "Look up part number 2-200-070-67",
        "Tell me about part number UK 60A890216",
        "1560-00-817-5790",
        "NIIN 008175790",
        "NSN: 1560008175790",
        "NIIN: 015794366",
        "Part number: UK 60A890216",
        "Find vendors for 1680-01-579-4366.",
        "1680015794366",
        "NIIN 008175790 suppliers",
        "Suppliers for part no. 2-200-070-67",
        "What contracts reference NSN 1560-00-817-5790?",
    ],
    "contract_or_opportunity": [
        "Tell me everything about this defense contract award: N0002417C2100",
        "Tell me everything about this contract or opportunity: N0002417C2100",
        "Tell me about contract W56HZV23C0024",
        "Who won contract N0002417C2100?",
        "Show me award N0001920C0004",
        "Explain contract FA820625F0006",
        "N0002417C2100",
        "W56HZV23C0024",
        "FA820625F0006",
        "SPE4A626PC235",
        "Explain award HQ014721C0001.",
        "What was purchased under N0001923F2616?",
        "N0016424CJR15",
        "W9127N26PA012",
        "What is opportunity PANRSA26P000012345?",
    ],
    "market_record_search": [
        "Find current US defense opportunities relevant to: avionics",
        "Find current US defence opportunities relevant to companies supplying military avionics.",
        "Show open defense opportunities relevant to propulsion manufacturers.",
        "Find currently open Sources Sought notices related to military aircraft avionics.",
        "Find currently open RFIs related to unmanned aircraft systems.",
        "Find currently open solicitations related to military vehicle components.",
        "Find recent US defence contract awards related to military aircraft avionics.",
        "Show recent contract awards for missile propulsion work.",
        "What opportunities are open for radar manufacturers?",
        "Search current military-avionics contracting opportunities.",
        "Show open RFIs for unmanned systems.",
        "Find Sources Sought for aircraft electronics.",
        "Which live solicitations concern missile components?",
        "What defense opportunities are open for radar manufacturers?",
        "Find recent awards concerning electronic warfare.",
        "Show active RFIs concerning avionics.",
        "Find open requests for information about C-UAS.",
        "Which Sources Sought notices cover ship components?",
    ],
    "state_industrial_base": [
        "Give me an overview of the defence industrial base in Alabama.",
        "Which defence companies and facilities are most important in Texas?",
        "Map Connecticut's defense industrial base.",
        "What platforms drive defence activity in Arizona?",
        "Tell me about the defense industry in Florida.",
        "Show defense contractors in California.",
        "Alabama defense suppliers",
        "Show the US defense footprint in Ohio.",
        "Which military programs matter most to Colorado?",
        "Map aerospace and defense activity across Connecticut.",
        "Florida military industry overview",
        "Who are the leading defense firms in Pennsylvania?",
        "Defense activity by facility in Arizona",
    ],
    "market_segment_intelligence": [
        "What is happening in the US military ground vehicle market?",
        "Give me an overview of the US rotorcraft market.",
        "Which programs drive the US fighter aircraft market?",
        "What is the outlook for the US bomber market?",
        "Assess the US submarine industrial base.",
        "What is happening in the US C-UAS market?",
        "Tell me about the US unmanned aircraft market.",
        "Give me an overview of the air and missile defence market.",
        "Which missile programs are driving the most activity?",
        "Show me the military airlift and tanker market.",
        "How is the fighter aircraft market changing?",
        "Which programs drive military rotorcraft activity?",
        "What is happening in tactical missiles?",
        "Give me a submarine market overview.",
        "Which companies matter most across military UAS?",
        "Give me the US fighter jet market outlook.",
        "What programs matter across US bombers?",
        "Describe the naval shipbuilding market.",
        "Who matters in military space systems?",
    ],
    "capability_discovery": [
        "Find US manufacturers that supply braking systems to military aircraft.",
        "Which US suppliers make aircraft actuation equipment?",
        "Find suppliers of military radar components.",
        "Identify US manufacturers of rugged mission computers.",
        "Find US manufacturers supplying environmental-control or thermal-management equipment for military aircraft.",
        "Which companies make missile antennas?",
        "Who makes military-aircraft landing gear?",
        "Find companies with flight-control experience.",
        "Identify suppliers of aerospace fuel systems.",
        "Which manufacturers provide electronic warfare equipment?",
        "Find suppliers capable of producing energetic components.",
        "Show manufacturers of military aircraft fuel pumps.",
        "Who manufactures rugged computers for defense platforms?",
        "Identify companies supplying missile seeker electronics.",
    ],
    "platform_comparison": [
        "Compare the supplier bases of the UH-60 Black Hawk and CH-47 Chinook.",
        "Compare F-16 and F-35 suppliers.",
        "Which suppliers overlap between Tomahawk and JASSM?",
        "How do the Stryker and Bradley industrial bases differ?",
        "Show shared suppliers across PAC-3 MSE and THAAD.",
        "What supplier capabilities do F-15 and F-16 have in common?",
        "Contrast the Abrams and Stryker industrial bases.",
        "Who supplies both CH-53K and V-22?",
        "Compare suppliers on F16 versus F35.",
        "Where do Apache and Black Hawk suppliers overlap?",
        "Differences between Patriot and THAAD suppliers",
    ],
    "news_article_implications": [
        "https://www.defensenews.com/example/article",
        "Analyse this news article and explain the implications: The Department of Defense announced a major production award for a missile system, including new capacity and supplier investment across several US sites.",
        "Analyze this article text: The Army awarded a production contract expected to increase output materially over the next five years and expand the supplier base.",
        "What are the industrial-base implications of this article? The Navy announced a new multi-year procurement award for submarine components.",
        "What does this announcement mean? The Air Force announced a five-year production award for guided weapons, including supplier investment and expanded factory capacity.",
    ],
    "competitor_discovery": [
        "Who are Eaton Aerospace's closest observed competitors?",
        "Which companies compete with Eaton Aerospace?",
        "Find firms similar to Eaton Aerospace.",
    ],
    "defined_market_competitive_position": [
        "Who has the strongest observed position in power systems across Army ground vehicles?",
        "Which suppliers have leading electrical-power positions on US Army ground vehicles?",
    ],
    "program_momentum": [
        "Which missile programs are accelerating fastest?",
        "Rank accelerating missile programs by procurement momentum.",
    ],
    "general_defense_research": [
        "Who competes with Honeywell in military avionics?",
        "What does this contract announcement mean?",
        "How concentrated is the US defense industrial base?",
        "Where should an avionics supplier sell into US defense?",
        "What are the strongest defense growth signals right now?",
    ],
    "out_of_domain": [
        "What is the weather forecast?",
        "Write me a recipe for lasagna.",
        "Who won the football game?",
        "Tell me tomorrow's weather.",
        "Recommend a restaurant in London.",
        "Write a JavaScript tutorial.",
        "Help me plan a holiday in Spain.",
    ],
}


FOLLOW_UP_CASES = [
    (
        "platform_intelligence",
        ActiveScope(scope_type="platform", scope_id="F-16", scope_name="F-16"),
        "What does each of those suppliers provide?",
    ),
    (
        "platform_intelligence",
        ActiveScope(scope_type="platform", scope_id="F-16", scope_name="F-16"),
        "Show me the evidence supporting that conclusion.",
    ),
    (
        "company_site_intelligence",
        ActiveScope(
            scope_type="company_site",
            scope_id="19645",
            scope_name="Data Device Corporation",
            resolved_cages=["19645"],
        ),
        "Which platforms appear most important to it?",
    ),
    (
        "state_industrial_base",
        ActiveScope(scope_type="state_market", scope_id="AL", scope_name="Alabama"),
        "Which facilities appear most important in the state?",
    ),
    (
        "market_segment_intelligence",
        ActiveScope(
            scope_type="market_segment",
            scope_id="US_MILITARY_ROTORCRAFT",
            scope_name="US military rotorcraft market",
        ),
        "Which companies appear most important across the market?",
    ),
    (
        "capability_discovery",
        ActiveScope(
            scope_type="capability_market",
            scope_id="aircraft_braking",
            scope_name="Military-aircraft braking systems and components",
        ),
        "Which of these suppliers support multiple platforms?",
    ),
    (
        "market_record_search",
        ActiveScope(
            scope_type="record_search",
            scope_id="OPP|ANY|military avionics",
            scope_name="Opportunity search: military avionics",
        ),
        "Tell me more about the third one.",
    ),
]


CONVERSATION_CASES = [
    (
        "platform_intelligence",
        None,
        [
            ChatMessage(role="user", content="https://www.defensenews.com/example/article"),
            ChatMessage(role="assistant", content="The announcement expands production."),
            ChatMessage(role="user", content="Who supplies F-16?"),
        ],
    ),
    (
        "company_site_intelligence",
        ActiveScope(scope_type="platform", scope_id="F-16", scope_name="F-16"),
        [
            ChatMessage(role="user", content="Who supplies F-16?"),
            ChatMessage(role="assistant", content="Here is the observed supplier base."),
            ChatMessage(role="user", content="Tell me about Honeywell's US defense business."),
        ],
    ),
    (
        "item_intelligence",
        ActiveScope(scope_type="platform", scope_id="F-16", scope_name="F-16"),
        [
            ChatMessage(role="user", content="Who supplies F-16?"),
            ChatMessage(role="assistant", content="Here is the observed supplier base."),
            ChatMessage(role="user", content="Tell me about NSN 1560-00-817-5790."),
        ],
    ),
    (
        "out_of_domain",
        ActiveScope(scope_type="platform", scope_id="F-16", scope_name="F-16"),
        [
            ChatMessage(role="user", content="Who supplies F-16?"),
            ChatMessage(role="assistant", content="Here is the observed supplier base."),
            ChatMessage(role="user", content="What is tomorrow's weather?"),
        ],
    ),
]


RESOLUTION_CASES = {
    "platform": {
        "Show me F-16 suppliers": "F-16",
        "Who supplies Patriot?": "PATRIOT AIR DEFENSE SYSTEM",
        "Show the supplier base for the Virginia-class submarine.": "VIRGINIA CLASS (SSN 774)",
        "How is B-52 modernization progressing?": "B-52",
    },
    "company_query": {
        "Tell me about Curtiss-Wright": "Curtiss-Wright",
        "Give me an overview of Lockheed Martin's Camden, Arkansas facility.": "Lockheed Martin Camden Arkansas",
        "What does Honeywell's Clearwater facility manufacture?": "Honeywell Clearwater",
        "Which programs does Aerojet Rocketdyne's East Camden site support?": "Aerojet Rocketdyne East Camden",
        "Parker Hannifin company-wide": "Parker Hannifin",
    },
    "item": {
        "Find vendors for 1680-01-579-4366.": "1680015794366",
        "Tell me about part number UK 60A890216": "UK 60A890216",
        "1560-00-817-5790": "1560008175790",
    },
    "award": {
        "What was purchased under N0001923F2616?": "N0001923F2616",
        "W56HZV23C0024": "W56HZV23C0024",
    },
    "state": {
        "Alabama defense suppliers": "AL",
        "Show the US defense footprint in Ohio.": "OH",
    },
    "segment": {
        "Give me an overview of the US rotorcraft market.": "US_MILITARY_ROTORCRAFT",
        "Which missile programs are driving the most activity?": "US_MISSILES_AND_MUNITIONS",
    },
    "capability": {
        "Identify suppliers of aerospace fuel systems.": "capability:aerospace fuel systems",
        "Who makes military-aircraft landing gear?": "capability:military-aircraft landing gear",
    },
}

COMPANY_MATCH_CASES = {
    "Curtiss-Wright": {"04808"},
    "Lockheed Martin Camden Arkansas": {"62313"},
    "Honeywell Clearwater": {"09128", "0BFA5"},
    "Aerojet Rocketdyne East Camden": {"62006"},
    "Parker Hannifin": {"59211"},
}


def _resolution_failures():
    failures = []
    for kind, cases in RESOLUTION_CASES.items():
        for question, expected in cases.items():
            messages = [ChatMessage(role="user", content=question)]
            if kind == "platform":
                actual = explicit_platform_query(messages, runtime.platform_contexts)
            elif kind == "company_query":
                actual = explicit_company_name_query(messages)
            elif kind == "item":
                actual = explicit_item_query(messages)
            elif kind == "award":
                actual = explicit_award_or_opportunity_query(messages)
            elif kind == "state":
                actual = resolve_state(question)
            elif kind == "segment":
                actual = resolve_market_segment(question)
            else:
                actual = resolve_capability(question)
            if actual != expected:
                failures.append((kind, question, expected, actual))
    for query, expected_cages in COMPANY_MATCH_CASES.items():
        result = runtime.company_contexts.search(query, limit=20)
        actual_cages = {
            str(row.get("scope_id") or "").upper()
            for row in result.get("matches", [])
            if row.get("scope_type") == "company_site"
        }
        if not expected_cages.issubset(actual_cages):
            failures.append(
                (
                    "company_match",
                    query,
                    sorted(expected_cages),
                    sorted(actual_cages),
                )
            )
    return failures


def main() -> None:
    results = []
    for expected, questions in ROUTING_CASES.items():
        for question in questions:
            request = AskRequest(messages=[ChatMessage(role="user", content=question)])
            results.append((expected, question, workflow_for_request(request)))
    for expected, active_scope, question in FOLLOW_UP_CASES:
        request = AskRequest(
            messages=[ChatMessage(role="user", content=question)],
            active_scope=active_scope,
        )
        results.append((expected, question, workflow_for_request(request)))
    for expected, active_scope, messages in CONVERSATION_CASES:
        request = AskRequest(messages=messages, active_scope=active_scope)
        results.append((expected, messages[-1].content, workflow_for_request(request)))

    failures = [result for result in results if result[0] != result[2]]
    print(
        f"Ask Mimir routing: {len(results) - len(failures)}/{len(results)} passed; "
        f"{len(failures)} failed; 0 OpenAI calls."
    )
    for expected, question, actual in failures:
        print(f"FAIL | expected={expected} actual={actual} | {question}")
    print("Routes exercised:", dict(sorted(Counter(row[2] for row in results).items())))
    resolution_failures = _resolution_failures()
    resolution_total = (
        sum(len(cases) for cases in RESOLUTION_CASES.values())
        + len(COMPANY_MATCH_CASES)
    )
    print(
        f"Scope and entity resolution: {resolution_total - len(resolution_failures)}/"
        f"{resolution_total} passed."
    )
    for kind, question, expected, actual in resolution_failures:
        print(
            f"FAIL | resolver={kind} expected={expected!r} actual={actual!r} | {question}"
        )
    if failures or resolution_failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
