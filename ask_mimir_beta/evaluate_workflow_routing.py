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
    routing_decision_for_request,
    runtime,
    validate_routing_decision,
    workflow_for_request,
)
from market_record_search import resolve_market_record_search
from market_segment import resolve_market_segment


ROUTING_CASES = {
    "product_intelligence": [
        "Find out everything about the Leonardo DRS flight recorder product line.",
        "What is the outlook for DFIRS 2100?",
        "Assess the EAS3000F and ELB3000F product family.",
        "What supports the M-346 AJT CSMU?",
    ],
    "platform_intelligence": [
        "Tell me about the F-16 market.",
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
        "Give me a summary on Ontic.",
        "Tell me everything about AeroCore X - including relevant platforms and contracts.",
        "What does Triumph Group do in the US defense market?",
        "Give me an overview of Collins Aerospace's defence activity in Cedar Rapids, Iowa.",
        "Which programs and capabilities are associated with BAE Systems' York, Pennsylvania facility?",
        "Give me an overview of RTXs defense business.",
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
        "Tell me about the aviation fuel controls market.",
        "What is the market for aircraft engine fuel controls?",
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
        "Find US manufacturers supplying avionics to military aircraft.",
        "Which companies supply aircraft electrical power-management equipment?",
        "Find military antenna and waveguide suppliers.",
        "Who manufactures tactical communications equipment?",
        "Find electro-optical and infrared equipment suppliers.",
        "Identify energetic initiation-component manufacturers.",
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
        "What's happening in the US tactical missile market at the moment?",
        "Give me the current outlook for US tactical missiles.",
    ],
    "general_defense_research": [
        "Who competes with Honeywell in military avionics?",
        "What does this contract announcement mean?",
        "How concentrated is the US defense industrial base?",
        "Where should an avionics supplier sell into US defense?",
        "What are the strongest defense growth signals right now?",
        "Tell me about the broader defense ecosystem.",
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


FUZZY_PARAPHRASE_FAMILIES = {
    "platform_intelligence": {
        "subjects": ("F16", "Tomahawk", "CH53K", "Virginia class"),
        "templates": (
            "Can you map the firms behind {subject}?",
            "I need the industrial picture for {subject}.",
            "Who's actually involved with {subject}?",
            "Walk me through {subject} suppliers and what they make.",
            "pls show the {subject} supply chain",
        ),
    },
    "company_site_intelligence": {
        "subjects": ("Honeywell", "Moog", "Curtiss Wright", "Parker Hannifin"),
        "templates": (
            "What does {subject} actually do in US defence?",
            "Build me a defence profile of {subject}.",
            "Can u map {subject}'s military business?",
            "Customers, facilities and programs for {subject}, please.",
            "Give me the analyst view on {subject} in defense.",
        ),
    },
    "item_intelligence": {
        "subjects": (
            "NSN 1560-00-817-5790",
            "NIIN 015794366",
            "part no UK 60A890216",
            "1680015794366",
        ),
        "templates": (
            "Can you look up {subject}?",
            "Who makes or supplies {subject}?",
            "Price, sources and platforms for {subject}.",
            "Tell me what we know about {subject}.",
            "pls find the procurement history for {subject}",
        ),
    },
    "contract_or_opportunity": {
        "subjects": (
            "N0002417C2100",
            "W56HZV23C0024",
            "FA820625F0006",
            "SPE4A626PC235",
        ),
        "templates": (
            "What's behind {subject}?",
            "Explain the work and award history for {subject}.",
            "Who got {subject} and what was bought?",
            "Can you pull the public record for {subject}?",
            "diligence {subject} for me",
        ),
    },
    "capability_discovery": {
        "subjects": (
            "aircraft fuel controls",
            "military landing gear",
            "radar components",
            "rugged mission computers",
        ),
        "templates": (
            "Map the US defence ecosystem for {subject}.",
            "Which manufacturers are credible in {subject}?",
            "I need a supplier landscape for {subject}.",
            "Who has demonstrated military work in {subject}?",
            "find firms making {subject} for defense",
        ),
    },
    "market_segment_intelligence": {
        "subjects": (
            "military rotorcraft",
            "fighter aircraft",
            "naval shipbuilding",
            "missiles and munitions",
        ),
        "templates": (
            "What's going on in US {subject}?",
            "Give me the market picture for {subject}.",
            "Which programs and firms drive {subject}?",
            "How is the {subject} industrial base changing?",
            "pls summarise the US {subject} market",
        ),
    },
    "state_industrial_base": {
        "subjects": ("Alabama", "Connecticut", "Texas", "Arizona"),
        "templates": (
            "What defence work happens in {subject}?",
            "Map the military industrial footprint in {subject}.",
            "Which facilities matter most in {subject}?",
            "Give me an aerospace and defense profile of {subject}.",
            "top defence firms and programs in {subject}",
        ),
    },
    "market_record_search": {
        "subjects": (
            "aircraft avionics",
            "vehicle electronics",
            "missile propulsion",
            "electronic warfare",
        ),
        "templates": (
            "What open opportunities are relevant to {subject} suppliers?",
            "Find live Sources Sought about {subject}.",
            "Show recent defence awards involving {subject}.",
            "Any current RFIs for {subject}?",
            "search open solicitations for {subject}",
        ),
    },
}


def generated_fuzzy_cases():
    for expected, family in FUZZY_PARAPHRASE_FAMILIES.items():
        for subject in family["subjects"]:
            for template in family["templates"]:
                yield expected, template.format(subject=subject)


ROUTING_ROBUSTNESS_CASES = {
    "misspellings": [
        ("platform_intelligence", "Show me the F-16 suply chain."),
        ("platform_intelligence", "Who are the main Tomahwk suppliers?"),
        ("company_site_intelligence", "Give me a compny profile for Honeywell."),
        ("company_site_intelligence", "What does Curtiss Wrigth do in defence?"),
        ("capability_discovery", "Find manufaturers of military aircraft brakes."),
        ("capability_discovery", "Who supplies ruged mission computers for defense?"),
        ("market_record_search", "Find open oportunities for avionics suppliers."),
        ("state_industrial_base", "Map the defence industral base in Alabama."),
    ],
    "ambiguous_company_names": [
        ("company_site_intelligence", "Tell me about Collins."),
        ("company_site_intelligence", "What does Mercury supply to defense?"),
        ("company_site_intelligence", "Profile RTX's US military business."),
        ("company_site_intelligence", "Show me the defence footprint of GE Aerospace."),
        ("company_site_intelligence", "Which sites belong to Ontic?"),
        ("company_site_intelligence", "What does Boeing do for the US military?"),
    ],
    "multiple_entities": [
        ("platform_comparison", "Compare the F-16 and F-35 supplier bases."),
        ("platform_comparison", "Who supplies both the UH-60 and CH-47?"),
        ("platform_comparison", "Contrast Patriot with THAAD."),
        ("platform_comparison", "Compare Tomahawk, JASSM and LRASM suppliers."),
        ("general_defense_research", "Compare Honeywell and Moog's US defence positions."),
        ("general_defense_research", "Which Alabama and Florida sites support missile programs?"),
    ],
    "current_events": [
        ("news_article_implications", "https://www.defensenews.com/example/article"),
        (
            "news_article_implications",
            "Read this announcement and assess the supplier implications: the Navy awarded a new multiyear submarine-production contract.",
        ),
        (
            "news_article_implications",
            "What does this news mean for the industrial base? The Army announced a production increase for counter-UAS systems.",
        ),
        (
            "general_defense_research",
            "What changed after the Pentagon's latest munitions announcement?",
        ),
    ],
    "unsupported_and_non_defense": [
        ("out_of_domain", "What will the weather be in London tomorrow?"),
        ("out_of_domain", "Recommend somewhere for dinner in Boston."),
        ("out_of_domain", "Write a Python tutorial for beginners."),
        ("out_of_domain", "Who won last night's basketball game?"),
        ("general_defense_research", "Estimate the global commercial-airline seating market."),
        ("general_defense_research", "Predict a private company's share price next month."),
    ],
}


FOLLOW_UP_CASES = [
    (
        "platform_intelligence",
        ActiveScope(scope_type="platform", scope_id="F-16", scope_name="F-16"),
        "Why is that?",
    ),
    (
        "capability_discovery",
        ActiveScope(
            scope_type="capability_market",
            scope_id="aviation_fuel_controls",
            scope_name="Aviation fuel controls",
        ),
        "Tell me about the broader ecosystem.",
    ),
    (
        "company_site_intelligence",
        ActiveScope(
            scope_type="company_parent",
            scope_id="PARENT_MOOG",
            scope_name="MOOG INC.",
            resolved_cages=["77777", "88888"],
        ),
        "Go deeper on those positions.",
    ),
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
        "capability_discovery",
        ActiveScope(
            scope_type="capability_market",
            scope_id="capability:aviation fuel controls",
            scope_name="Aviation Fuel Controls",
        ),
        "US military",
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
    (
        "company_site_intelligence",
        ActiveScope(
            scope_type="company_parent",
            scope_id="PARENT_MOOG",
            scope_name="MOOG INC.",
            resolved_cages=["77777", "88888"],
        ),
        "Which platforms does the Blacksburg site support?",
    ),
    (
        "company_site_intelligence",
        ActiveScope(
            scope_type="company_parent",
            scope_id="PARENT_HONEYWELL",
            scope_name="HONEYWELL INTERNATIONAL INC.",
            resolved_cages=["09128", "0BFA5", "99193"],
        ),
        "Now do Honeywell Phoenix.",
    ),
    (
        "platform_intelligence",
        ActiveScope(scope_type="platform", scope_id="F-16", scope_name="F-16"),
        "Give me the full supplier list.",
    ),
    (
        "item_intelligence",
        ActiveScope(
            scope_type="item",
            scope_id="004050631",
            scope_name="1280-00-405-0631",
        ),
        "What is the part number?",
    ),
    (
        "item_intelligence",
        ActiveScope(
            scope_type="item",
            scope_id="004050631",
            scope_name="1280-00-405-0631",
        ),
        "Which suppliers are associated with it?",
    ),
    (
        "contract_or_opportunity",
        ActiveScope(
            scope_type="contract",
            scope_id="N0002417C2100",
            scope_name="N0002417C2100",
        ),
        "Who received it?",
    ),
    (
        "contract_or_opportunity",
        ActiveScope(
            scope_type="opportunity",
            scope_id="PANRSA26P000012345",
            scope_name="Example opportunity",
        ),
        "What is the deadline?",
    ),
    (
        "company_site_intelligence",
        ActiveScope(
            scope_type="company_parent",
            scope_id="PARENT_HONEYWELL",
            scope_name="HONEYWELL INTERNATIONAL INC.",
            resolved_cages=["09128", "0BFA5", "99193"],
        ),
        "Company wide",
    ),
    (
        "company_site_intelligence",
        ActiveScope(
            scope_type="company_parent",
            scope_id="PARENT_HONEYWELL",
            scope_name="HONEYWELL INTERNATIONAL INC.",
            resolved_cages=["09128", "0BFA5", "99193"],
        ),
        "What prime contractors buy from them?",
    ),
    (
        "platform_intelligence",
        ActiveScope(scope_type="platform", scope_id="TOMAHAWK", scope_name="Tomahawk"),
        "Who are the major first tier suppliers?",
    ),
    (
        "platform_intelligence",
        ActiveScope(scope_type="platform", scope_id="TOMAHAWK", scope_name="Tomahawk"),
        "How concentrated is the supplier base?",
    ),
    (
        "market_record_search",
        ActiveScope(
            scope_type="record_search",
            scope_id="OPP|ANY|electronic warfare",
            scope_name="Opportunity search: electronic warfare",
        ),
        "Any information on the history of any of these contracts, such as past incumbents?",
    ),
    (
        "contract_or_opportunity",
        ActiveScope(
            scope_type="contract",
            scope_id="N0002417C2100",
            scope_name="N0002417C2100",
        ),
        "Can you analyse the action history and how the work evolved?",
    ),
    (
        "platform_comparison",
        ActiveScope(
            scope_type="platform_comparison",
            scope_id="UH-60 | CH-47",
            scope_name="UH-60 and CH-47",
            compared_platform_ids=["UH-60", "CH-47"],
        ),
        "Where do the two platforms share important suppliers or capabilities?",
    ),
    (
        "item_intelligence",
        ActiveScope(
            scope_type="item",
            scope_id="004050631",
            scope_name="1280-00-405-0631",
        ),
        "What is the part number?",
    ),
    (
        "item_intelligence",
        ActiveScope(
            scope_type="item",
            scope_id="004050631",
            scope_name="1280-00-405-0631",
        ),
        "part no?",
    ),
    (
        "capability_discovery",
        ActiveScope(
            scope_type="capability_market",
            scope_id="capability:aircraft engine fuel controls",
            scope_name="Aircraft engine fuel controls",
        ),
        "US military",
    ),
    (
        "capability_discovery",
        ActiveScope(
            scope_type="capability_market",
            scope_id="capability:aircraft engine fuel controls",
            scope_name="Aircraft engine fuel controls",
        ),
        "broader ecosystem pls",
    ),
    (
        "company_site_intelligence",
        ActiveScope(
            scope_type="company_parent",
            scope_id="PARENT_MOOG",
            scope_name="MOOG INC.",
            resolved_cages=["94697"],
        ),
        "Which platforms matter most?",
    ),
    (
        "company_site_intelligence",
        ActiveScope(
            scope_type="company_parent",
            scope_id="PARENT_MOOG",
            scope_name="MOOG INC.",
            resolved_cages=["94697"],
        ),
        "How has activity changed since FY21?",
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
    (
        "platform_intelligence",
        ActiveScope(
            scope_type="company_parent",
            scope_id="PARENT_HONEYWELL",
            scope_name="HONEYWELL INTERNATIONAL INC.",
            resolved_cages=["09128", "0BFA5", "99193"],
        ),
        [
            ChatMessage(role="user", content="Tell me about Honeywell's US defense business."),
            ChatMessage(role="assistant", content="Here is Honeywell's observed footprint."),
            ChatMessage(role="user", content="Who supplies the Virginia class?"),
        ],
    ),
    (
        "platform_intelligence",
        ActiveScope(
            scope_type="company_parent",
            scope_id="PARENT_MOOG",
            scope_name="MOOG INC.",
            resolved_cages=["94697"],
        ),
        [
            ChatMessage(role="user", content="Tell me about Moog's defense business."),
            ChatMessage(role="assistant", content="Here is Moog's observed footprint."),
            ChatMessage(role="user", content="Now show me F16 suppliers"),
        ],
    ),
    (
        "company_site_intelligence",
        ActiveScope(scope_type="platform", scope_id="F-16", scope_name="F-16"),
        [
            ChatMessage(role="user", content="Who supplies the F-16?"),
            ChatMessage(role="assistant", content="Here is the observed supplier base."),
            ChatMessage(role="user", content="Actually, tell me about Moog Blacksburg."),
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
        "What is the part number?": None,
        "Who received it?": None,
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
        "Identify suppliers of aerospace fuel systems.": "aircraft_fuel_systems",
        "Who makes military-aircraft landing gear?": "aircraft_landing_gear",
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
    fuzzy_results = []
    results_by_dimension = {}

    def record(dimension, expected, question, actual):
        result = (expected, question, actual)
        results.append(result)
        results_by_dimension.setdefault(dimension, []).append(result)

    for expected, questions in ROUTING_CASES.items():
        for question in questions:
            request = AskRequest(messages=[ChatMessage(role="user", content=question)])
            record("baseline_workflows", expected, question, workflow_for_request(request))
    for expected, question in generated_fuzzy_cases():
        request = AskRequest(messages=[ChatMessage(role="user", content=question)])
        result = (expected, question, workflow_for_request(request))
        record("natural_phrasing_and_shorthand", *result)
        fuzzy_results.append(result)
    for dimension, cases in ROUTING_ROBUSTNESS_CASES.items():
        for expected, question in cases:
            request = AskRequest(messages=[ChatMessage(role="user", content=question)])
            record(dimension, expected, question, workflow_for_request(request))
    for expected, active_scope, question in FOLLOW_UP_CASES:
        request = AskRequest(
            messages=[ChatMessage(role="user", content=question)],
            active_scope=active_scope,
        )
        record("pronoun_and_context_follow_ups", expected, question, workflow_for_request(request))
    for expected, active_scope, messages in CONVERSATION_CASES:
        request = AskRequest(messages=messages, active_scope=active_scope)
        record("subject_changes", expected, messages[-1].content, workflow_for_request(request))

    failures = [result for result in results if result[0] != result[2]]
    print(
        f"Ask Mimir routing: {len(results) - len(failures)}/{len(results)} passed; "
        f"{len(failures)} failed; 0 OpenAI calls."
    )
    for expected, question, actual in failures:
        print(f"FAIL | expected={expected} actual={actual} | {question}")
    fuzzy_failures = [result for result in fuzzy_results if result[0] != result[2]]
    print(
        f"Generated fuzzy routing: {len(fuzzy_results) - len(fuzzy_failures)}/"
        f"{len(fuzzy_results)} passed."
    )
    print("Coverage by dimension:")
    for dimension, dimension_results in sorted(results_by_dimension.items()):
        dimension_failures = [row for row in dimension_results if row[0] != row[2]]
        print(
            f"  {dimension}: {len(dimension_results) - len(dimension_failures)}/"
            f"{len(dimension_results)} passed"
        )
    telemetry_requests = [
        AskRequest(messages=[ChatMessage(role="user", content="Show me F-16 suppliers")]),
        AskRequest(messages=[ChatMessage(role="user", content="Tell me about the broader defense ecosystem.")]),
        AskRequest(
            messages=[ChatMessage(role="user", content="What is the part number?")],
            active_scope=ActiveScope(
                scope_type="item",
                scope_id="004050631",
                scope_name="1280-00-405-0631",
            ),
        ),
        AskRequest(
            messages=[ChatMessage(role="user", content="Actually, show me F-16 suppliers")],
            active_scope=ActiveScope(
                scope_type="company_parent",
                scope_id="PARENT_MOOG",
                scope_name="MOOG INC.",
                resolved_cages=["94697"],
            ),
        ),
    ]
    telemetry_failures = []
    for request in telemetry_requests:
        decision = routing_decision_for_request(request)
        if (
            not decision.reason
            or not 0 <= decision.confidence <= 1
            or decision.intended_workflow is None
            or (request.active_scope and decision.current_scope is None)
            or (decision.workflow != "general_defense_research" and not decision.candidates)
            or (
                request.messages[-1].content.lower().startswith("actually")
                and not (decision.subject_changed and decision.user_correction)
            )
        ):
            telemetry_failures.append(decision.model_dump())
    print(
        f"Routing telemetry: {len(telemetry_requests) - len(telemetry_failures)}/"
        f"{len(telemetry_requests)} passed."
    )
    for failure in telemetry_failures:
        print(f"FAIL | routing_telemetry={failure}")
    validation_cases = [
        ("Tell me about Ontic", False),
        ("Tell me about ZZQX Nonexistent Aerostructures", True),
    ]
    validation_failures = []
    for question, expected_clarification in validation_cases:
        request = AskRequest(messages=[ChatMessage(role="user", content=question)])
        decision = validate_routing_decision(
            request, routing_decision_for_request(request)
        )
        if decision.clarification_needed != expected_clarification:
            validation_failures.append(decision.model_dump())
    print(
        f"Routing validation gate: {len(validation_cases) - len(validation_failures)}/"
        f"{len(validation_cases)} passed."
    )
    for failure in validation_failures:
        print(f"FAIL | routing_validation={failure}")
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
    if failures or resolution_failures or telemetry_failures or validation_failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
