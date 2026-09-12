import unittest

from beta_controls import (
    remove_unsafe_and_internal_answer_content,
    validate_answer_citations,
)
from capability_discovery import (
    CAPABILITY_DEFINITIONS,
    _capability_domains,
    _capability_terms,
    capability_market_follow_up_intent,
    load_capability_ontology,
    resolve_capability,
    summarize_platform_breadth,
)
from geographic_market import (
    is_geographic_market_request,
    resolve_state,
    state_market_follow_up_intent,
)
from platform_intent import platform_follow_up_intent
from market_segment import market_segment_follow_up_intent, resolve_market_segment
from market_record_search import resolve_market_record_search


class MarketWorkflowResolutionTests(unittest.TestCase):
    def test_aircraft_braking_capability_resolves(self):
        self.assertEqual(
            resolve_capability(
                "Find US manufacturers that supply braking systems or brake components to military aircraft."
            ),
            "aircraft_braking",
        )

    def test_aircraft_anti_skid_capability_resolves(self):
        self.assertEqual(
            resolve_capability(
                "Which US suppliers have evidence of military-aircraft brake or anti-skid content?"
            ),
            "aircraft_braking",
        )

    def test_aircraft_actuation_overview_resolves(self):
        self.assertEqual(
            resolve_capability(
                "Give me an overview of this US defense market, capability area or industrial base: aircraft actuation"
            ),
            "aircraft_actuation",
        )

    def test_open_landing_gear_capability_uses_governed_definition(self):
        self.assertEqual(
            resolve_capability(
                "Give me an overview of this US defense market, capability area or industrial base: Military Landing Gear"
            ),
            "aircraft_landing_gear",
        )

    def test_governed_technology_markets_resolve_before_dynamic_fallback(self):
        cases = {
            "Who makes military-aircraft landing gear?": "aircraft_landing_gear",
            "Find aircraft environmental-control equipment manufacturers.": "aircraft_environmental_control",
            "Who supplies electronic warfare equipment?": "electronic_warfare",
            "Find missile propulsion system suppliers.": "missile_propulsion",
            "Find US manufacturers supplying avionics to military aircraft.": "military_avionics",
            "Find military-aircraft electrical power generation suppliers.": "aircraft_electrical_power",
            "Find missile antenna and waveguide suppliers.": "military_antennas_rf",
            "Who supplies tactical communications equipment?": "tactical_communications",
            "Find electro-optical and infrared system manufacturers.": "electro_optical_infrared",
            "Find suppliers capable of producing energetic components.": "energetic_components",
        }
        for question, expected in cases.items():
            with self.subTest(question=question):
                self.assertEqual(resolve_capability(question), expected)

    def test_ungoverned_flight_recorder_market_uses_dynamic_evidence_search(self):
        self.assertEqual(
            resolve_capability("Give me an overview of the military flight recorder market."),
            "capability:military flight recorder",
        )

    def test_natural_language_aircraft_fuel_market_uses_governed_definition(self):
        for question in (
            "Give me an overview of aerospace fuel systems in the US defense market.",
            "Give me an overview of the US market for aircraft fuel systems.",
            "What is happening across the military-aircraft fuel-system ecosystem?",
        ):
            with self.subTest(question=question):
                self.assertEqual(resolve_capability(question), "aircraft_fuel_systems")

    def test_general_capability_supplier_phrasings_resolve_dynamically(self):
        cases = {
            "Who makes military-aircraft landing gear?": "aircraft_landing_gear",
            "Find companies with flight-control experience.": "capability:flight-control experience",
            "Identify suppliers of aerospace fuel systems.": "aircraft_fuel_systems",
            "Find suppliers capable of producing energetic components.": "energetic_components",
        }
        for question, expected in cases.items():
            with self.subTest(question=question):
                self.assertEqual(resolve_capability(question), expected)

    def test_aerospace_qualifier_scopes_fuel_without_becoming_a_search_term(self):
        self.assertEqual(_capability_terms("aerospace fuel systems"), ["fuel"])
        self.assertEqual(
            _capability_domains("aerospace fuel systems"),
            (["AIR"], ["aircraft", "aviation", "airborne", "aerospace"]),
        )

    def test_aircraft_fuel_systems_definition_covers_the_full_functional_chain(self):
        definition = CAPABILITY_DEFINITIONS["aircraft_fuel_systems"]
        lanes = " ".join(definition["included_lanes"]).lower()
        for expected_term in ("tanks", "pumps", "engine fuel-control", "gauging", "test equipment"):
            with self.subTest(expected_term=expected_term):
                self.assertIn(expected_term, lanes)

    def test_platform_breadth_does_not_make_the_leader_a_market_center(self):
        summary = summarize_platform_breadth(
            [
                {"platform": "F-15", "matching_niin_count": 615},
                {"platform": "F-16", "matching_niin_count": 359},
                {"platform": "C-130", "matching_niin_count": 249},
                {"platform": "C/KC-135", "matching_niin_count": 220},
            ]
        )
        self.assertEqual(summary["leading_platform"], "F-15")
        self.assertFalse(summary["single_platform_dominates_associations_shown"])
        self.assertLess(summary["leading_platform_share_of_associations_shown"], 0.5)

    def test_alabama_market_request_resolves(self):
        question = "Give me an overview of the defence industrial base in Alabama."
        self.assertEqual(resolve_state(question), "AL")
        self.assertTrue(is_geographic_market_request(question))

    def test_named_company_site_is_not_routed_as_state_market(self):
        question = (
            "Give me an overview of the defence activity associated with "
            "Lockheed Martin's Orlando, Florida operations."
        )
        self.assertFalse(is_geographic_market_request(question))

    def test_geographic_shorthand_resolves(self):
        for question, state in (
            ("Alabama defense suppliers", "AL"),
            ("Show the US defense footprint in Ohio.", "OH"),
            ("Map aerospace and defense activity across Connecticut.", "CT"),
        ):
            with self.subTest(question=question):
                self.assertEqual(resolve_state(question), state)
                self.assertTrue(is_geographic_market_request(question))

    def test_capability_follow_up_is_retained(self):
        self.assertTrue(
            capability_market_follow_up_intent(
                "Which of these suppliers support multiple military aircraft platforms?"
            )
        )

    def test_capability_clarification_selection_is_retained(self):
        for answer in (
            "US military",
            "U.S. defense",
            "the broader ecosystem",
            "complete units",
            "both",
        ):
            with self.subTest(answer=answer):
                self.assertTrue(capability_market_follow_up_intent(answer))

    def test_general_capability_market_phrasing_resolves(self):
        for question, expected_phrase in (
            (
                "Tell me about the aviation fuel controls market",
                "aviation_fuel_controls",
            ),
            (
                "Tell be about the aviation fuel controls market",
                "aviation_fuel_controls",
            ),
            (
                "What is the market for aircraft engine fuel controls?",
                "aviation_fuel_controls",
            ),
        ):
            with self.subTest(question=question):
                self.assertEqual(resolve_capability(question), expected_phrase)

    def test_capability_ontology_is_valid_and_governed(self):
        ontology = load_capability_ontology()
        self.assertEqual(ontology["schema_version"], 1)
        self.assertEqual(ontology["capabilities"], CAPABILITY_DEFINITIONS)
        self.assertGreaterEqual(len(CAPABILITY_DEFINITIONS), 15)
        for capability_id, definition in CAPABILITY_DEFINITIONS.items():
            with self.subTest(capability_id=capability_id):
                self.assertIn(
                    definition["evidence_mode"],
                    {
                        "classification_complete",
                        "classification_complete_sparse_items",
                        "description_bounded",
                        "hybrid",
                    },
                )

    def test_state_market_follow_up_is_retained(self):
        self.assertTrue(
            state_market_follow_up_intent(
                "Which defence companies and facilities appear most important in the state?"
            )
        )

    def test_state_platform_capability_follow_up_is_retained(self):
        self.assertTrue(
            state_market_follow_up_intent(
                "Which platforms and capability areas account for the most visible activity?"
            )
        )

    def test_platform_evidence_follow_up_is_retained(self):
        self.assertTrue(
            platform_follow_up_intent(
                "Show me the evidence supporting that conclusion."
            )
        )

    def test_platform_outlook_follow_up_is_retained(self):
        self.assertTrue(
            platform_follow_up_intent(
                "What are the main risks or uncertainties in that outlook?"
            )
        )

    def test_military_rotorcraft_market_resolves(self):
        self.assertEqual(
            resolve_market_segment(
                "What is happening in the US military rotorcraft market?"
            ),
            "US_MILITARY_ROTORCRAFT",
        )

    def test_market_segment_language_variants_resolve(self):
        cases = {
            "Give me an overview of the US rotorcraft market.": "US_MILITARY_ROTORCRAFT",
            "Which missile programs are driving the most activity?": "US_MISSILES_AND_MUNITIONS",
            "Show me the military airlift and tanker market.": "US_AIRLIFT_AND_TANKER_AIRCRAFT",
            "Which companies matter most across military UAS?": "US_UNCREWED_AIRCRAFT",
        }
        for question, expected in cases.items():
            with self.subTest(question=question):
                self.assertEqual(resolve_market_segment(question), expected)

    def test_space_launch_market_does_not_capture_happening_in_prefix(self):
        question = "What is happening in the US military space-launch market?"
        self.assertEqual(resolve_market_segment(question), "US_MILITARY_SPACE_SYSTEMS")
        self.assertEqual(resolve_capability(question), "capability:military space-launch")

    def test_named_submarine_platform_is_not_broadened_to_market(self):
        self.assertIsNone(
            resolve_market_segment(
                "Show the supplier base for the Virginia-class submarine."
            )
        )

    def test_market_segment_follow_up_is_retained(self):
        self.assertTrue(
            market_segment_follow_up_intent(
                "Which companies appear most important across the market?"
            )
        )

    def test_ground_vehicle_market_resolves(self):
        self.assertEqual(
            resolve_market_segment(
                "What is happening in the US military ground vehicle market?"
            ),
            "US_MILITARY_GROUND_VEHICLES",
        )

    def test_uncrewed_aircraft_market_resolves(self):
        self.assertEqual(
            resolve_market_segment("What is happening in the US military UAS market?"),
            "US_UNCREWED_AIRCRAFT",
        )

    def test_fighter_aircraft_market_resolves(self):
        self.assertEqual(
            resolve_market_segment("Which programs drive the US fighter jet market?"),
            "US_FIGHTER_AIRCRAFT",
        )

    def test_bomber_market_resolves(self):
        self.assertEqual(
            resolve_market_segment("Give me an overview of the US bomber market."),
            "US_BOMBER_AIRCRAFT",
        )

    def test_submarine_market_resolves(self):
        self.assertEqual(
            resolve_market_segment("What is happening in the US submarine industrial base?"),
            "US_SUBMARINES",
        )

    def test_opportunity_starter_accepts_colon_separator(self):
        result = resolve_market_record_search(
            "Find current US defense opportunities relevant to: aircraft thermal management"
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["record_type"], "opportunity")
        self.assertEqual(result["subject"], "aircraft thermal management")

    def test_what_opportunities_are_open_resolves(self):
        result = resolve_market_record_search(
            "What defense opportunities are open for radar manufacturers?"
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["record_type"], "opportunity")
        self.assertEqual(result["subject"], "radar")

    def test_recent_awards_concerning_subject_resolves(self):
        result = resolve_market_record_search(
            "Find recent awards concerning electronic warfare."
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["record_type"], "award")
        self.assertEqual(result["subject"], "electronic warfare")

    def test_market_record_search_accepts_common_natural_phrasings(self):
        cases = (
            (
                "What open opportunities are relevant to aircraft avionics suppliers?",
                "opportunity",
            ),
            (
                "Any current RFIs for electronic warfare?",
                "opportunity",
            ),
            (
                "Show recent defence awards involving missile propulsion.",
                "award",
            ),
            (
                "Find open oportunities for avionics suppliers.",
                "opportunity",
            ),
        )
        for question, record_type in cases:
            with self.subTest(question=question):
                result = resolve_market_record_search(question)
                self.assertIsNotNone(result)
                self.assertEqual(result["record_type"], record_type)

    def test_internal_identifier_is_removed_without_discarding_answer(self):
        answer = (
            "The leading supplier is supported by source_report_id 123. "
            "[Local evidence](http://localhost:3000/private)"
        )
        validation = validate_answer_citations(answer, [])
        cleaned = remove_unsafe_and_internal_answer_content(answer, validation)
        final_validation = validate_answer_citations(cleaned, [])
        self.assertIn("leading supplier", cleaned)
        self.assertNotIn("source_report_id", cleaned)
        self.assertNotIn("localhost", cleaned)
        self.assertEqual(final_validation["status"], "pass")

    def test_repository_link_and_internal_output_template_are_removed(self):
        answer = (
            "USAspending supports the fields. "
            "[Implementation](https://github.com/example/project/blob/main/spec.md)\n\n"
            "For every platform, the standardized output should be: "
            "Platform → supplier CAGE/site → fiscal year → value.\n\n"
            "The annual values are shown above."
        )
        validation = validate_answer_citations(answer, [])
        cleaned = remove_unsafe_and_internal_answer_content(answer, validation)
        final_validation = validate_answer_citations(cleaned, [])
        self.assertNotIn("github.com", cleaned)
        self.assertNotIn("standardized output", cleaned.lower())
        self.assertIn("annual values", cleaned)
        self.assertEqual(final_validation["status"], "pass")

    def test_internal_company_modelling_language_is_customerized(self):
        answer = (
            "A residual Missile systems (multiple programs) grouping represents "
            "$120.7 million of FY2021-FY2026 Mimir-modelled reported subcontract value."
        )
        validation = validate_answer_citations(answer, [])
        cleaned = remove_unsafe_and_internal_answer_content(answer, validation)

        self.assertIn(
            "Missile-related activity not attributable to one named program",
            cleaned,
        )
        self.assertIn("reported subcontract value", cleaned)
        self.assertNotIn("residual", cleaned.lower())
        self.assertNotIn("Mimir-modelled", cleaned)


if __name__ == "__main__":
    unittest.main()
