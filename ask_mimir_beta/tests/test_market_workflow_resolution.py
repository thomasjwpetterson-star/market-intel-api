import unittest

from capability_discovery import capability_market_follow_up_intent, resolve_capability
from geographic_market import (
    is_geographic_market_request,
    resolve_state,
    state_market_follow_up_intent,
)
from platform_intent import platform_follow_up_intent
from market_segment import market_segment_follow_up_intent, resolve_market_segment


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

    def test_alabama_market_request_resolves(self):
        question = "Give me an overview of the defence industrial base in Alabama."
        self.assertEqual(resolve_state(question), "AL")
        self.assertTrue(is_geographic_market_request(question))

    def test_capability_follow_up_is_retained(self):
        self.assertTrue(
            capability_market_follow_up_intent(
                "Which of these suppliers support multiple military aircraft platforms?"
            )
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

    def test_market_segment_follow_up_is_retained(self):
        self.assertTrue(
            market_segment_follow_up_intent(
                "Which companies appear most important across the market?"
            )
        )


if __name__ == "__main__":
    unittest.main()
