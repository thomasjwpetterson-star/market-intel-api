import unittest

from capability_discovery import resolve_capability
from geographic_market import (
    is_geographic_market_request,
    resolve_state,
    state_market_follow_up_intent,
)


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

    def test_state_market_follow_up_is_retained(self):
        self.assertTrue(
            state_market_follow_up_intent(
                "Which defence companies and facilities appear most important in the state?"
            )
        )


if __name__ == "__main__":
    unittest.main()
