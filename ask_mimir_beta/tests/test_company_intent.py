import unittest

from company_intent import company_follow_up_intent


class CompanyIntentTests(unittest.TestCase):
    def test_company_change_question_retains_active_company_scope(self):
        self.assertTrue(
            company_follow_up_intent(
                "Which parts of Eaton's defence activity appear to have changed most significantly over the last five years?"
            )
        )

    def test_unrelated_initial_question_does_not_inherit_company_scope(self):
        self.assertFalse(company_follow_up_intent("Who supplies the Black Hawk?"))


if __name__ == "__main__":
    unittest.main()
