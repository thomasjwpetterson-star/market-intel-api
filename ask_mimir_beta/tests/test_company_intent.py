import unittest

from company_intent import company_follow_up_intent, company_wide_intent


class CompanyIntentTests(unittest.TestCase):
    def test_what_company_actually_does_is_company_wide(self):
        self.assertTrue(
            company_wide_intent(
                "What does VSE Corporation actually do in the defense market?"
            )
        )

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
