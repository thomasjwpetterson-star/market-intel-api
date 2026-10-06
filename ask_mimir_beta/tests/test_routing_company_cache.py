"""Routing lookups should reuse results without changing returned identities."""

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from lab_test_support import load_lab


lab = load_lab()


class RoutingCompanyCacheTests(unittest.TestCase):
    def test_repeat_name_lookup_is_cached_and_callers_cannot_mutate_it(self):
        row = {"scope_type": "company_site", "scope_id": "12345",
               "scope_name": "CURTISS-WRIGHT", "option_label": "CURTISS-WRIGHT"}
        directory = Mock()
        directory.search.return_value = {"matches": [row]}
        cache = lab.RoutingCompanySearchCache(directory, max_entries=2)
        first = cache.search("Curtiss-Wright", limit=10)
        first["matches"][0]["scope_name"] = "mutated"
        second = cache.search("  CURTISS-WRIGHT ", limit=10)
        self.assertEqual(second["matches"][0]["scope_name"], "CURTISS-WRIGHT")
        self.assertEqual(directory.search.call_count, 1)

    def test_name_validation_reuses_admission_lookup(self):
        row = {"scope_type": "company_site", "scope_id": "12345",
               "scope_name": "CURTISS-WRIGHT", "option_label": "CURTISS-WRIGHT"}
        directory = Mock()
        directory.search.return_value = {"matches": [row]}
        cache = lab.RoutingCompanySearchCache(directory)
        runtime = SimpleNamespace(routing_company_contexts=cache,
                                  company_contexts=directory)
        request = lab.AskRequest(messages=[{
            "role": "user", "content": "Give me an overview of Curtiss-Wright's US defence business."
        }])
        decision = lab.RoutingDecision(workflow="company_site_intelligence",
                                       reason="company_or_site_language", confidence=0.985)
        with patch.object(lab, "runtime", runtime, create=True):
            validated = lab.validate_routing_decision(request, decision)
            query = lab._validated_company_query(request)
        self.assertEqual(validated.workflow, "company_site_intelligence")
        self.assertEqual(query, "CURTISS WRIGHT")
        self.assertEqual(directory.search.call_count, 1)


if __name__ == "__main__":
    unittest.main()
