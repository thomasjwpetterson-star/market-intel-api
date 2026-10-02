"""Verify the explicitly requested pre-audit brief rollback without paid calls."""

import asyncio
import hashlib
import threading
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pandas as pd

import main


class OriginalBriefRestorationTests(unittest.IsolatedAsyncioTestCase):
    def test_entire_function_matches_pre_audit_source_except_worker_dispatch(self):
        # Frozen from de56e371cffafcc95338c8470428038043338da1:main.py.
        # Exact text protects prompts, amounts, headings, model settings,
        # response fields and error handling from another approximate rewrite.
        source = Path(main.__file__).read_text()
        start = source.index('@app.post("/api/public/unlock-brief")\n')
        end = source.index('\n\n# ==========================================\n#        PUBLIC TEASER & RATE LIMITING', start)
        function = source[start:end].rstrip()
        self.assertEqual(function.count('await run_in_threadpool('), 6)
        for name in ('query_summary_df', 'get_company_parts', 'get_subset_from_disk', 'get_company_network'):
            function = function.replace(f'await run_in_threadpool({name},', f'{name}(')
        self.assertEqual(hashlib.sha256(function.encode()).hexdigest(),
                         '0c7a2cc96dc594b8ed5fe67eee9410f22777fc61ccd0e758052a04d36ee92fd8')
        self.assertNotIn('public_brief_runtime', source)

    def mocked_data(self, stack, provider):
        stack.enter_context(patch.object(main, 'build_summary_where', return_value=('cage = ?', ['81755'])))
        stack.enter_context(patch.object(main, 'query_summary_df', return_value=pd.DataFrame()))
        parts = stack.enter_context(patch.object(main, 'get_company_parts', return_value=[]))
        awards = stack.enter_context(patch.object(main, 'get_subset_from_disk', return_value=pd.DataFrame()))
        stack.enter_context(patch.object(main, 'get_company_network', return_value={
            'primes': [{'name': 'PRIME CUSTOMER', 'total': 1_000_000.0}], 'subs': [],
        }))
        stack.enter_context(patch.object(main.aclient.chat.completions, 'create', provider))
        return parts, awards

    async def test_original_request_inputs_and_provider_text_are_returned_without_cache_or_rewrite(self):
        original_output = '**Position:** Original model wording.\n\n**Dependency:** Original detail.\n\n**Implication:** Original conclusion.'
        provider = AsyncMock(return_value=SimpleNamespace(choices=[SimpleNamespace(
            message=SimpleNamespace(content=original_output))]))
        body = {'name': 'TEST COMPANY', 'cage': '81755', 'prime_exposure': 3_000_000.0}
        with ExitStack() as stack:
            parts, awards = self.mocked_data(stack, provider)
            profile = stack.enter_context(patch.object(main, 'get_company_profile'))
            first = await main.generate_unlocked_brief(SimpleNamespace(json=AsyncMock(return_value=body)))
            second = await main.generate_unlocked_brief(SimpleNamespace(json=AsyncMock(return_value=body)))
        profile.assert_not_called()
        self.assertEqual(provider.await_count, 2)
        self.assertEqual(first, second)
        self.assertEqual(first['ai_brief'], original_output)
        self.assertEqual(set(first), {'success', 'ai_brief', 'headline_metric', 'deep_data'})
        self.assertEqual(set(first['deep_data']), {'platforms', 'agencies', 'nsns', 'nsn_period_label',
                                                  'contracts', 'network', 'network_title'})
        self.assertEqual(first['deep_data']['network_title'], 'Top Prime Customers')
        self.assertEqual(awards.call_args.kwargs['where_clause'], '(vendor_cage = ?) AND spend_amount >= 250000')
        self.assertEqual(awards.call_args.kwargs['columns_sql'], 'action_date, sub_agency, description, spend_amount')
        self.assertEqual(parts.call_args.kwargs['cage'], '81755')
        arguments = provider.call_args.kwargs
        self.assertEqual(set(arguments), {'model', 'messages', 'max_tokens', 'temperature'})
        self.assertEqual((arguments['model'], arguments['max_tokens'], arguments['temperature']), ('gpt-4o', 400, 0.2))
        self.assertIn('Entity: TEST COMPANY (CAGE: 81755)', arguments['messages'][1]['content'])
        self.assertIn('Total Mapped Revenue: $4.0M (75% Prime / 25% Sub)', arguments['messages'][1]['content'])

    async def test_original_failure_response_has_no_fallback_brief(self):
        provider = AsyncMock(side_effect=RuntimeError('mock provider failure'))
        with ExitStack() as stack:
            self.mocked_data(stack, provider)
            result = await main.generate_unlocked_brief(SimpleNamespace(json=AsyncMock(
                return_value={'name': 'TEST COMPANY', 'cage': '81755', 'prime_exposure': 1_000_000})))
        self.assertEqual(result, {'success': False, 'error': 'mock provider failure'})
        provider.assert_awaited_once()

    async def test_original_parent_contract_filter_and_parts_skip_are_restored(self):
        provider = AsyncMock(return_value=SimpleNamespace(choices=[SimpleNamespace(
            message=SimpleNamespace(content='Parent brief'))]))
        with ExitStack() as stack:
            parts, awards = self.mocked_data(stack, provider)
            result = await main.generate_unlocked_brief(SimpleNamespace(json=AsyncMock(
                return_value={'name': 'Parent Company', 'cage': 'AGGREGATE', 'is_parent': True, 'prime_exposure': 1_000_000})))
        self.assertIs(result['success'], True)
        parts.assert_not_called()
        self.assertEqual(awards.call_args.kwargs['where_clause'], '(upper(vendor_name) LIKE ?) AND spend_amount >= 250000')
        self.assertEqual(awards.call_args.kwargs['params'], ('%PARENT COMPANY%',))

    async def test_database_work_still_shares_service_worker_limit(self):
        entered, release = threading.Event(), threading.Event()
        thread_ids = []
        event_loop_thread = threading.get_ident()

        def blocked_query(*args, **kwargs):
            thread_ids.append(threading.get_ident())
            entered.set()
            release.wait(timeout=2)
            raise RuntimeError('mock query stopped')

        async def wait_until_entered():
            while not entered.is_set():
                await asyncio.sleep(0.001)

        limiter = main.anyio.to_thread.current_default_thread_limiter()
        previous_tokens = limiter.total_tokens
        limiter.total_tokens = 1
        tasks = []
        try:
            with patch.object(main, 'build_summary_where', return_value=('cage = ?', ['81755'])), \
                    patch.object(main, 'query_summary_df', side_effect=blocked_query):
                for index in range(2):
                    request = SimpleNamespace(json=AsyncMock(return_value={'name': 'TEST COMPANY', 'cage': '81755'}))
                    tasks.append(asyncio.create_task(main.generate_unlocked_brief(request)))
                    if index == 0:
                        await asyncio.wait_for(wait_until_entered(), timeout=1)
                await asyncio.sleep(0.03)
                self.assertEqual(len(thread_ids), 1)
                self.assertNotEqual(thread_ids[0], event_loop_thread)
                release.set()
                results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=2)
                self.assertEqual(len(thread_ids), 2)
                self.assertTrue(all(result == {'success': False, 'error': 'mock query stopped'} for result in results))
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
            limiter.total_tokens = previous_tokens


if __name__ == '__main__':
    unittest.main()
