"""Protect the original brief plus the explicitly requested numeric corrections."""

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
    def test_original_source_is_preserved_except_workers_and_requested_numeric_fixes(self):
        # Frozen from de56e371cffafcc95338c8470428038043338da1:main.py.
        # Reverse only the authorized numeric fixes before comparing the original
        # hash. Prompts, queries, structure, provider settings and response/error
        # handling remain protected from another approximate rewrite.
        source = Path(main.__file__).read_text()
        start = source.index('@app.post("/api/public/unlock-brief")\n')
        end = source.index('\n\n# ==========================================\n#        PUBLIC TEASER & RATE LIMITING', start)
        function = source[start:end].rstrip()
        self.assertEqual(function.count('await run_in_threadpool('), 6)
        for name in ('query_summary_df', 'get_company_parts', 'get_subset_from_disk', 'get_company_network'):
            function = function.replace(f'await run_in_threadpool({name},', f'{name}(')
        numeric_changes = (
            (
                '        # Match the public snapshot\'s full upstream total, not just the ten displayed partners.\n'
                '        sub_spend = float(primes_list[0].get("network_total") or 0.0) if primes_list else 0.0\n'
                '        if sub_spend <= 0:\n'
                '            sub_spend = sum(float(p.get("total", 0) or 0) for p in primes_list)',
                '        sub_spend = sum(float(p.get("total", 0) or 0) for p in primes_list)',
            ),
            ('        def fmt_m(val): return _format_brief_money(val)',
             '        def fmt_m(val): return f"${val/1_000_000:.1f}M"'),
            (
                '        prime_share = _format_brief_mix_share(prime_spend, sub_spend, total_mapped)\n'
                '        sub_share = _format_brief_mix_share(sub_spend, prime_spend, total_mapped)',
                '        prime_pct = (prime_spend / total_mapped * 100) if total_mapped > 0 else 0\n'
                '        sub_pct = (sub_spend / total_mapped * 100) if total_mapped > 0 else 0',
            ),
            (
                '        Copy the supplied monetary amounts and prime/subcontract percentages exactly, including decimal places and < or > signs. Do not round the prime/subcontract mix to whole percentages.\n',
                '',
            ),
            (
                '        - Total Mapped Revenue: {fmt_m(total_mapped)} ({prime_share} Prime / {sub_share} Sub)\n'
                '        - Prime Contract Value: {fmt_m(prime_spend)}\n'
                '        - Subcontract Value: {fmt_m(sub_spend)}',
                '        - Total Mapped Revenue: {fmt_m(total_mapped)} ({prime_pct:.0f}% Prime / {sub_pct:.0f}% Sub)',
            ),
        )
        for updated, original in numeric_changes:
            self.assertEqual(function.count(updated), 1, updated)
            function = function.replace(updated, original)
        self.assertEqual(hashlib.sha256(function.encode()).hexdigest(),
                         '0c7a2cc96dc594b8ed5fe67eee9410f22777fc61ccd0e758052a04d36ee92fd8')
        self.assertNotIn('public_brief_runtime', source)

    def mocked_data(self, stack, provider, network=None):
        stack.enter_context(patch.object(main, 'build_summary_where', return_value=('cage = ?', ['81755'])))
        stack.enter_context(patch.object(main, 'query_summary_df', return_value=pd.DataFrame()))
        parts = stack.enter_context(patch.object(main, 'get_company_parts', return_value=[]))
        awards = stack.enter_context(patch.object(main, 'get_subset_from_disk', return_value=pd.DataFrame()))
        stack.enter_context(patch.object(main, 'get_company_network', return_value=network if network is not None else {
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
        self.assertIn('Total Mapped Revenue: $4M (75.00% Prime / 25.00% Sub)', arguments['messages'][1]['content'])
        self.assertIn('Prime Contract Value: $3M', arguments['messages'][1]['content'])
        self.assertIn('Subcontract Value: $1M', arguments['messages'][1]['content'])

    async def test_81755_uses_all_upstream_partners_and_preserves_small_subcontract_share(self):
        provider = AsyncMock(return_value=SimpleNamespace(choices=[SimpleNamespace(
            message=SimpleNamespace(content='Unmodified provider wording'))]))
        network = {
            'primes': [{'name': 'VISIBLE PRIME CUSTOMER', 'total': 411_271_184.0,
                        'network_total': 442_844_038.33000004},
                       {'name': 'OTHER VISIBLE PRIME CUSTOMERS', 'total': 28_243_004.79}],
            'subs': [{'name': 'DOWNSTREAM SUBCONTRACTOR', 'total': 6_904_118_351.81,
                      'network_total': 50_940_044_259.16005}],
        }
        with ExitStack() as stack:
            self.mocked_data(stack, provider, network)
            result = await main.generate_unlocked_brief(SimpleNamespace(json=AsyncMock(return_value={
                'name': 'LOCKHEED MARTIN CORPORATION', 'cage': '81755',
                'prime_exposure': 175_766_233_950.50998,
                'sub_exposure': 1,  # Never use a submitted display value in place of the server total.
            })))
        self.assertTrue(result['success'])
        self.assertEqual(result['ai_brief'], 'Unmodified provider wording')
        signals = provider.call_args.kwargs['messages'][1]['content']
        self.assertIn('Total Mapped Revenue: $176.2B (99.75% Prime / 0.25% Sub)', signals)
        self.assertIn('Prime Contract Value: $175.8B', signals)
        self.assertIn('Subcontract Value: $442.8M', signals)
        self.assertNotIn('Subcontract Value: $411.3M', signals)
        self.assertNotIn('Subcontract Value: $50.9B', signals)
        self.assertIn('Copy the supplied monetary amounts and prime/subcontract percentages exactly',
                      provider.call_args.kwargs['messages'][0]['content'])

    async def test_network_total_fallback_matches_public_snapshot(self):
        cases = (
            ([], '$3M (100.00% Prime / 0.00% Sub)', '$0'),
            ([{'total': 1_000_000}], '$4M (75.00% Prime / 25.00% Sub)', '$1M'),
            ([{'total': 1_000_000, 'network_total': None}], '$4M (75.00% Prime / 25.00% Sub)', '$1M'),
            ([{'total': 1_000_000, 'network_total': 0}], '$4M (75.00% Prime / 25.00% Sub)', '$1M'),
            ([{'total': 1_000_000, 'network_total': -1}], '$4M (75.00% Prime / 25.00% Sub)', '$1M'),
            ([{'total': 750_000}, {'total': 250_000}], '$4M (75.00% Prime / 25.00% Sub)', '$1M'),
        )
        for rows, expected_mix, expected_sub in cases:
            with self.subTest(rows=rows):
                provider = AsyncMock(return_value=SimpleNamespace(choices=[SimpleNamespace(
                    message=SimpleNamespace(content='Provider wording'))]))
                with ExitStack() as stack:
                    self.mocked_data(stack, provider, {'primes': rows, 'subs': []})
                    result = await main.generate_unlocked_brief(SimpleNamespace(json=AsyncMock(return_value={
                        'name': 'TEST COMPANY', 'cage': '81755', 'prime_exposure': 3_000_000,
                    })))
                self.assertTrue(result['success'])
                signals = provider.call_args.kwargs['messages'][1]['content']
                self.assertIn(f'Total Mapped Revenue: {expected_mix}', signals)
                self.assertIn(f'Subcontract Value: {expected_sub}', signals)

    def test_brief_money_uses_compact_units_and_promotes_rounding_boundaries(self):
        examples = (
            (0, '$0'), (12.34, '$12.34'), (500, '$500'), (1_500, '$1.5K'),
            (442_844_038.33, '$442.8M'), (175_766_233_950.51, '$175.8B'),
            (176_205_700_000, '$176.2B'), (1_500_000_000_000, '$1.5T'),
            (1_250_000, '$1.3M'), (-1_250_000, '-$1.3M'),
            (999_949.99, '$999.9K'), (999_950, '$1M'), (999_950_000, '$1B'),
            (999_950_000_000, '$1T'), (-442_844_038.33, '-$442.8M'), (-12.34, '-$12.34'),
        )
        for amount, expected in examples:
            with self.subTest(amount=amount):
                self.assertEqual(main._format_brief_money(amount), expected)
        self.assertEqual(main._format_brief_money(float('nan')), 'Not reported')
        self.assertEqual(main._format_brief_money(float('inf')), 'Not reported')

    def test_mix_share_distinguishes_zero_from_tiny_nonzero_amounts(self):
        examples = (
            ((0, 0, 0), '0.00%'), ((1, 0, 1), '100.00%'), ((0, 1, 1), '0.00%'),
            ((1, 1_000_000, 1_000_001), '<0.01%'),
            ((1_000_000, 1, 1_000_001), '>99.99%'),
            ((1e-20, 1e20, 1e20), '<0.01%'), ((1e20, 1e-20, 1e20), '>99.99%'),
            ((-1, 101, 100), '-1.00%'), ((101, -1, 100), '101.00%'),
        )
        for arguments, expected in examples:
            with self.subTest(arguments=arguments):
                self.assertEqual(main._format_brief_mix_share(*arguments), expected)

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
