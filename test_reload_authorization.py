import ast
import hmac
import os
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from fastapi import HTTPException, Request


class ReloadAuthorizationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        node = next(n for n in ast.parse(Path(__file__).with_name('main.py').read_text()).body
                    if isinstance(n, ast.AsyncFunctionDef) and n.name == 'trigger_reload')
        node.decorator_list = []
        self.scheduler = SimpleNamespace(create_task=Mock(), to_thread=Mock(return_value='reload-task'))
        self.ns = dict(os=os, hmac=hmac, Request=Request, HTTPException=HTTPException,
                       asyncio=self.scheduler, RELOAD_LOCK=Mock(locked=Mock(return_value=False)),
                       GLOBAL_CACHE={}, reload_all_data=Mock())
        exec(compile(ast.Module(body=[node], type_ignores=[]), 'main.py', 'exec'), self.ns)

    async def test_unconfigured_missing_and_wrong_keys_never_schedule_work(self):
        for configured, supplied, status in [('', '', 503), ('service-secret', '', 403), ('service-secret', 'wrong', 403), ('service-secret', 'non-ascii-é', 403)]:
            with self.subTest(configured=bool(configured), supplied=bool(supplied)):
                with patch.dict(os.environ, {'MIMIR_RELOAD_SECRET': configured}):
                    with self.assertRaises(HTTPException) as error:
                        await self.ns['trigger_reload'](SimpleNamespace(headers={'X-Mimir-Reload-Key':supplied}))
                self.assertEqual(error.exception.status_code, status)
        self.scheduler.create_task.assert_not_called()
        self.scheduler.to_thread.assert_not_called()

    async def test_authorized_reload_preserves_in_progress_guard(self):
        request = SimpleNamespace(headers={'X-Mimir-Reload-Key':'service-secret'})
        with patch.dict(os.environ, {'MIMIR_RELOAD_SECRET':'service-secret'}):
            self.assertEqual(await self.ns['trigger_reload'](request), {'message':'Reloading...'})
            self.scheduler.create_task.assert_called_once_with('reload-task')
            self.ns['GLOBAL_CACHE']['is_loading'] = True
            self.assertEqual(await self.ns['trigger_reload'](request), {'message':'Reload already running'})
            self.assertEqual(self.scheduler.create_task.call_count, 1)
