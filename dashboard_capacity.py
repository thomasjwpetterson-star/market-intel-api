"""Queue browser reads separately from expensive research and crawler work.

One interactive query and one background query can run on the existing two
analytical readers. No new DuckDB connection or scan concurrency is introduced.
"""
from analytical_admission import AnalyticalAdmissionMiddleware
from dashboard_routes import DASHBOARD_REQUEST, READERS, is_dashboard_read


class DashboardBudgets(dict):
    def get(self, key, default=None):
        return 45 if key[0] == "GET" else default


class DashboardCapacityMiddleware:
    def __init__(self, app, generation=lambda: None):
        self.app = app
        self.generation = generation
        self.gate = AnalyticalAdmissionMiddleware(self._serve, pending_limit=24,
            pending_timeout=35, budgets=DashboardBudgets())

    async def __call__(self, scope, receive, send):
        if not is_dashboard_read(scope):
            return await self.app(scope, receive, send)
        READERS.dashboard_demand(1)
        try:
            await self.gate(scope, receive, send)
        finally:
            READERS.dashboard_demand(-1)

    async def _serve(self, scope, receive, send):
        token = DASHBOARD_REQUEST.set(True)
        READERS.dashboard_demand(1)
        generation = self.generation()
        async def send_with_generation(message):
            if message['type'] == 'http.response.start' and generation is not None:
                message = {**message, 'headers': [*message.get('headers', []),
                    (b'x-mimir-data-generation', str(generation).encode('ascii'))]}
            await send(message)
        try:
            await self.app(scope, receive, send_with_generation)
        finally:
            DASHBOARD_REQUEST.reset(token)
            READERS.dashboard_demand(-1)
