"""Interactive reads have their own admission lane, using existing readers."""
from contextvars import ContextVar
from contextlib import contextmanager
import threading

DASHBOARD_REQUEST = ContextVar("mimir_dashboard_request", default=False)
BACKGROUND_ANALYSIS = ContextVar("mimir_background_analysis", default=False)


class ReaderReservation:
    """Let background reads borrow idle capacity, yielding a reader to browsing.

    Existing scans finish under their execution deadline. Once a dashboard is
    waiting or active, new background reads may occupy at most one reader.
    """
    def __init__(self):
        self.condition = threading.Condition()
        self.demand = self.background = self.foreground = 0

    def dashboard_demand(self, change):
        with self.condition:
            self.demand += change
            self.condition.notify_all()

    @contextmanager
    def read(self, background):
        from analytical_deadline import remaining_query_budget
        with self.condition:
            if not background:
                self.demand += 1
            try:
                while (self.background + self.foreground >= 2 or
                       (background and self.background >= (1 if self.demand else 2))):
                    remaining = remaining_query_budget()
                    self.condition.wait(.1 if remaining is None else min(.1, remaining))
                if background:
                    self.background += 1
                else:
                    self.foreground += 1
            except BaseException:
                if not background:
                    self.demand -= 1
                self.condition.notify_all()
                raise
        try:
            yield
        finally:
            with self.condition:
                if background:
                    self.background -= 1
                else:
                    self.foreground -= 1
                    self.demand -= 1
                self.condition.notify_all()


READERS = ReaderReservation()
PREFIXES = ("/api/platform/", "/api/company/", "/api/nsn/", "/api/award/",
            "/api/dashboard/", "/api/pipeline/", "/api/search/")


def is_dashboard_read(scope):
    return scope.get("type") == "http" and scope.get("method") == "GET" and scope.get("path", "").startswith(PREFIXES)
