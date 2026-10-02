"""Process-local inference safeguards for the existing public company preview.

Only generated text is cached. Callers build fresh evidence and response data.
This is a capacity safeguard, not authentication or a durable spending quota.
"""

import asyncio
import hashlib
import json
import logging
import time
from collections import OrderedDict
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple


logger = logging.getLogger("mimir-api")


def brief_cache_key(completion_parameters: Dict[str, Any]) -> str:
    """Include the exact evidence, instructions, model and generation settings."""
    payload = json.dumps(completion_parameters, sort_keys=True, ensure_ascii=False,
                         separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def evidence_only_brief(*, name: str, prime_value: Optional[float],
                        prime_period: Optional[str], sub_value: Optional[float],
                        sub_basis: str, agency_summary: str) -> str:
    """Render supplied observations without turning absent coverage into zero."""
    if prime_value is None:
        position = f"The loaded reference identifies {name}; prime contract-value coverage is unavailable."
    else:
        period = f" over {prime_period}" if prime_period else "; the fiscal-year range is unavailable"
        position = f"Loaded records report ${prime_value:,.2f} in observed prime contract value for {name}{period}."
        position += " Depending on the available records, this measure can include USAspending net obligations and DLA procurement-line values (net price times ordered quantity)."
    if sub_value is None:
        position += " A subcontract-award breakdown is unavailable in the loaded records."
    else:
        position += f" {sub_basis}: ${sub_value:,.2f}, reported separately; its observation period is not established here."
    return (
        f"**Position:** {position}\n\n"
        f"**Dependency:** {agency_summary}\n\n"
        "**Implication:** This is an evidence-only summary of loaded records, not a complete view of company revenue. "
        "Prime and subcontract measures can overlap and should not be added together. "
        "Verify the underlying government records and reporting dates before a commercial decision; "
        "platform associations remain Mimir mappings."
    )


class PublicBriefRuntime:
    """Bound work on one service event loop, sharing identical in-flight calls.

    A disconnected waiter cannot cancel other customers' shared generation.
    New distinct work gets the caller's evidence-based fallback when capacity
    is occupied. Failures have a short cooldown and never enter the success TTL.
    """

    def __init__(self, *, max_entries: int = 128, ttl_seconds: float = 3600,
                 max_concurrent: int = 2, timeout_seconds: float = 20,
                 failure_cooldown_seconds: float = 15, max_text_chars: int = 6000,
                 clock: Callable[[], float] = time.monotonic):
        if min(max_entries, ttl_seconds, max_concurrent, timeout_seconds,
               failure_cooldown_seconds, max_text_chars) <= 0:
            raise ValueError("Public brief runtime limits must be positive")
        self.max_entries = max_entries
        self.ttl_seconds = ttl_seconds
        self.max_concurrent = max_concurrent
        self.timeout_seconds = timeout_seconds
        self.failure_cooldown_seconds = failure_cooldown_seconds
        self.max_text_chars = max_text_chars
        self.clock = clock
        self._cache: OrderedDict[str, Tuple[float, Optional[str]]] = OrderedDict()
        self._inflight: Dict[str, asyncio.Task] = {}

    def _remember(self, key: str, text: Optional[str], ttl_seconds: float) -> None:
        self._cache[key] = (self.clock() + ttl_seconds, text)
        self._cache.move_to_end(key)
        while len(self._cache) > self.max_entries:
            self._cache.popitem(last=False)

    async def _generate(self, key: str, generate: Callable[[], Awaitable[str]]) -> Optional[str]:
        try:
            text = await asyncio.wait_for(generate(), timeout=self.timeout_seconds)
            if not isinstance(text, str) or not text.strip() or len(text) > self.max_text_chars:
                raise ValueError("Invalid brief result")
            self._remember(key, text, self.ttl_seconds)
            logger.info("public_brief generation=completed")
            return text
        except Exception as exc:
            self._remember(key, None, self.failure_cooldown_seconds)
            # Provider errors may contain request data; log the class only.
            logger.warning("public_brief generation=fallback error_type=%s", type(exc).__name__)
            return None
        finally:
            self._inflight.pop(key, None)

    async def get_or_generate(self, key: str, generate: Callable[[], Awaitable[str]],
                              fallback: str) -> str:
        cached = self._cache.get(key)
        if cached is not None:
            expires_at, text = cached
            if expires_at > self.clock():
                self._cache.move_to_end(key)
                return text if text is not None else fallback
            self._cache.pop(key, None)

        task = self._inflight.get(key)
        if task is None:
            # No await between the capacity check and registration: operations
            # are atomic with respect to other requests on this event loop.
            if len(self._inflight) >= self.max_concurrent:
                logger.info("public_brief generation=fallback reason=capacity")
                return fallback
            task = asyncio.create_task(self._generate(key, generate))
            self._inflight[key] = task

        result = await asyncio.shield(task)
        return result if result is not None else fallback
