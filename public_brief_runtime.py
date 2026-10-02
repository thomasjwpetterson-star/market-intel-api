"""Process-local inference safeguards for the existing public company preview.

Only generated text is cached. Callers build fresh evidence and response data.
This is a capacity safeguard, not authentication or a durable spending quota.
"""

import asyncio
import hashlib
import json
import logging
import math
import re
import time
import unicodedata
from collections import OrderedDict
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple


logger = logging.getLogger("mimir-api")

PUBLIC_BRIEF_SOURCE_POLICY = "concise-business-brief-v2"
PUBLIC_BRIEF_METHODOLOGY = (
    "Based on government contract and subcontract records. Values cover the periods shown and may overlap; "
    "subcontract totals include Mimir adjustments."
)


def format_brief_currency(value: float) -> str:
    """Compact display only; precise monetary evidence remains in the cache key."""
    if not math.isfinite(value):
        raise ValueError("Brief currency must be finite")
    amount = abs(value)
    sign = "-" if value < 0 else ""
    for divisor, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if amount >= divisor or (divisor > 1e3 and round(amount / (divisor / 1000), 1) >= 1000):
            return f"{sign}${amount / divisor:.1f}{suffix}"
    number = f"{amount:.2f}".rstrip("0").rstrip(".")
    return f"{sign}${number}"


def validate_brief_narrative(text: str, max_text_chars: int = 6000) -> str:
    """Keep source methodology outside the business narrative.

    This is a narrow claim-class boundary, not general semantic verification.
    DLA may still be named as an awarding agency. Removing spacing/punctuation
    prevents formatting variants of reserved source terms from bypassing it.
    """
    if not isinstance(text, str) or not text.strip():
        raise ValueError("Invalid brief result")
    if len(text) > max_text_chars:
        raise ValueError("Brief exceeds text limit")
    normalized = unicodedata.normalize("NFKC", text).casefold()
    compact = re.sub(r"[^a-z0-9]", "", normalized)
    reserved = ("usaspending", "federalactionobligation", "procurementlinevalue",
                "contractlinevalue", "netprice", "orderedquantity",
                "sourcecomposition", "sourcespecificsplit", "generaldatamethodology")
    if any(term in compact for term in reserved):
        raise ValueError("Generated brief includes reserved source methodology")
    # Apply the same readable formatting even if a model expands a supplied
    # amount. This changes presentation only, not the value's evidential basis.
    def compact_amount(match):
        amount = float(match.group("amount").replace(",", ""))
        multiplier = {"": 1, "k": 1e3, "thousand": 1e3, "m": 1e6, "million": 1e6,
                      "b": 1e9, "billion": 1e9, "t": 1e12, "trillion": 1e12}[(match.group("unit") or "").lower()]
        sign = -1 if match.group("before") or match.group("after") else 1
        return format_brief_currency(sign * amount * multiplier)

    return re.sub(r"(?P<before>-?)\$\s*(?P<after>-?)\s*(?P<amount>\d+(?:,\d{3})*(?:\.\d+)?)"
                  r"(?:\s*(?P<unit>trillion|billion|million|thousand|[KMBT]))?(?!\w)",
                  compact_amount, text, flags=re.IGNORECASE)


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
        position = f"{name} has a reference profile; prime contract values are not available."
    else:
        period = f" ({prime_period})" if prime_period else ""
        position = f"{name} has {format_brief_currency(prime_value)} in observed prime contract value{period}."
    if sub_value is not None:
        scope = " from displayed partners" if "displayed" in sub_basis.casefold() else ""
        position += f" Separately, tracked subcontract value totals {format_brief_currency(sub_value)}{scope}."
    return (
        f"**Position:** {position}\n\n"
        f"**Dependency:** {agency_summary}\n\n"
        "**Implication:** Use the available contract and customer detail to qualify sales or partnership opportunities."
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
