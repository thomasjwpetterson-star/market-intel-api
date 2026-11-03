"""Identify bounded public projection reads without changing analytical admission."""
from contextvars import ContextVar
import logging
import os
import resource
import sys


PUBLIC_LOOKUP_REQUEST = ContextVar("mimir_public_lookup_request", default=False)
_MEMORY_CEILING_BYTES = int(os.getenv("DUCK_PUBLIC_MEMORY_CEILING_MB", "6144")) * 1024**2
if _MEMORY_CEILING_BYTES <= 0:
    raise ValueError("DUCK_PUBLIC_MEMORY_CEILING_MB must be positive")
_MEMORY_SUSPENDED = False

# Only these indexed, single-row projections may use the reserved connection.
# Legacy fallbacks, manifest searches and analytical scans retain the two-reader
# pool even when called from a public HTTP route.
_POINT_LOOKUPS = frozenset(" ".join(sql.split()).casefold() for sql in (
    "SELECT * FROM public_award_profile WHERE contract_id = ? LIMIT 1",
    "SELECT * FROM public_solicitation_profile WHERE opportunity_id = ? LIMIT 1",
    "SELECT payload_json FROM public_company_profile WHERE cage = ? LIMIT 1",
    "SELECT payload_json FROM public_platform_profile WHERE slug = ? LIMIT 1",
    "SELECT payload_json FROM public_nsn_profile WHERE entity_id = ? LIMIT 1",
    "SELECT payload_json FROM public_nsn_profile WHERE niin = ? LIMIT 1",
))


def is_reserved_public_lookup(sql):
    return PUBLIC_LOOKUP_REQUEST.get() and " ".join(sql.split()).casefold() in _POINT_LOOKUPS


def public_reader_memory_ok():
    """Latch off extra query concurrency after crossing the process RSS ceiling.

    Linux reports ru_maxrss in KiB; macOS reports bytes. Check before and after
    each reserved query. This is a high-water mark, so the reader stays disabled
    until restart rather than repeatedly switching on and off under pressure.
    The connection remains idle; existing analytical concurrency is unchanged.
    """
    global _MEMORY_SUSPENDED
    if _MEMORY_SUSPENDED:
        return False
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_bytes = int(peak if sys.platform == "darwin" else peak * 1024)
    if peak_bytes >= _MEMORY_CEILING_BYTES:
        _MEMORY_SUSPENDED = True
        logging.getLogger("mimir-api").error(
            "Reserved public reader disabled: peak_process_bytes=%s ceiling_bytes=%s routing=shared",
            peak_bytes, _MEMORY_CEILING_BYTES,
        )
        return False
    return True
