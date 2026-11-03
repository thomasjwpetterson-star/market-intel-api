"""Identify bounded public projection reads without changing analytical admission."""
from contextvars import ContextVar
from contextlib import contextmanager
import logging
import os
import resource
import sys
import threading


PUBLIC_LOOKUP_REQUEST = ContextVar("mimir_public_lookup_request", default=False)
_MEMORY_CEILING_BYTES = int(os.getenv("DUCK_PUBLIC_MEMORY_CEILING_MB", "6144")) * 1024**2
if _MEMORY_CEILING_BYTES <= 0:
    raise ValueError("DUCK_PUBLIC_MEMORY_CEILING_MB must be positive")
_MEMORY_SUSPENDED = False
_ADMISSION_CEILING_BYTES = int(os.getenv("PUBLIC_SERVING_MEMORY_CEILING_MB", "5120")) * 1024**2
if _ADMISSION_CEILING_BYTES <= 0:
    raise ValueError("PUBLIC_SERVING_MEMORY_CEILING_MB must be positive")
_ADMISSION_SUSPENDED = False
_PUBLIC_FALLBACK_CONCURRENCY = int(os.getenv("DUCK_PUBLIC_FALLBACK_CONCURRENCY", "2"))
if _PUBLIC_FALLBACK_CONCURRENCY not in (1, 2):
    raise ValueError("DUCK_PUBLIC_FALLBACK_CONCURRENCY must be 1 or 2")
_PUBLIC_ANALYTICAL_SLOT = threading.BoundedSemaphore(_PUBLIC_FALLBACK_CONCURRENCY)


@contextmanager
def public_analytical_slot():
    """Use idle analytical readers without exceeding the existing pool size."""
    if PUBLIC_LOOKUP_REQUEST.get():
        with _PUBLIC_ANALYTICAL_SLOT:
            yield
    else:
        yield


def public_admission_memory_ok():
    """Latch public admission back to one before the reserved-reader ceiling."""
    global _ADMISSION_SUSPENDED
    if _ADMISSION_SUSPENDED:
        return False
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_bytes = int(peak if sys.platform == "darwin" else peak * 1024)
    if peak_bytes >= min(_ADMISSION_CEILING_BYTES, _MEMORY_CEILING_BYTES):
        _ADMISSION_SUSPENDED = True
        logging.getLogger("mimir-api").error(
            "Public serving admission reduced: peak_process_bytes=%s ceiling_bytes=%s limit=1",
            peak_bytes, min(_ADMISSION_CEILING_BYTES, _MEMORY_CEILING_BYTES),
        )
        return False
    return True

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
    """SELECT entity_type, entity_id, canonical_path, display_name,
        richness_score, decision_reasons, last_modified, quality_gate_version,
        sitemap_batch FROM public_intelligence_manifest
        WHERE entity_type = ? AND entity_id = ? LIMIT 1""",
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
