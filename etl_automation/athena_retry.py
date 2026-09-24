"""Classify transient Athena failures that are safe to retry."""

from __future__ import annotations


RETRYABLE_ATHENA_REASON_MARKERS = (
    "HIVE_S3_THROTTLING",
    "INTERNAL_ERROR",
    "GENERIC_INTERNAL_ERROR",
    "TOO_MANY_REQUESTS",
    "STATUS CODE: 503",
)


class AthenaQueryFailure(RuntimeError):
    def __init__(self, state: str, reason: str):
        self.state = state
        self.reason = reason
        super().__init__(f"Query Failed: {state} - {reason}")


def is_retryable_athena_reason(reason: str) -> bool:
    normalized = str(reason or "").upper()
    return any(marker in normalized for marker in RETRYABLE_ATHENA_REASON_MARKERS)
