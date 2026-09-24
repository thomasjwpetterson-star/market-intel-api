"""Run-scoped S3 prefixes shared by orchestration and comparison stages."""


def staging_cache_prefix(run_id: str) -> str:
    return f"mimir/staging/{run_id}/app_cache/"


def manual_cache_prefix(run_id: str) -> str:
    return f"mimir/comparisons/{run_id}/manual/app_cache/"

