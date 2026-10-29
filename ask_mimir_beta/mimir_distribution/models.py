from datetime import datetime
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

class Query(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    kind: Literal["company", "nsn", "platform", "award", "search"]
    identifier: str = Field(min_length=2, max_length=128)
    limit: int = Field(default=5, ge=1, le=10)

    @field_validator("identifier")
    @classmethod
    def safe_identifier(cls, value, info):
        import re
        kind = info.data.get("kind")
        patterns = {"company": r"[A-Za-z0-9]{5}", "nsn": r"(?:[0-9]{9}|[0-9]{13}|[0-9]{4}-[0-9]{2}-[0-9]{3}-[0-9]{4})", "platform": r"[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*", "award": r"[A-Za-z0-9_-]+"}
        if kind in patterns and not re.fullmatch(patterns[kind], value):
            raise ValueError(f"Invalid {kind} identifier")
        if kind in ("company", "award"):
            return value.upper()
        if kind == "nsn":
            return value.replace("-", "")
        return value

class Source(BaseModel):
    url: HttpUrl
    retrieved_at: datetime
    published_at: str | None = None

class Result(BaseModel):
    schema_version: Literal["1.0"] = "1.0"
    request_id: str
    kind: str
    identifier: str
    data: dict
    sources: list[Source]
    warnings: list[str] = Field(default_factory=list)
    next_cursor: str | None = None
    truncated: bool = False

class Opportunity(BaseModel):
    """Contract only; the opportunity workstream owns all values and identifiers."""
    model_config = ConfigDict(extra="forbid")
    external_id: str = Field(min_length=1, max_length=160)
    title: str = Field(min_length=1, max_length=300)
    description: str = Field(max_length=5000)
    buyer: str | None = None
    estimated_value_usd: float | None = Field(default=None, ge=0)
    value_basis: str | None = None
    confidence: float | None = Field(default=None, ge=0, le=1)
    expected_date: datetime | None = None
    updated_at: datetime
    evidence: list[HttpUrl] = Field(min_length=1, max_length=20)
    recommended_actions: list[str] = Field(default_factory=list, max_length=10)
    cages: list[str] = Field(default_factory=list, max_length=20)
    nsns: list[str] = Field(default_factory=list, max_length=20)
    platforms: list[str] = Field(default_factory=list, max_length=20)
    contracts: list[str] = Field(default_factory=list, max_length=20)
    sync_status: Literal["ready"] = "ready"
