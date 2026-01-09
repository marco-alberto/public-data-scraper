from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class JobPostingV1(BaseModel):
    """
    Canonical job posting schema (v1).

    Design goals:
    - Client-friendly, stable output fields
    - Works for CSV/SQLite
    - Keeps both raw and normalized fields where it matters
    """

    model_config = ConfigDict(extra="ignore")  # ignore unexpected fields safely

    # Identity / lineage
    source: str = Field(..., examples=["ibm_careers"])
    external_id: str = Field(..., description="Stable identifier from the source system")

    # Core business fields
    title: str
    company: str = Field(default="IBM")
    job_url: HttpUrl

    # Dates / metadata
    posted_date: Optional[date] = None
    scraped_at: datetime = Field(default_factory=utc_now)

    # Location (raw + normalized where possible)
    location_raw: Optional[str] = None
    location_country: Optional[str] = None
    location_city: Optional[str] = None

    # Optional enrichment
    employment_type: Optional[str] = None
    description: Optional[str] = None

    @classmethod
    def dedupe_key(cls, source: str, external_id: str) -> str:
        """Natural key used for deduplication."""
        return f"{source}::{external_id}"
