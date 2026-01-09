from __future__ import annotations

from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

from .models import JobPostingV1


def extract_job_id(job_url: str) -> Optional[str]:
    """
    IBM Avature job URLs often include a stable jobId query param:
    https://.../JobDetail?jobId=80194
    """
    try:
        qs = parse_qs(urlparse(job_url).query)
        job_ids = qs.get("jobId")
        if job_ids and job_ids[0]:
            return str(job_ids[0])
    except Exception:
        return None
    return None


def parse_location(location_raw: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Very light normalization:
    - "Hyderabad, IN" -> ("Hyderabad", "IN")
    If format doesn't match, keep raw only.
    """
    if not location_raw:
        return None, None

    parts = [p.strip() for p in location_raw.split(",")]
    if len(parts) >= 2 and parts[0] and parts[1]:
        return parts[0], parts[1]

    return None, None


def jobposting_from_api_record(record: dict[str, Any], *, source: str = "ibm_careers") -> JobPostingV1:
    """
    Map one API record (raw) into our canonical schema (v1).

    Expected record shape (example):
    {
      "_id": "...",
      "_source": {
        "url": "...JobDetail?jobId=80194",
        "title": "...",
        "description": "...",
        "field_keyword_19": "Hyderabad, IN",
        ...
      }
    }
    """
    src = record.get("_source") or {}

    job_url = src.get("url")
    if not job_url:
        raise ValueError("Missing _source.url in API record")

    external_id = extract_job_id(job_url) or record.get("_id")
    if not external_id:
        raise ValueError("Missing external id (jobId or _id) in API record")

    title = (src.get("title") or "").strip()
    if not title:
        raise ValueError("Missing _source.title in API record")

    location_raw = src.get("field_keyword_19")
    city, country = parse_location(location_raw)

    return JobPostingV1(
        source=source,
        external_id=str(external_id),
        title=title,
        job_url=job_url,
        description=src.get("description"),
        location_raw=location_raw,
        location_city=city,
        location_country=country,
        posted_date=None,
        employment_type=None,
        # company defaults to "IBM" from the model
    )
