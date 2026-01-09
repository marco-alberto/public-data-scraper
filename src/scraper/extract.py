from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

import random
import time
from http import HTTPStatus


@dataclass(frozen=True)
class ExtractResult:
    raw_path: Path
    records: List[Dict[str, Any]]
    total: Optional[int]


IBM_SEARCH_ENDPOINT = "https://www-api.ibm.com/search/api/v2"


DEFAULT_SOURCE_FIELDS = [
    "_id",
    "title",
    "url",
    "description",
    "language",
    "entitled",
    "field_keyword_17",
    "field_keyword_08",
    "field_keyword_18",
    "field_keyword_19",
]


def build_payload(*, size: int = 30, offset: int = 0) -> Dict[str, Any]:
    """
    Build a minimal payload to retrieve job postings.
    We intentionally omit `aggs` (facets) because our dataset doesn't need them.
    """
    payload: Dict[str, Any] = {
        "appId": "careers",
        "scopes": ["careers2"],
        "query": {"bool": {"must": []}},
        "size": size,
        "sort": [{"_score": "desc"}, {"pageviews": "desc"}],
        "lang": "zz",
        "localeSelector": {},
        "sm": {"query": "", "lang": "zz"},
        "_source": DEFAULT_SOURCE_FIELDS,
    }

    # Many Elasticsearch APIs support `from` for pagination.
    # If IBM's endpoint supports it, this is the simplest paging mechanism.
    if offset > 0:
        payload["from"] = offset

    return payload


def save_raw_json(*, data: Dict[str, Any], raw_dir: Path, source: str, run_date: str, page: int) -> Path:
    out_dir = raw_dir / source / f"run_date={run_date}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"search_page={page:03d}.json"
    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path

def post_with_retries(
    *,
    client: httpx.Client,
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str],
    timeout_s: int,
    attempts: int,
    base_delay_s: float = 0.8,
    max_delay_s: float = 8.0,
) -> httpx.Response:
    """
    POST with retries + exponential backoff + jitter.

    Retries on:
    - network errors / timeouts (httpx.RequestError)
    - 429 Too Many Requests
    - 5xx server errors
    Respects Retry-After header when present.
    """
    last_exc: Exception | None = None

    for i in range(1, attempts + 1):
        try:
            resp = client.post(url, json=payload, headers=headers, timeout=timeout_s)

            # Retry on rate limiting
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    sleep_s = min(float(retry_after), max_delay_s)
                else:
                    sleep_s = min(base_delay_s * (2 ** (i - 1)), max_delay_s)
                    sleep_s += random.uniform(0, 0.4)
                time.sleep(sleep_s)
                continue

            # Retry on transient server errors
            if 500 <= resp.status_code <= 599:
                sleep_s = min(base_delay_s * (2 ** (i - 1)), max_delay_s)
                sleep_s += random.uniform(0, 0.4)
                time.sleep(sleep_s)
                continue

            # Success or non-retriable 4xx
            return resp

        except httpx.RequestError as e:
            last_exc = e
            sleep_s = min(base_delay_s * (2 ** (i - 1)), max_delay_s)
            sleep_s += random.uniform(0, 0.4)
            time.sleep(sleep_s)

    # If we exhausted attempts, raise the last error if we have one.
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("post_with_retries failed without exception (unexpected)")



def extract_page(
    *,
    retry_attempts: int = 3,
    client: httpx.Client,
    raw_dir: Path,
    source: str = "ibm_careers",
    size: int = 30,
    offset: int = 0,
    page: int = 1,
    user_agent: str = "public-data-scraper/1.0",
    timeout_s: int = 30,
) -> ExtractResult:
    """
    Fetch one page of job postings from IBM search endpoint and save the raw JSON.

    Returns:
      - raw_path: where the response was saved
      - records: list of hit objects (each contains _id, _source, etc.)
      - total: total hits reported by the API (if present)
    """
    payload = build_payload(size=size, offset=offset)

    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    resp = post_with_retries(
        client=client,
        url=IBM_SEARCH_ENDPOINT,
        payload=payload,
        headers=headers,
        timeout_s=timeout_s,
        attempts=3,  # we'll wire to settings next
    )
    resp.raise_for_status()
    data: Dict[str, Any] = resp.json()


    # Wrapper is Elasticsearch-like: hits.total.value and hits.hits
    hits = (data.get("hits") or {})
    total_obj = hits.get("total") or {}
    total = total_obj.get("value") if isinstance(total_obj, dict) else None

    records = hits.get("hits") or []
    if not isinstance(records, list):
        raise ValueError("Unexpected response shape: hits.hits is not a list")

    run_date = datetime.utcnow().date().isoformat()
    raw_path = save_raw_json(data=data, raw_dir=raw_dir, source=source, run_date=run_date, page=page)

    return ExtractResult(raw_path=raw_path, records=records, total=total)
