from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .mappers import jobposting_from_api_record
from .models import JobPostingV1


@dataclass(frozen=True)
class TransformResult:
    run_date: str
    records_in: int
    records_out: int
    duplicates_dropped: int
    invalid_dropped: int
    items: List[JobPostingV1]


def _latest_run_date_dir(raw_root: Path, source: str) -> Path:
    """
    Find latest run_date=YYYY-MM-DD directory under data/raw/<source>/.
    """
    source_dir = raw_root / source
    if not source_dir.exists():
        raise FileNotFoundError(f"No raw directory found: {source_dir}")

    run_dirs = sorted([p for p in source_dir.glob("run_date=*") if p.is_dir()])
    if not run_dirs:
        raise FileNotFoundError(f"No run_date directories found under: {source_dir}")

    return run_dirs[-1]


def _load_hits_from_raw_file(path: Path) -> List[Dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    hits = (data.get("hits") or {}).get("hits") or []
    if not isinstance(hits, list):
        raise ValueError(f"Unexpected response shape in {path}: hits.hits is not a list")
    return hits


def transform_latest_run(
    *,
    raw_root: Path,
    source: str = "ibm_careers",
) -> TransformResult:
    run_dir = _latest_run_date_dir(raw_root, source)
    run_date = run_dir.name.split("run_date=")[-1]

    raw_files = sorted(run_dir.glob("search_page=*.json"))
    if not raw_files:
        raise FileNotFoundError(f"No raw page files found in: {run_dir}")

    seen_keys: set[str] = set()
    items: List[JobPostingV1] = []

    records_in = 0
    duplicates_dropped = 0
    invalid_dropped = 0

    for f in raw_files:
        hits = _load_hits_from_raw_file(f)
        records_in += len(hits)

        for record in hits:
            try:
                jp = jobposting_from_api_record(record, source=source)
            except Exception:
                invalid_dropped += 1
                continue

            key = JobPostingV1.dedupe_key(jp.source, jp.external_id)
            if key in seen_keys:
                duplicates_dropped += 1
                continue

            seen_keys.add(key)
            items.append(jp)

    return TransformResult(
        run_date=run_date,
        records_in=records_in,
        records_out=len(items),
        duplicates_dropped=duplicates_dropped,
        invalid_dropped=invalid_dropped,
        items=items,
    )
