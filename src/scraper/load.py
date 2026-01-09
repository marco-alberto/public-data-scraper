from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

import csv

from .models import JobPostingV1


@dataclass(frozen=True)
class LoadResult:
    out_path: Path
    rows_written: int


CSV_COLUMNS: List[str] = [
    "source",
    "external_id",
    "title",
    "company",
    "job_url",
    "posted_date",
    "scraped_at",
    "location_raw",
    "location_country",
    "location_city",
    "employment_type",
    "description",
]


def _to_row(item: JobPostingV1) -> dict[str, str]:
    # Convert to CSV-friendly strings
    d = item.model_dump()
    row: dict[str, str] = {}

    for col in CSV_COLUMNS:
        val = d.get(col)

        if val is None:
            row[col] = ""
        elif col in ("posted_date", "scraped_at"):
            # date/datetime -> ISO 8601
            row[col] = val.isoformat()
        else:
            row[col] = str(val)

    return row


def write_job_postings_csv(
    *,
    items: Iterable[JobPostingV1],
    processed_root: Path,
    source: str,
    run_date: str,
    filename: str = "job_postings_v1.csv",
) -> LoadResult:
    """
    Write processed CSV for a given run_date.
    Idempotent: overwrites output file for that run_date.
    """
    out_dir = processed_root / source / f"run_date={run_date}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / filename

    count = 0
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for item in items:
            writer.writerow(_to_row(item))
            count += 1

    return LoadResult(out_path=out_path, rows_written=count)
