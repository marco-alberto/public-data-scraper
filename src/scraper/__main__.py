from __future__ import annotations

import time
from pathlib import Path

import httpx

from .config import load_settings
from .extract import extract_page

# 👉 NUEVO imports (transform + load)
from .transform import transform_latest_run
from .load import write_job_postings_csv


def main() -> int:
    # =========================
    # 1) Load configuration
    # =========================
    s = load_settings()
    s.raw_data_path.mkdir(parents=True, exist_ok=True)
    s.processed_data_path.mkdir(parents=True, exist_ok=True)
    s.log_path.mkdir(parents=True, exist_ok=True)

    # =========================
    # 2) EXTRACT (paginado)
    # =========================
    total_records = 0
    last_total = None

    with httpx.Client() as client:
        for page in range(1, s.max_pages + 1):
            offset = (page - 1) * s.page_size

            result = extract_page(
                client=client,
                retry_attempts=s.retry_attempts,
                raw_dir=Path(s.raw_data_path),
                source="ibm_careers",
                size=s.page_size,
                offset=offset,
                page=page,
                user_agent=s.user_agent,
                timeout_s=s.request_timeout_s,
            )

            last_total = result.total
            total_records += len(result.records)

            print(f"Saved raw: {result.raw_path}")
            print(f"Page {page}: records={len(result.records)} / total={result.total}")

            if len(result.records) == 0:
                break

            time.sleep(1.2)  # polite delay

    print(f"Extract done. Downloaded records={total_records} / total={last_total}")

    # =========================
    # 3) TRANSFORM
    # =========================
    tr = transform_latest_run(
        raw_root=Path(s.raw_data_path),
        source="ibm_careers",
    )

    print(
        f"Transform: in={tr.records_in} out={tr.records_out} "
        f"dupes={tr.duplicates_dropped} invalid={tr.invalid_dropped} "
        f"run_date={tr.run_date}"
    )

    # =========================
    # 4) LOAD
    # =========================
    lr = write_job_postings_csv(
        items=tr.items,
        processed_root=Path(s.processed_data_path),
        source="ibm_careers",
        run_date=tr.run_date,
    )

    print(f"Processed CSV: {lr.out_path} rows={lr.rows_written}")

    # =========================
    # 5) Exit cleanly
    # =========================
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
