from __future__ import annotations

import time
from pathlib import Path

import httpx

from .config import load_settings
from .extract import extract_page


def main() -> int:
    s = load_settings()
    s.raw_data_path.mkdir(parents=True, exist_ok=True)
    s.log_path.mkdir(parents=True, exist_ok=True)

    total_records = 0
    last_total = None

    with httpx.Client() as client:
        for page in range(1, s.max_pages + 1):
            offset = (page - 1) * s.page_size

            result = extract_page(
                client=client,
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

            # Stop early if API returns no results
            if len(result.records) == 0:
                break

            # Polite rate limit between pages (simple, effective)
            time.sleep(1.2)

    print(f"Done. Downloaded records={total_records} / total={last_total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
