from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    return default if val in (None, "") else int(val)


def _env_str(name: str, default: str) -> str:
    val = os.getenv(name)
    return default if val in (None, "") else val


@dataclass(frozen=True)
class Settings:
    env: str
    raw_data_path: Path
    processed_data_path: Path
    log_path: Path

    request_timeout_s: int
    retry_attempts: int
    user_agent: str

    page_size: int
    max_pages: int


def load_settings() -> Settings:
    return Settings(
        env=_env_str("ENV", "development"),
        raw_data_path=Path(_env_str("RAW_DATA_PATH", "data/raw")),
        processed_data_path=Path(_env_str("PROCESSED_DATA_PATH", "data/processed")),
        log_path=Path(_env_str("LOG_PATH", "logs")),
        request_timeout_s=_env_int("REQUEST_TIMEOUT", 30),
        retry_attempts=_env_int("RETRY_ATTEMPTS", 3),
        user_agent=_env_str("USER_AGENT", "public-data-scraper/1.0"),
        page_size=_env_int("PAGE_SIZE", 30),
        max_pages=_env_int("MAX_PAGES", 3),
    )
