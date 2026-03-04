"""
src/shared/config.py

Typed configuration loaded from environment variables / .env file.

Environment variables:
    APP_ENV   — development | production | test  (default: development)
    LOG_LEVEL — debug | info | warn | error      (default: info)
    DB_PATH   — path to the DuckDB file          (default: data/quant.db)
"""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

_VALID_ENVS = {"development", "production", "test"}
_VALID_LOG_LEVELS = {"debug", "info", "warn", "error"}


@dataclass(frozen=True)
class Config:
    env: str
    log_level: str
    db_path: str


def _load() -> Config:
    env = os.environ.get("APP_ENV", "development")
    if env not in _VALID_ENVS:
        raise ValueError(f"APP_ENV must be one of {_VALID_ENVS}, got '{env}'")

    log_level = os.environ.get("LOG_LEVEL", "info").lower()
    if log_level not in _VALID_LOG_LEVELS:
        raise ValueError(f"LOG_LEVEL must be one of {_VALID_LOG_LEVELS}, got '{log_level}'")

    db_path = os.environ.get("DB_PATH", "data/quant.db")

    return Config(env=env, log_level=log_level, db_path=db_path)


config = _load()
