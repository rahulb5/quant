"""
src/shared/utils.py

Shared logger. Mirrors the winston setup:
  - Console output with timestamp + level
  - logs/error.log  (ERROR and above)
  - logs/combined.log (all levels)
"""

import logging
import os
from pathlib import Path

Path("logs").mkdir(exist_ok=True)

_LEVEL_MAP = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warn": logging.WARNING,
    "error": logging.ERROR,
}

_log_level_str = os.environ.get("LOG_LEVEL", "info").lower()
_log_level = _LEVEL_MAP.get(_log_level_str, logging.INFO)

_formatter = logging.Formatter(
    fmt="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger("quant")
logger.setLevel(_log_level)

# Console
_console = logging.StreamHandler()
_console.setFormatter(_formatter)
logger.addHandler(_console)

# Error file
_error_file = logging.FileHandler("logs/error.log")
_error_file.setLevel(logging.ERROR)
_error_file.setFormatter(_formatter)
logger.addHandler(_error_file)

# Combined file
_combined_file = logging.FileHandler("logs/combined.log")
_combined_file.setFormatter(_formatter)
logger.addHandler(_combined_file)
