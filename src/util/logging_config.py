"""
Production-ready logging configuration for Hudi catalog sync tests.
Configurable via environment (LOG_LEVEL, LOG_FORMAT) or programmatic setup.
"""

import logging
import os
import sys
from typing import Optional

# Default format: timestamp, level, logger name, message
DEFAULT_FORMAT = (
    "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Module-level registry of loggers we've configured (optional, for testing)
_configured_root = False


def get_logger(name: str) -> logging.Logger:
    """
    Return a logger for the given name (e.g. __name__ or 'hudi_sync.runner').
    Call setup_logging() once (e.g. in main) to configure level and format.
    """
    return logging.getLogger(name)


def setup_logging(
    level: Optional[str] = None,
    format_string: Optional[str] = None,
    stream: Optional[object] = None,
    force: bool = False,
) -> None:
    """
    Configure root logging for the application.
    Safe to call multiple times; subsequent calls no-op unless force=True.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR). Defaults to env LOG_LEVEL or INFO.
        format_string: Format for log messages. Defaults to env LOG_FORMAT or DEFAULT_FORMAT.
        stream: Where to send logs (default sys.stderr). Any file-like object.
        force: If True, re-apply configuration even if already configured.
    """
    global _configured_root
    if _configured_root and not force:
        return

    level = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    format_string = format_string or os.environ.get("LOG_FORMAT", DEFAULT_FORMAT)
    stream = stream or sys.stderr

    numeric_level = getattr(logging, level, logging.INFO)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO

    handler = logging.StreamHandler(stream)
    handler.setLevel(numeric_level)
    formatter = logging.Formatter(format_string, datefmt=DEFAULT_DATE_FORMAT)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(numeric_level)
    # Avoid duplicate handlers when calling setup multiple times
    for h in root.handlers[:]:
        root.removeHandler(h)
    root.addHandler(handler)
    _configured_root = True
