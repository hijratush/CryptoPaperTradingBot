"""
Centralized logging configuration.
- Structured logs to file (JSON-friendly via rich)
- Colored console output
- Separate trade activity log
"""
import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from rich.logging import RichHandler
from rich.console import Console

_configured = False


def setup_logging(log_dir: Path, log_file: str, log_level: str = "INFO") -> None:
    global _configured
    if _configured:
        return
    _configured = True

    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / log_file

    level = getattr(logging, log_level.upper(), logging.INFO)

    # Root logger
    root = logging.getLogger()
    root.setLevel(level)

    # Console handler via rich
    console = Console(stderr=True)
    rich_handler = RichHandler(
        console=console,
        show_path=False,
        rich_tracebacks=True,
        markup=True,
    )
    rich_handler.setLevel(level)

    # File handler (rotating, 10 MB max, 5 backups)
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_fmt)

    root.addHandler(rich_handler)
    root.addHandler(file_handler)

    # Suppress noisy third-party loggers
    for noisy in ("websockets", "aiohttp", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
