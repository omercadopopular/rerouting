"""Logging helpers for the passthrough rebuild pipeline."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import logging

from .config import PipelineConfig


def setup_logging(config: PipelineConfig, logger_name: str = "passthru_data") -> tuple[logging.Logger, Path]:
    """Configure file and console logging."""
    config.logs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    log_path = config.logs_dir / f"pipeline_{timestamp}.log"

    logger = logging.getLogger(logger_name)
    logger.setLevel(getattr(logging, config.log_level.upper(), logging.INFO))
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger, log_path
