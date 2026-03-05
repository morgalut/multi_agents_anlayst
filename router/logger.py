from __future__ import annotations

import logging
import sys


def configure_logging(level: str = "INFO") -> logging.Logger:
    """
    Configure global logging for router services.
    """

    logger = logging.getLogger("multi_agen")
    logger.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(handler)

    return logger


# default logger instance
logger = configure_logging()