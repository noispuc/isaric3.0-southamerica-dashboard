import logging
import sys
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler

_LOGGING_CONFIGURED = False


def _configure_root_logger() -> None:
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    root_logger = logging.getLogger()
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    log_path = Path(__file__).resolve().parents[2] / "app.log"

    handlers = [
        logging.StreamHandler(sys.stdout),
        TimedRotatingFileHandler(
            log_path,
            when="midnight",
            interval=1,
            backupCount=7,
            encoding="utf-8",
            delay=True,
        ),
    ]

    for handler in handlers:
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)

    root_logger.setLevel(logging.INFO)
    _LOGGING_CONFIGURED = True


def setup_logger(name: str | None = None):
    _configure_root_logger()
    return logging.getLogger(name)