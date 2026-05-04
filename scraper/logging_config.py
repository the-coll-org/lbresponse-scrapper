"""Structured JSON logging for the scraper.

Reads LOG_LEVEL (default INFO) and LOG_FORMAT (json|text, default json in
production, text otherwise). Attaches the current scrape_id (set per run) to
every log record via a contextvar.
"""

import contextvars
import json
import logging
import os
import sys
from datetime import UTC, datetime

scrape_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "scrape_id", default=None
)

_RESERVED_RECORD_FIELDS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "message",
    "module",
    "msecs",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
}


class JsonFormatter(logging.Formatter):
    """Render log records as a single line of JSON suitable for log shippers."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "service": "lbresponse-scrapper",
            "env": os.getenv("APP_ENV", "development"),
            "msg": record.getMessage(),
        }

        sid = scrape_id_var.get()
        if sid:
            payload["scrape_id"] = sid

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        for key, value in record.__dict__.items():
            if key in _RESERVED_RECORD_FIELDS or key.startswith("_"):
                continue
            payload[key] = value

        return json.dumps(payload, default=str, ensure_ascii=False)


def setup_logging() -> None:
    """Configure the root logger from LOG_LEVEL / LOG_FORMAT env vars."""

    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    fmt = os.getenv("LOG_FORMAT", "").lower()
    if not fmt:
        fmt = "json" if os.getenv("APP_ENV", "").lower() == "production" else "text"

    handler = logging.StreamHandler(sys.stdout)
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))

    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level)
