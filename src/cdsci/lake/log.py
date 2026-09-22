"""Loguru logging for the lake substrate and its ingestors.

One import — ``from .log import logger`` — and a module logs. The CLI entry
points call :func:`configure` once to install a single stderr sink at a sane
level (``CDSCI_LOG_LEVEL`` or ``INFO``); **library code never configures at
import**, so a consumer that only `pip install`s the read client and calls
:func:`cdsci.lake.lake_connect` stays quiet unless it opts in.

Long bulk loads (PMC especially) are the reason this exists: the per-range
download → stream → curate progress, the ``ops.run`` lifecycle, and maintenance
expiry/cleanup all log here so a multi-hour run leaves a legible trail and a
failure points at the exact range/step instead of a bare traceback.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC
from typing import TYPE_CHECKING, Any

from loguru import logger

from .publish.release import _UNSAFE_PATTERN

if TYPE_CHECKING:
    from datetime import datetime

_configured = False

_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> "
    "<level>{level: <8}</level> "
    "<cyan>{extra[ctx]}</cyan> "
    "<level>{message}</level>"
)

# design §8.2's required JSON event fields sourced from bound `extra` -- included
# only when actually bound, so an event that never set e.g. `asset` doesn't emit
# a null placeholder.
_JSON_EXTRA_FIELDS = (
    "event", "run_id", "writer", "job", "asset", "release", "rows", "status", "duration_ms",
)


def _redact(value: Any) -> Any:
    """Replace a secret-shaped string (design §8.2: logs never contain credentials)
    with a placeholder rather than raising -- a log line is best-effort, not a
    publish-time contract that should crash a run over a field it didn't expect.
    """
    if isinstance(value, str) and _UNSAFE_PATTERN.search(value):
        return "<redacted>"
    return value


def _iso_ts(t: datetime) -> str:
    t = t.astimezone(UTC)
    return t.strftime("%Y-%m-%dT%H:%M:%S.") + f"{t.microsecond // 1000:03d}Z"


def _json_format(record: dict[str, Any]) -> str:
    """loguru dynamic formatter: one JSON object per line (design §8.2).

    Even a dynamic (callable) formatter's return value is run through loguru's
    own color-tag parser and then ``str.format_map(record)`` -- so literal
    ``{``/``}`` from ``json.dumps`` must be doubled (else read as a field
    placeholder) and literal ``<`` must be backslash-escaped (else read as an
    unknown color tag, e.g. our own ``"<redacted>"``), or the sink raises.
    """
    payload: dict[str, Any] = {
        "timestamp": _iso_ts(record["time"]),
        "level": record["level"].name.lower(),
    }
    extra = record["extra"]
    for key in _JSON_EXTRA_FIELDS:
        if key in extra and extra[key] is not None:
            payload[key] = extra[key]
    payload["message"] = record["message"]
    payload = {k: _redact(v) for k, v in payload.items()}
    text = json.dumps(payload, default=str).replace("{", "{{").replace("}", "}}")
    return text.replace("<", "\\<") + "\n"


def configure(level: str | None = None, *, json: bool = False, force: bool = False) -> None:
    """Install a single stderr sink (idempotent). CLIs call this once at startup.

    ``level`` defaults to ``$CDSCI_LOG_LEVEL`` then ``INFO``. Re-calls are no-ops
    unless ``force`` — so a CLI invoking another CLI's helper can't double-sink.
    ``json`` (or ``$CDSCI_LOG_JSON=1``) switches the sink to one structured JSON
    object per line (design §8.2) instead of the human-readable format.
    """
    global _configured
    if _configured and not force:
        return
    use_json = json or os.environ.get("CDSCI_LOG_JSON", "").strip().lower() in ("1", "true", "yes")
    logger.remove()
    logger.configure(extra={"ctx": "-"})
    logger.add(
        sys.stderr,
        level=(level or os.environ.get("CDSCI_LOG_LEVEL", "INFO")).upper(),
        format=_json_format if use_json else _FORMAT,
        backtrace=False,
        diagnose=False,
    )
    _configured = True


def event(name: str, **fields: Any) -> None:
    """Emit one structured event (design §8.2) -- silent until a CLI called
    :func:`configure`, same as any other loguru call (ADR-0009).

        log.event("run_started", run_id=rid, writer=writer, job=source, status="running")
    """
    logger.bind(event=name, **fields).info(name)


__all__ = ["configure", "event", "logger"]
