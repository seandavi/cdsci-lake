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

import os
import sys

from loguru import logger

_configured = False

_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> "
    "<level>{level: <8}</level> "
    "<cyan>{extra[ctx]}</cyan> "
    "<level>{message}</level>"
)


def configure(level: str | None = None, *, force: bool = False) -> None:
    """Install a single stderr sink (idempotent). CLIs call this once at startup.

    ``level`` defaults to ``$CDSCI_LOG_LEVEL`` then ``INFO``. Re-calls are no-ops
    unless ``force`` — so a CLI invoking another CLI's helper can't double-sink.
    """
    global _configured
    if _configured and not force:
        return
    logger.remove()
    logger.configure(extra={"ctx": "-"})
    logger.add(
        sys.stderr,
        level=(level or os.environ.get("CDSCI_LOG_LEVEL", "INFO")).upper(),
        format=_FORMAT,
        backtrace=False,
        diagnose=False,
    )
    _configured = True


__all__ = ["configure", "logger"]
