"""Resumable HTTP download + small JSON helpers, shared by every ingestor.

Bulk dumps are large (iCite metadata is multi-GB), so downloads are **resumable**
(an interrupted fetch continues via an HTTP ``Range`` request against a ``.part``
file) and retried on transient errors. ``unzip`` extracts the source archives
into the raw layer for DuckDB to scan.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .log import logger

_TIMEOUT = httpx.Timeout(60.0, read=300.0)
_log = logger.bind(ctx="download")


@retry(
    retry=retry_if_exception_type(httpx.HTTPError),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, max=60),
    reraise=True,
)
def get_json(url: str, *, params: dict | None = None) -> dict | list:
    """GET and parse JSON, retrying transient HTTP errors."""
    with httpx.Client(timeout=_TIMEOUT, follow_redirects=True) as client:
        resp = client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()


@retry(
    retry=retry_if_exception_type(httpx.HTTPError),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, max=60),
    reraise=True,
)
def post_json(url: str, body: dict) -> dict | list:
    """POST a JSON body and parse the JSON response, retrying transient errors."""
    with httpx.Client(timeout=_TIMEOUT, follow_redirects=True) as client:
        resp = client.post(url, json=body)
        resp.raise_for_status()
        return resp.json()


def download(
    url: str,
    dest: Path,
    *,
    params: dict | None = None,
    resume: bool = True,
    chunk_size: int = 1 << 20,
) -> Path:
    """Stream ``url`` to ``dest``, resuming a partial ``dest.part`` when possible.

    Returns ``dest`` (renamed from ``.part`` on completion). If ``dest`` already
    exists it is returned untouched — re-running an ingest does not re-download.
    """
    if dest.exists():
        _log.debug("cached, skipping download: {}", dest.name)
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_suffix(dest.suffix + ".part")
    existing = part.stat().st_size if (resume and part.exists()) else 0
    _log.info(
        "downloading {} → {}{}",
        url, dest.name, f" (resuming from {existing / 1e6:.1f} MB)" if existing else "",
    )

    @retry(
        retry=retry_if_exception_type(httpx.HTTPError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, max=60),
        reraise=True,
    )
    def _fetch(start: int) -> None:
        headers = {"Range": f"bytes={start}-"} if start else {}
        mode = "ab" if start else "wb"
        with (
            httpx.Client(timeout=_TIMEOUT, follow_redirects=True) as client,
            client.stream("GET", url, params=params, headers=headers) as resp,
            open(part, mode) as fh,
        ):
            # Server ignored Range (200) or rejected it (416/400/501) → the
            # partial is unusable; restart the download from scratch.
            if start and resp.status_code in (200, 400, 416, 501):
                raise _RangeIgnored
            resp.raise_for_status()
            for chunk in resp.iter_bytes(chunk_size):
                fh.write(chunk)

    try:
        _fetch(existing)
    except _RangeIgnored:
        _log.debug("server ignored Range for {} — restarting from 0", dest.name)
        part.unlink(missing_ok=True)
        _fetch(0)

    part.rename(dest)
    _log.info("downloaded {} ({:.1f} MB)", dest.name, dest.stat().st_size / 1e6)
    return dest


def unzip(archive: Path, dest_dir: Path) -> list[Path]:
    """Extract a ``.zip`` into ``dest_dir``; return the extracted file paths.

    Idempotent: members already present (same size) are not re-extracted.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    out: list[Path] = []
    with zipfile.ZipFile(archive) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            target = dest_dir / Path(info.filename).name
            if not (target.exists() and target.stat().st_size == info.file_size):
                with zf.open(info) as src, open(target, "wb") as dst:
                    while chunk := src.read(1 << 20):
                        dst.write(chunk)
            out.append(target)
    _log.info("extracted {} file(s) from {} → {}", len(out), archive.name, dest_dir)
    return out


class _RangeIgnored(Exception):
    """Server returned 200 to a ranged request — restart the download."""
