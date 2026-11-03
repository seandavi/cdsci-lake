"""Offline test for the shared resumable download (``cdsci.lake.download``)."""

from __future__ import annotations

import httpx

from cdsci.lake import download as dl


def test_download_sends_the_callers_headers(tmp_path, monkeypatch):
    seen = []
    transport = httpx.MockTransport(
        lambda req: seen.append(req.headers) or httpx.Response(200, content=b"x"))
    client = httpx.Client
    monkeypatch.setattr(dl.httpx, "Client", lambda **kw: client(transport=transport, **kw))
    path = dl.download("https://example.org/f", tmp_path / "f", headers={"User-Agent": "ua"})
    assert path.read_bytes() == b"x"
    assert seen[0]["User-Agent"] == "ua" and "Range" not in seen[0]
