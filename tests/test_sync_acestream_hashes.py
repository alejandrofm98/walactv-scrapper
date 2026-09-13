"""Tests de sync_acestream_hashes: normalizacion y fallback de gateways sin red."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from iptv_scrapper.sync_acestream_hashes import (
    fetch_payload,
    normalize_entry,
    normalize_payload,
    write_output,
)

SAMPLE_PAYLOAD = {
    "generated": "2026-09-13T18:00:04Z",
    "count": 6,
    "hashes": [
        {
            "title": "DAZN Baloncesto 720p *",
            "hash": "4005AABE21BA9C6D748845DB91E4F48D99F58639",
            "group": "baloncesto",
            "logo": "https://example.com/logo.png",
            "tvg_id": "",
        },
        # Duplicado del anterior (mismo hash, distinto titulo): se descarta.
        {
            "title": "DAZN Baloncesto (duplicado)",
            "hash": "4005aabe21ba9c6d748845db91e4f48d99f58639",
            "group": "BALONCESTO",
            "logo": None,
            "tvg_id": "",
        },
        # Dato sucio real de la lista: logo numerico en vez de URL.
        {
            "title": "Canal sucio",
            "hash": "9dd9cd78f0b08097a0ad290606dbbee999709fa5",
            "group": "GENERAL",
            "logo": 28296,
            "tvg_id": "",
        },
        # Hash invalido: se descarta.
        {"title": "Roto", "hash": "no-es-un-hash", "group": "X", "logo": "", "tvg_id": ""},
        # Sin hash: se descarta.
        {"title": "Sin hash", "group": "X"},
        # Titulo vacio: fallback con prefijo del hash.
        {"title": "", "hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "group": ""},
    ],
}


def test_normalize_entry_valid_and_dirty() -> None:
    entry = normalize_entry(SAMPLE_PAYLOAD["hashes"][0])
    assert entry is not None
    # Hash a minusculas.
    assert entry["hash"] == "4005aabe21ba9c6d748845db91e4f48d99f58639"
    # Grupo a mayusculas.
    assert entry["group"] == "BALONCESTO"
    assert entry["logo"] == "https://example.com/logo.png"

    # Logo numerico (dato sucio real) se descarta a cadena vacia.
    dirty = normalize_entry(SAMPLE_PAYLOAD["hashes"][2])
    assert dirty is not None
    assert dirty["logo"] == ""


def test_normalize_entry_rejects_invalid() -> None:
    assert normalize_entry(SAMPLE_PAYLOAD["hashes"][3]) is None  # hash invalido
    assert normalize_entry(SAMPLE_PAYLOAD["hashes"][4]) is None  # sin hash


def test_normalize_entry_fallback_title() -> None:
    entry = normalize_entry(SAMPLE_PAYLOAD["hashes"][5])
    assert entry is not None
    assert entry["title"] == "Acestream aaaaaaaa"


def test_normalize_payload_dedupes_and_sorts() -> None:
    channels = normalize_payload(SAMPLE_PAYLOAD)
    hashes = [c["hash"] for c in channels]
    assert len(hashes) == len(set(hashes))
    # Orden estable por (group, title, hash).
    assert channels == sorted(channels, key=lambda c: (c["group"], c["title"], c["hash"]))
    # 6 entradas - 1 duplicado - 2 invalidas = 3 canales.
    assert len(channels) == 3


def test_write_output_shape(tmp_path: Path) -> None:
    channels = normalize_payload(SAMPLE_PAYLOAD)
    out = tmp_path / "acestream_channels.json"
    write_output(channels, "https://gateway.test/hashes.json", out, "2026-09-13T18:00:04Z")

    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["count"] == len(channels)
    assert data["source"] == "https://gateway.test/hashes.json"
    assert data["source_generated"] == "2026-09-13T18:00:04Z"
    assert data["generated"]
    assert len(data["channels"]) == len(channels)


class _FakeResponse:
    def __init__(self, payload: dict[str, Any] | None = None, status: int = 200) -> None:
        self._payload = payload
        self.status_code = status

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> dict[str, Any]:
        assert self._payload is not None
        return self._payload


def test_fetch_payload_falls_back_on_429(monkeypatch: pytest.MonkeyPatch) -> None:
    """Con 429 en la primera gateway pasa a la siguiente (sin dormir en tests)."""
    monkeypatch.setattr("iptv_scrapper.sync_acestream_hashes.RETRY_BACKOFF_SECONDS", [])
    monkeypatch.setattr(
        "iptv_scrapper.sync_acestream_hashes._gateway_urls",
        lambda: ["https://lim.test/hashes.json", "https://ok.test/hashes.json"],
    )
    good = {"generated": "g", "hashes": [{"title": "A", "hash": "a" * 40}]}
    calls: list[str] = []

    def fake_get(url: str, **_: Any) -> _FakeResponse:
        calls.append(url)
        if url.startswith("https://lim.test"):
            return _FakeResponse(status=429)
        return _FakeResponse(good)

    with patch("iptv_scrapper.sync_acestream_hashes.requests.get", side_effect=fake_get):
        url, payload = fetch_payload()

    assert url == "https://ok.test/hashes.json"
    assert payload["hashes"][0]["hash"] == "a" * 40
    assert calls == ["https://lim.test/hashes.json", "https://ok.test/hashes.json"]


def test_fetch_payload_retries_same_gateway(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reintenta la misma gateway con backoff antes de saltar a la siguiente.

    RETRY_BACKOFF_SECONDS=[0, 0] simula los 3 intentos (1 + 2 backoffs)
    sin dormir en tests.
    """
    monkeypatch.setattr("iptv_scrapper.sync_acestream_hashes.RETRY_BACKOFF_SECONDS", [0, 0])
    monkeypatch.setattr(
        "iptv_scrapper.sync_acestream_hashes._gateway_urls",
        lambda: ["https://a.test/hashes.json", "https://b.test/hashes.json"],
    )
    good = {"generated": "g", "hashes": []}
    attempts: dict[str, int] = {"a": 0, "b": 0}

    def fake_get(url: str, **_: Any) -> _FakeResponse:
        key = "a" if "a.test" in url else "b"
        attempts[key] += 1
        if key == "a" and attempts["a"] <= 2:
            return _FakeResponse(status=429)
        return _FakeResponse(good)

    with patch("iptv_scrapper.sync_acestream_hashes.requests.get", side_effect=fake_get):
        url, _ = fetch_payload()

    # 2 fallos (429) y el 3er intento con exito, todo contra la primera gateway.
    assert attempts["a"] == 3
    assert attempts["b"] == 0
    assert url == "https://a.test/hashes.json"


def test_fetch_payload_raises_when_all_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("iptv_scrapper.sync_acestream_hashes.RETRY_BACKOFF_SECONDS", [])
    monkeypatch.setattr(
        "iptv_scrapper.sync_acestream_hashes._gateway_urls",
        lambda: ["https://x.test/hashes.json"],
    )

    with (
        patch(
            "iptv_scrapper.sync_acestream_hashes.requests.get",
            side_effect=lambda *a, **k: _FakeResponse(status=429),
        ),
        pytest.raises(RuntimeError, match="Ninguna gateway"),
    ):
        fetch_payload()
