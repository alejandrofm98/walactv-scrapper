"""Fixtures compartidos para tests de walactv-scrapper."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest


@pytest.fixture(autouse=True)
def _setup_test_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Configura variables de entorno minimas para tests."""
    monkeypatch.setenv("PG_HOST", "localhost")
    monkeypatch.setenv("PG_PORT", "5432")
    monkeypatch.setenv("PG_USER", "test_user")
    monkeypatch.setenv("PG_PASSWORD", "test_pass")
    monkeypatch.setenv("PG_DATABASE", "test_db")


@pytest.fixture
def mock_pool() -> AsyncMock:
    """Pool de asyncpg mockeado."""
    pool = AsyncMock()
    pool.acquire = AsyncMock()
    pool.release = AsyncMock()
    return pool


@pytest.fixture
def sample_m3u_line() -> str:
    """Linea de ejemplo de un archivo M3U."""
    return '#EXTINF:-1 tvg-id="beinsports1.es" tvg-name="beIN Sports 1" group-title="Deportes",beIN Sports 1 (ES)\nhttp://example.com/stream.m3u8'


@pytest.fixture
def sample_channel_data() -> dict[str, Any]:
    """Datos de canal de ejemplo."""
    return {
        "id": "test-channel-1",
        "nombre": "beIN Sports 1",
        "grupo": "Deportes",
        "logo": "http://example.com/logo.png",
        "country": "ES",
        "language": "es",
    }


@pytest.fixture
def sample_event_data() -> dict[str, Any]:
    """Datos de evento deportivo de ejemplo."""
    return {
        "id": "event-1",
        "titulo": "Real Madrid vs Barcelona",
        "fecha": "2026-06-01",
        "hora": "21:00",
        "competicion": "La Liga",
        "canales": ["beIN Sports 1", "Sky Sports"],
    }
