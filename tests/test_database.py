"""Tests del modulo database.py — DatabasePG singleton y API iptv-db."""

from __future__ import annotations

import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from database import DatabasePG


def _mock_engines():
    """Mockea los engines iptv-db para evitar conexiones reales."""
    return patch.multiple(
        "database",
        get_async_engine=MagicMock(return_value=AsyncMock()),
        get_async_session_factory=MagicMock(return_value=MagicMock()),
        get_sync_engine=MagicMock(return_value=MagicMock()),
        get_sync_session_factory=MagicMock(return_value=MagicMock()),
    )


@pytest.fixture(autouse=True)
def _reset_singleton():
    """Resetea el singleton de DatabasePG antes de cada test."""
    DatabasePG.reset()
    yield
    DatabasePG.reset()


class TestDatabasePGIptvDb:
    """Tests de la nueva API iptv-db (get_session_factory, get_sync_session_factory)."""

    @pytest.mark.asyncio
    async def test_get_session_factory_requiere_initialize(self):
        with _mock_engines(), pytest.raises(RuntimeError, match="no inicializado"):
            DatabasePG.get_session_factory()

    @pytest.mark.asyncio
    async def test_get_session_factory_retorna_factory(self):
        mock_factory = MagicMock()
        with (
            _mock_engines(),
            patch("database.get_async_session_factory", return_value=mock_factory),
        ):
            await DatabasePG.initialize()
            factory = DatabasePG.get_session_factory()
            assert factory is mock_factory

    @pytest.mark.asyncio
    async def test_get_sync_session_factory_requiere_initialize(self):
        with _mock_engines(), pytest.raises(RuntimeError, match="no inicializado"):
            DatabasePG.get_sync_session_factory()

    @pytest.mark.asyncio
    async def test_get_sync_session_factory_retorna_factory(self):
        mock_factory = MagicMock()
        with (
            _mock_engines(),
            patch("database.get_sync_session_factory", return_value=mock_factory),
        ):
            await DatabasePG.initialize()
            factory = DatabasePG.get_sync_session_factory()
            assert factory is mock_factory
