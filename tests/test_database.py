"""Tests del modulo database.py — DatabasePG singleton y API iptv-db."""

from __future__ import annotations

import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from database import DatabasePG


def _mock_engines():
    """Mockea los engines iptv-db y asyncpg para evitar conexiones reales."""
    return patch.multiple(
        "database",
        asyncpg=MagicMock(),
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


class TestDatabasePGLegacy:
    """Tests de la API legacy (initialize, get_pool, close, reset)."""

    def test_reset_limpia_pool(self):
        DatabasePG._pool = AsyncMock()
        DatabasePG.reset()
        assert DatabasePG._pool is None

    @pytest.mark.asyncio
    async def test_initialize_crea_pool(self):
        with (
            _mock_engines(),
            patch("database.asyncpg.create_pool", new_callable=AsyncMock) as mock_create,
        ):
            mock_create.return_value = AsyncMock()
            pool = await DatabasePG.initialize()
            assert pool is not None
            mock_create.assert_called_once()

    @pytest.mark.asyncio
    async def test_initialize_no_duplica_pool(self):
        with (
            _mock_engines(),
            patch("database.asyncpg.create_pool", new_callable=AsyncMock) as mock_create,
        ):
            mock_create.return_value = AsyncMock()
            await DatabasePG.initialize()
            await DatabasePG.initialize()
            mock_create.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_pool_inicializa_si_es_necesario(self):
        with (
            _mock_engines(),
            patch("database.asyncpg.create_pool", new_callable=AsyncMock) as mock_create,
        ):
            mock_create.return_value = AsyncMock()
            pool = await DatabasePG.get_pool()
            assert pool is not None
            mock_create.assert_called_once()

    @pytest.mark.asyncio
    async def test_initialize_falla_sin_env_vars(self):
        for var in ("PG_HOST", "PG_USER", "PG_PASSWORD"):
            os.environ.pop(var, None)
        with pytest.raises(ValueError, match="No se encontraron"):
            await DatabasePG.initialize()

    @pytest.mark.asyncio
    async def test_close_cierra_pool(self):
        with (
            _mock_engines(),
            patch("database.asyncpg.create_pool", new_callable=AsyncMock) as mock_create,
        ):
            mock_pool = AsyncMock()
            mock_create.return_value = mock_pool
            await DatabasePG.initialize()
            await DatabasePG.close()
            mock_pool.close.assert_called_once()
            assert DatabasePG._pool is None


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
            patch("database.asyncpg.create_pool", new_callable=AsyncMock),
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
            patch("database.asyncpg.create_pool", new_callable=AsyncMock),
        ):
            await DatabasePG.initialize()
            factory = DatabasePG.get_sync_session_factory()
            assert factory is mock_factory
