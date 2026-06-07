"""Tests del modulo database.py — DatabasePG singleton y configuracion."""

from __future__ import annotations

import os
import sys
from unittest.mock import AsyncMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from database import DatabasePG


@pytest.fixture(autouse=True)
def _reset_singleton():
    """Resetea el singleton de DatabasePG antes de cada test."""
    DatabasePG.reset()
    yield
    DatabasePG.reset()


class TestDatabasePGSingleton:
    def test_reset_limpia_pool(self):
        DatabasePG._pool = AsyncMock()
        DatabasePG.reset()
        assert DatabasePG._pool is None

    @pytest.mark.asyncio
    async def test_initialize_crea_pool(self):
        with patch("database.asyncpg.create_pool", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = AsyncMock()
            pool = await DatabasePG.initialize()
            assert pool is not None
            mock_create.assert_called_once()

    @pytest.mark.asyncio
    async def test_initialize_no_duplica_pool(self):
        with patch("database.asyncpg.create_pool", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = AsyncMock()
            await DatabasePG.initialize()
            await DatabasePG.initialize()
            mock_create.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_pool_inicializa_si_es_necesario(self):
        with patch("database.asyncpg.create_pool", new_callable=AsyncMock) as mock_create:
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
        with patch("database.asyncpg.create_pool", new_callable=AsyncMock) as mock_create:
            mock_pool = AsyncMock()
            mock_create.return_value = mock_pool
            await DatabasePG.initialize()
            await DatabasePG.close()
            mock_pool.close.assert_called_once()
            assert DatabasePG._pool is None
