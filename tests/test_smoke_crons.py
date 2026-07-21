"""Smoke tests: verify cron scripts import and initialize without errors.

These tests catch import-time regressions (e.g., missing imports, broken
shims, circular dependencies) that would crash the Ofelia cron jobs in
production.

Cron scripts are those listed in docker/ofelia-config.ini and
docker/run-sync.sh (which wraps sync_iptv.py + poblar_mapeo_canales.py).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPTS_DIR = Path(__file__).parent.parent / "src" / "iptv_scrapper"

# Ensure iptv_scrapper/ is in sys.path so that bare module imports
# (e.g. "from database import ...") resolve correctly.
sys.path.insert(0, str(SCRIPTS_DIR))

# Cron scripts from docker/ofelia-config.ini (all 6 Ofelia jobs)
# plus poblar_mapeo_canales.py (run via run-sync.sh within iptv-sync job).
CRON_SCRIPTS = [
    "main",  # futbol-daily: python main.py
    "sync_iptv",  # iptv-sync: /app/run-sync.sh
    "sync_replays",  # sync-replays: python iptv_scrapper/sync_replays.py
    "scrape_tmdb_metadata",  # tmdb-metadata-sync: python iptv_scrapper/scrape_tmdb_metadata.py --batch-size 100
    "import_imdb_ratings",  # imdb-ratings: python iptv_scrapper/import_imdb_ratings.py --batch-size 10000
    "populate_episode_imdb_ids",  # imdb-episode-ids: python iptv_scrapper/populate_episode_imdb_ids.py --batch-size 5000
    "poblar_mapeo_canales",  # iptv-sync (via run-sync.sh): python iptv_scrapper/poblar_mapeo_canales.py
]


@pytest.mark.parametrize("script_name", CRON_SCRIPTS)
def test_cron_script_imports(script_name: str) -> None:
    """Verify that the cron script can be imported without errors."""
    script_path = SCRIPTS_DIR / f"{script_name}.py"
    assert script_path.exists(), f"Script not found: {script_path}"

    spec = importlib.util.spec_from_file_location(script_name, script_path)
    assert spec is not None and spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)


@pytest.mark.parametrize("script_name", CRON_SCRIPTS)
def test_cron_script_has_main_entrypoint(script_name: str) -> None:
    """Verify that the cron script has a main() function or __main__ block."""
    script_path = SCRIPTS_DIR / f"{script_name}.py"
    content = script_path.read_text(encoding="utf-8")

    has_main_block = 'if __name__ == "__main__"' in content
    has_main_func = "async def main" in content or "def main" in content

    assert has_main_block or has_main_func, (
        f"{script_name}.py must have either 'if __name__' block or 'main()' function"
    )


def test_main_initializes_databasepg(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify main.py can import DatabasePG with mock env vars."""
    monkeypatch.setenv("PG_HOST", "localhost")
    monkeypatch.setenv("PG_PORT", "5432")
    monkeypatch.setenv("PG_DATABASE", "test")
    monkeypatch.setenv("PG_USER", "test")
    monkeypatch.setenv("PG_PASSWORD", "test")
    monkeypatch.setenv("API_SECRET_KEY", "test-secret")

    spec = importlib.util.spec_from_file_location("main", SCRIPTS_DIR / "main.py")
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Verify that DatabasePG is available
    from database import DatabasePG

    assert hasattr(DatabasePG, "initialize")
    assert hasattr(DatabasePG, "get_session_factory")


def test_sync_iptv_imports_with_iptv_db() -> None:
    """Verify sync_iptv.py imports successfully with iptv-db available."""
    spec = importlib.util.spec_from_file_location("sync_iptv", SCRIPTS_DIR / "sync_iptv.py")
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Verify that key F3c functions are still available
    assert hasattr(module, "sync_to_postgres")  # F3c2a
