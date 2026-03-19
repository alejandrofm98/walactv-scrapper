import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from sync_iptv import crear_template_m3u, procesar_item  # noqa: E402
import utils.constants as CONSTANTS  # noqa: E402
from config import get_settings  # noqa: E402


BASE_PUBLIC_URL = get_settings().public_domain.rstrip('/')


def test_crear_template_m3u_no_inyecta_metadatos_walac() -> None:
    playlist = """#EXTM3U
#EXTINF:-1 tvg-name="ES - Canal Demo" group-title="ES - Noticias",ES - Canal Demo
http://provider.test/live/user/pass/100.ts
"""

    templates = crear_template_m3u(playlist, "http://provider.test")

    assert "walac-language" not in templates["full"]
    assert "walac-name-normalized" not in templates["live"]
    assert "walac-group-normalized" not in templates["live"]


def test_procesar_item_guarda_normalizados_solo_quitando_idioma_en_channel() -> None:
    item = {
        "name": "ES - Canal Demo HD",
        "group": "ES - Noticias",
        "logo": "",
        "url": "http://provider.test/live/user/pass/100.ts",
        "tvg_id": "demo.channel",
    }

    processed = procesar_item(item, 1, CONSTANTS.CONTENT_TYPE_CHANNEL, provider_username="admin", provider_password="secret")

    assert processed["nombre"] == "ES - Canal Demo HD"
    assert processed["nombre_normalizado"] == "Canal Demo HD"
    assert processed["grupo"] == "ES - Noticias"
    assert processed["grupo_normalizado"] == "Noticias"
    assert processed["stream_url"] == f"{BASE_PUBLIC_URL}/live/admin/secret/100"


def test_procesar_item_guarda_normalizados_solo_quitando_idioma_en_movie() -> None:
    item = {
        "name": "LAT - Mi Pelicula 4K",
        "group": "LAT - Estrenos",
        "logo": "",
        "url": "http://provider.test/movie/user/pass/200.mp4",
        "tvg_id": "",
    }

    processed = procesar_item(item, 2, CONSTANTS.CONTENT_TYPE_MOVIE, provider_username="admin", provider_password="secret")

    assert processed["nombre_normalizado"] == "Mi Pelicula 4K"
    assert processed["grupo_normalizado"] == "Estrenos"
    assert processed["stream_url"] == f"{BASE_PUBLIC_URL}/movie/admin/secret/200.mp4"


def test_procesar_item_guarda_stream_url_para_series() -> None:
    item = {
        "name": "ES - Serie Demo S01 E01",
        "group": "ES - Series",
        "logo": "",
        "url": "http://provider.test/series/user/pass/300.mkv",
        "tvg_id": "",
    }

    processed = procesar_item(item, 3, CONSTANTS.CONTENT_TYPE_SERIE, provider_username="admin", provider_password="secret")

    assert processed["stream_url"] == f"{BASE_PUBLIC_URL}/series/admin/secret/300.mkv"
