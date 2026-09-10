"""Tests de services.logo_mirror (Opción C: espejo propio de logos)."""

import hashlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "iptv_scrapper"))

from services import logo_mirror  # noqa: E402


@pytest.fixture(autouse=True)
def _env(tmp_path, monkeypatch):
    monkeypatch.setattr(logo_mirror, "IMAGES_DIR", tmp_path)
    logo_mirror.reset_run_state()


def test_decode_nested_doble_proxy() -> None:
    anidado = (
        "http://iptv.walerike.com/logo?url="
        "http%3A%2F%2Fiptv.walerike.com%2Flogo%3Furl%3Dhttp%253A%252F%252F51.158.145.100%252Fpicons%252Fa.png"
        "%26type%3Dchannel&type=channel"
    )
    assert logo_mirror.decode_nested(anidado) == "http://51.158.145.100/picons/a.png"


def test_decode_nested_sin_anidar_devuelve_igual() -> None:
    url = "http://picons.cmshulk.com/picons/926464.png"
    assert logo_mirror.decode_nested(url) == url


def test_target_path_estable_y_extension() -> None:
    url = "http://h/a.png"
    esperado = hashlib.sha1(url.encode()).hexdigest() + ".png"
    assert logo_mirror.target_path(url, "channel").name == esperado
    assert logo_mirror.target_path("http://h/b", "channel").name.endswith(".png")


def test_served_url_for() -> None:
    url = "http://h/a.png"
    esperado = logo_mirror.target_path(url, "channel").name
    assert logo_mirror.served_url_for(url, "channel", "https://iptv.test") == (
        f"https://iptv.test/images/logos/channels/{esperado}"
    )


def test_resolver_sin_logo_da_placeholder() -> None:
    assert logo_mirror.resolver_logo("", "channel", "https://iptv.test") == (
        "https://iptv.test/placeholder/channel.png"
    )


def test_resolver_https_pasa_directo() -> None:
    url = "https://images.example.com/logo.png"
    assert logo_mirror.resolver_logo(url, "channel", "https://iptv.test") == url


def test_resolver_https_serie_tmdb_promueve_tamano() -> None:
    url = "https://image.tmdb.org/t/p/w185/poster.jpg"
    assert logo_mirror.resolver_logo(url, "series", "https://iptv.test") == (
        "https://image.tmdb.org/t/p/w600_and_h900_bestv2/poster.jpg"
    )


def test_resolver_http_sin_descarga_da_placeholder() -> None:
    url = "http://caido.example/a.png"
    assert logo_mirror.resolver_logo(url, "channel", "https://iptv.test") == (
        "https://iptv.test/placeholder/channel.png"
    )


def test_resolver_http_con_archivo_existente_da_url_propia(tmp_path: Path) -> None:
    url = "http://vivo.example/a.png"
    dest = logo_mirror.target_path(url, "channel")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(b"x")
    url_esperada = logo_mirror.served_url_for(url, "channel", "https://iptv.test")
    assert logo_mirror.resolver_logo(url, "channel", "https://iptv.test") == url_esperada


def test_circuit_breaker_tras_limite_de_fallos(monkeypatch: pytest.MonkeyPatch) -> None:
    llamadas: list[str] = []

    def fallo_red(url: str, dest: Path) -> tuple[bool, bool]:
        llamadas.append(url)
        return False, False  # timeout/conexion: cuenta para el breaker

    monkeypatch.setattr(logo_mirror, "_download", fallo_red)
    monkeypatch.setattr(logo_mirror, "MAX_WORKERS", 1)
    urls = [f"http://muerto.example/{i}.png" for i in range(10)]
    logo_mirror.premirror(urls, "channel")
    # HOST_FAIL_LIMIT = 5: tras 5 fallos de red seguidos el resto no intenta red
    assert len(llamadas) == logo_mirror.HOST_FAIL_LIMIT
    for url in urls:
        assert logo_mirror.resolver_logo(url, "channel", "https://iptv.test") == (
            "https://iptv.test/placeholder/channel.png"
        )


def test_404_no_dispara_el_breaker(monkeypatch: pytest.MonkeyPatch) -> None:
    llamadas: list[str] = []

    def not_found(url: str, dest: Path) -> tuple[bool, bool]:
        llamadas.append(url)
        return False, True  # host vivo, logo inexistente

    monkeypatch.setattr(logo_mirror, "_download", not_found)
    monkeypatch.setattr(logo_mirror, "MAX_WORKERS", 1)
    urls = [f"http://vivo.example/{i}.png" for i in range(10)]
    logo_mirror.premirror(urls, "channel")
    assert len(llamadas) == len(urls)
    assert not logo_mirror._dead_hosts


def test_parsear_paises_config() -> None:
    # Sin valor: default de las apps
    assert logo_mirror.parsear_paises_config(None) == ["ES", "UK", "US", "WO"]
    # Con valor: limpia espacios y mayusculas
    assert logo_mirror.parsear_paises_config(" es, uk ,WO") == ["ES", "UK", "WO"]
    # Vacio explicito: todos los paises
    assert logo_mirror.parsear_paises_config("") == []
    assert logo_mirror.parsear_paises_config("  ") == []


def test_premirror_descarga_y_resuelve(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def ok(url: str, dest: Path) -> tuple[bool, bool]:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"fake")
        return True, True

    monkeypatch.setattr(logo_mirror, "_download", ok)
    url = "http://vivo.example/logo.png"
    logo_mirror.premirror([url], "channel")
    assert logo_mirror.resolver_logo(url, "channel", "https://iptv.test") == (
        logo_mirror.served_url_for(url, "channel", "https://iptv.test")
    )
    # segunda pasada: existe en disco, no reintenta red
    llamadas: list[str] = []

    def no_llamar(url: str, dest: Path) -> tuple[bool, bool]:
        llamadas.append(url)
        return True, True

    monkeypatch.setattr(logo_mirror, "_download", no_llamar)
    logo_mirror.reset_run_state()
    logo_mirror.premirror([url], "channel")
    assert llamadas == []


def test_buscar_override_por_tvg_id_y_provider_id() -> None:
    base = logo_mirror.IMAGES_DIR / "logos" / "overrides"
    base.mkdir(parents=True, exist_ok=True)
    (base / "skywitnessuk.png").write_bytes(b"fake")
    assert logo_mirror.buscar_override(
        ["SkyWitness.uk", "12345", None], "https://iptv.test"
    ) == "https://iptv.test/images/logos/overrides/skywitnessuk.png"
    assert logo_mirror.buscar_override(
        ["Otro", "999", None], "https://iptv.test"
    ) is None
    assert logo_mirror.buscar_override([None, "", "  "], "https://iptv.test") is None
