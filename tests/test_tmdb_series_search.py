"""Tests del saneado de titulos para busqueda en TMDB (series)."""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

# El modulo exige TMDB_API_KEY al importar; valor dummy para tests de
# funciones puras (no hacen red).
os.environ.setdefault("TMDB_API_KEY", "test-key")

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "iptv_scrapper"))

from iptv_scrapper.scrape_tmdb_metadata import (
    MAX_NOT_FOUND_RETRIES,
    _clamp_year,
    extract_series_search_info,
)


class TestExtractSeriesSearchInfo:
    def test_titulo_normal_con_año(self):
        assert extract_series_search_info("", "Academia de vampiros (2022)") == (
            "academia de vampiros",
            2022,
        )

    def test_sufijo_temporada_español(self):
        assert extract_series_search_info("", "Black Mirror T7") == ("black mirror", None)

    def test_sufijo_temporada_minusculas(self):
        assert extract_series_search_info("", "Dark t2") == ("dark", None)

    def test_año_truncado_cinco_digitos(self):
        title, year = extract_series_search_info("", "Agatha Christies Murder Is Easy (20230")
        assert title == "agatha christies murder is easy 2023"
        assert year is None

    def test_año_con_guion_bajo_colgando(self):
        assert extract_series_search_info("", "Kemono Michi: Rise Up (2019_") == (
            "kemono michi rise up 2019",
            None,
        )

    def test_año_imposible_pasado(self):
        assert extract_series_search_info("", "Foo (1890)") == ("foo", None)

    def test_año_imposible_futuro(self):
        assert extract_series_search_info("", "Foo (2099)") == ("foo", None)

    def test_titulo_sin_año(self):
        assert extract_series_search_info("", "12 Monos") == ("12 monos", None)

    def test_guiones_y_dos_puntos(self):
        assert extract_series_search_info("", "9-1-1: Lone Star") == ("9 1 1 lone star", None)

    def test_titulo_alfanumerico_con_año(self):
        assert extract_series_search_info("", "50m2 (2021)") == ("50m2", 2021)

    def test_fallback_nombre_con_temporada_episodio(self):
        assert extract_series_search_info("Breaking Bad S01 E01", "") == ("breaking bad", None)


class TestClampYear:
    def test_none(self):
        assert _clamp_year(None) is None

    def test_año_valido(self):
        assert _clamp_year(2020) == 2020

    def test_año_actual_mas_uno_valido(self):
        assert _clamp_year(datetime.now().year + 1) == datetime.now().year + 1

    def test_año_antiguo_invalido(self):
        assert _clamp_year(1066) is None

    def test_año_futuro_invalido(self):
        assert _clamp_year(datetime.now().year + 2) is None


class TestBlacklistConstant:
    def test_tope_definido_y_positivo(self):
        assert isinstance(MAX_NOT_FOUND_RETRIES, int)
        assert MAX_NOT_FOUND_RETRIES > 0
