"""Tests de normalizacion de sync_iptv."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from iptv_scrapper.sync_iptv import (
    contains_language,
    extraer_año,
    extraer_idioma_desde_grupo,
    extraer_idioma_desde_nombre,
    limpiar_etiquetas_calidad,
    normalizar_idioma,
    quitar_prefijo_idioma,
    split_extinf_line,
)


class TestNormalizarIdioma:
    def test_normaliza_eng(self):
        assert normalizar_idioma("ENG") == "EN"

    def test_normaliza_english(self):
        assert normalizar_idioma("ENGLISH") == "EN"

    def test_normaliza_espanol(self):
        assert normalizar_idioma("ESPANOL") == "ES"

    def test_normaliza_spanish(self):
        assert normalizar_idioma("SPANISH") == "ES"

    def test_normaliza_latam(self):
        assert normalizar_idioma("LATAM") == "LATAM"

    def test_normaliza_latino(self):
        assert normalizar_idioma("LATINO") == "LATAM"

    def test_normaliza_castellano(self):
        assert normalizar_idioma("CASTELLANO") == "CAST"

    def test_normaliza_none(self):
        assert normalizar_idioma(None) is None

    def test_normaliza_vacio(self):
        assert normalizar_idioma("") is None

    def test_normaliza_desconocido(self):
        assert normalizar_idioma("FR") is None


class TestExtraerIdiomaDesdeGrupo:
    def test_grupo_con_pipe(self):
        assert extraer_idioma_desde_grupo("Deportes |ES| La Liga") == "ES"

    def test_grupo_con_prefix(self):
        assert extraer_idioma_desde_grupo("ES - Fútbol") == "ES"

    def test_grupo_latam(self):
        assert extraer_idioma_desde_grupo("LATAM - Fútbol") == "LATAM"

    def test_grupo_sin_idioma(self):
        assert extraer_idioma_desde_grupo("Fútbol General") is None

    def test_grupo_vacio(self):
        assert extraer_idioma_desde_grupo("") is None


class TestExtraerIdiomaDesdeNombre:
    def test_nombre_con_prefix(self):
        assert extraer_idioma_desde_nombre("ES - Real Madrid vs Barça") == "ES"

    def test_nombre_sin_prefix(self):
        assert extraer_idioma_desde_nombre("Real Madrid vs Barça") is None

    def test_nombre_vacio(self):
        assert extraer_idioma_desde_nombre("") is None


class TestLimpiarEtiquetasCalidad:
    def test_elimina_bracket_hd(self):
        assert limpiar_etiquetas_calidad("Canal [HD]") == "Canal"

    def test_elimina_parentesis_uhd(self):
        assert limpiar_etiquetas_calidad("Canal (UHD)") == "Canal"

    def test_elimina_4k_suelto(self):
        assert limpiar_etiquetas_calidad("Canal 4K") == "Canal"

    def test_elimina_fhd(self):
        assert limpiar_etiquetas_calidad("Canal [FHD]") == "Canal"

    def test_mantiene_nombre_original(self):
        assert limpiar_etiquetas_calidad("Canal Normal") == "Canal Normal"

    def test_vacio(self):
        assert limpiar_etiquetas_calidad("") == ""


class TestQuitarPrefijoIdioma:
    def test_quita_prefix_es(self):
        assert quitar_prefijo_idioma("ES - Real Madrid", "ES") == "Real Madrid"

    def test_quita_prefix_latam(self):
        assert quitar_prefijo_idioma("LATAM - Liga", "LATAM") == "Liga"

    def test_sin_prefijo(self):
        assert quitar_prefijo_idioma("Real Madrid", "ES") == "Real Madrid"

    def test_texto_vacio(self):
        assert quitar_prefijo_idioma("", "ES") == ""


class TestSplitExtinfLine:
    def test_linea_normal(self):
        meta, name = split_extinf_line('#EXTINF:-1 group-title="Deportes",Canal Test')
        assert name == "Canal Test"
        assert 'group-title="Deportes"' in meta

    def test_linea_sin_coma(self):
        _meta, name = split_extinf_line('#EXTINF:-1 group-title="Deportes"')
        assert name == ""

    def test_nombre_con_comas(self):
        _meta, name = split_extinf_line("#EXTINF:-1,Canal, Test, Extra")
        assert name == "Canal, Test, Extra"


class TestExtraerAño:
    def test_anio_simple(self):
        assert extraer_año("Película (2020)") == 2020

    def test_anio_rango(self):
        assert extraer_año("Serie (2015-2020)") == 2020

    def test_sin_anio(self):
        assert extraer_año("Película") is None

    def test_vacio(self):
        assert extraer_año("") is None


class TestContainsLanguage:
    def test_grupo_con_es(self):
        assert contains_language('#EXTINF:-1 group-title="ES - Fútbol",Canal') is True

    def test_grupo_con_en(self):
        assert contains_language('#EXTINF:-1 group-title="EN - Sports",Canal') is True

    def test_grupo_sin_idioma_valido(self):
        assert contains_language('#EXTINF:-1 group-title="Fútbol General",Canal') is False
