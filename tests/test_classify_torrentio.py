"""Tests de clasificacion Torrentio del catalogo."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from iptv_scrapper.classify_torrentio import (
    Classification,
    classify_streams,
    detect_languages,
)


class TestDetectLanguages:
    def test_flag_espanol(self):
        assert detect_languages("Pelicula 1080p 🇪🇸 ⚙️ Wolfmax4k") == ["ES"]

    def test_flag_ingles(self):
        assert detect_languages("Movie 1080p 🇬🇧 ⚙️") == ["EN"]

    def test_flag_japones(self):
        assert detect_languages("Anime 🇯🇵 👤 4") == ["JP"]

    def test_palabra_spanish(self):
        assert detect_languages("Pelicula Spanish HDR") == ["ES"]

    def test_palabra_english(self):
        assert detect_languages("Movie English 4K") == ["EN"]

    def test_castellano_en_corchetes_con_flag(self):
        title = "Pelicula [Castellano] 👤 12 💾 1.5 GB ⚙️ Wolfmax4k\n🇪🇸"
        assert detect_languages(title) == ["ES"]

    def test_codigo_en_corchetes(self):
        assert detect_languages("Pelicula [ES] 1080p") == ["ES"]
        assert detect_languages("Pelicula [EN] 1080p") == ["EN"]
        assert detect_languages("Anime [JP] 1080p") == ["JP"]

    def test_latino_descartado(self):
        assert detect_languages("Pelicula 🇲🇽 latino ⚙️") is None

    def test_idioma_extranjero_sin_codigo_descartado(self):
        assert detect_languages("Película 🇮🇹 1080p") is None

    def test_sin_marcador_por_defecto_ingles(self):
        assert detect_languages("Pelicula 1080p WEB-DL") == ["EN"]


class TestClassifyStreams:
    def test_mezcla_idiomas_dedup(self):
        streams = [
            {"infoHash": "a" * 40, "title": "Pelicula 1080p 🇪🇸 ⚙️"},
            {"infoHash": "b" * 40, "title": "Movie 1080p 🇬🇧 ⚙️"},
            {"infoHash": "c" * 40, "title": "Pelicula 720p 🇪🇸 ⚙️"},
        ]
        result = classify_streams(streams)
        assert result == Classification(has_torrent=True, languages=["EN", "ES"])

    def test_sin_torrent_valido(self):
        streams = [
            {"infoHash": "zz", "title": "invalido"},
            {"infoHash": "d" * 40, "title": "Pelicula 🇲🇽 latino ⚙️"},
        ]
        result = classify_streams(streams)
        assert result == Classification(has_torrent=False, languages=[])

    def test_streams_vacios(self):
        assert classify_streams([]) == Classification(has_torrent=False, languages=[])
