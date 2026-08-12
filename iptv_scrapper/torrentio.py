"""Cliente minimo para consultar streams de Torrentio."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import requests

DEFAULT_BASE_URL = "https://torrentio.strem.fun"
IMDB_ID_PATTERN = re.compile(r"^tt\d+$", re.IGNORECASE)
DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "WalacTV-Catalog-Probe/0.1",
}


@dataclass(frozen=True)
class TorrentioStream:
    """Resumen seguro de un resultado de Torrentio."""

    name: str
    title: str
    has_url: bool
    has_info_hash: bool


class TorrentioClient:
    """Consulta el manifiesto y los streams de una instancia de Torrentio."""

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 15.0,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def get_manifest(self) -> dict[str, Any]:
        """Obtiene el manifiesto publico de la instancia."""
        response = self.session.get(f"{self.base_url}/manifest.json", timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("El manifiesto de Torrentio no es un objeto JSON")
        return payload

    def get_streams(
        self,
        imdb_id: str,
        content_type: str = "movie",
        season: int | None = None,
        episode: int | None = None,
    ) -> list[dict[str, Any]]:
        """Consulta streams para una pelicula o episodio identificado por IMDb."""
        self._validate_request(imdb_id, content_type, season, episode)

        content_id = imdb_id if content_type == "movie" else f"{imdb_id}:{season}:{episode}"

        response = self.session.get(
            f"{self.base_url}/stream/{content_type}/{content_id}.json",
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        streams = payload.get("streams", []) if isinstance(payload, dict) else []
        if not isinstance(streams, list):
            raise ValueError("La respuesta de Torrentio no contiene una lista de streams")
        return [stream for stream in streams if isinstance(stream, dict)]

    @staticmethod
    def summarize_streams(streams: list[dict[str, Any]]) -> list[TorrentioStream]:
        """Resume resultados sin imprimir URLs ni magnets."""
        return [
            TorrentioStream(
                name=str(stream.get("name") or ""),
                title=str(stream.get("title") or ""),
                has_url=bool(stream.get("url")),
                has_info_hash=bool(stream.get("infoHash")),
            )
            for stream in streams
        ]

    @staticmethod
    def _validate_request(
        imdb_id: str,
        content_type: str,
        season: int | None,
        episode: int | None,
    ) -> None:
        if not IMDB_ID_PATTERN.fullmatch(imdb_id):
            raise ValueError("imdb_id debe tener formato tt1234567")
        if content_type not in {"movie", "series"}:
            raise ValueError("content_type debe ser movie o series")
        if content_type == "movie" and (season is not None or episode is not None):
            raise ValueError("Una pelicula no acepta temporada ni episodio")
        if content_type == "series" and (
            season is None or episode is None or season < 0 or episode < 0
        ):
            raise ValueError("Una serie requiere season y episode validos")
