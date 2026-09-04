"""Cliente minimo para consultar streams de Torrentio."""

from __future__ import annotations

import logging
import os
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
        self.proxy = os.getenv("TORRENTIO_PROXY") or None
        self.flaresolverr = os.getenv("TORRENTIO_FLARESOLVERR") or None
        # Mismos filtros que usan las apps (wolfmax4k + spanish/english):
        # si el classify mirara TODA la config de Torrentio marcaria titulos
        # que los clientes no encuentran -> tarjetas sin enlaces.
        providers = os.getenv("TORRENTIO_PROVIDERS", "wolfmax4k,comando,yts,eztv,rarbg,1337x,thepiratebay,kickasstorrents,torrentgalaxy,magnetdl,torrentproject,ibit,filelist")
        languages = os.getenv("TORRENTIO_LANGUAGES", "spanish,english")
        self.config_path = ""
        if providers or languages:
            parts = []
            if providers:
                parts.append(f"providers={providers}")
            if languages:
                parts.append(f"language={languages}")
            self.config_path = "/".join(parts)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def _get(self, url: str) -> requests.Response:
        """GET con escape de Cloudflare: si devuelve 403/429 y hay
        FlareSolverr configurado, resuelve el reto (cookies cf_clearance +
        User-Agent) y reintenta una vez."""
        response = self.session.get(url, timeout=self.timeout, **self._proxy_kwargs())
        if response.status_code in (403, 429) and self.flaresolverr:
            self._solve_challenge(url)
            response = self.session.get(url, timeout=self.timeout, **self._proxy_kwargs())
        return response

    def _solve_challenge(self, url: str) -> None:
        """Resuelve el reto de Cloudflare via FlareSolverr y aplica las
        cookies y el User-Agent a la sesion (van ligados)."""
        endpoint = self.flaresolverr.rstrip("/") + "/v1"
        payload = {"cmd": "request.get", "url": url, "maxTimeout": 60000}
        response = requests.post(endpoint, json=payload, timeout=90)
        response.raise_for_status()
        solution = (response.json() or {}).get("solution") or {}
        for cookie in solution.get("cookies") or []:
            name = cookie.get("name")
            value = cookie.get("value")
            if name and value:
                self.session.cookies.set(name, value, domain=cookie.get("domain") or "")
        user_agent = solution.get("userAgent")
        if user_agent:
            self.session.headers["User-Agent"] = user_agent
        logging.getLogger("torrentio-client").info(
            "Reto de Cloudflare resuelto via FlareSolverr (%d cookies)",
            len(solution.get("cookies") or []),
        )

    def get_manifest(self) -> dict[str, Any]:
        """Obtiene el manifiesto publico de la instancia."""
        response = self._get(f"{self.base_url}/manifest.json")
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

        response = self._get(
            f"{self.base_url}/{self.config_path}/stream/{content_type}/{content_id}.json"
            if self.config_path
            else f"{self.base_url}/stream/{content_type}/{content_id}.json"
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

    def _proxy_kwargs(self) -> dict[str, Any]:
        if not self.proxy:
            return {}
        return {"proxies": {"http": self.proxy, "https": self.proxy}}
