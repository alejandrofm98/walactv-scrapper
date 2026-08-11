"""Cliente HTTP y modelos para los segmentos de IntroDB."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import requests

INTRODB_BASE_URL = "https://api.introdb.app"
SEGMENT_TYPES = ("intro", "recap", "outro")


@dataclass(frozen=True)
class IntroDbSegment:
    """Segmento temporal agregado por IntroDB."""

    segment_type: str
    start_ms: int
    end_ms: int
    confidence: float | None = None
    submission_count: int | None = None
    source_updated_at: str | None = None


def _parse_segment(segment_type: str, payload: dict[str, Any] | None) -> IntroDbSegment | None:
    """Convierte un segmento de la API y descarta datos incompletos."""
    if not payload:
        return None

    start_ms = payload.get("start_ms")
    end_ms = payload.get("end_ms")
    if not isinstance(start_ms, int) or not isinstance(end_ms, int):
        return None
    if start_ms < 0 or end_ms <= start_ms:
        return None

    return IntroDbSegment(
        segment_type=segment_type,
        start_ms=start_ms,
        end_ms=end_ms,
        confidence=payload.get("confidence"),
        submission_count=payload.get("submission_count"),
        source_updated_at=payload.get("updated_at"),
    )


def parse_segments(payload: dict[str, Any]) -> list[IntroDbSegment]:
    """Parsea la respuesta de ``GET /segments``."""
    return [
        segment
        for segment_type in SEGMENT_TYPES
        if (segment := _parse_segment(segment_type, payload.get(segment_type))) is not None
    ]


class IntroDbClient:
    """Cliente tolerante a errores para lecturas puntuales de IntroDB."""

    def __init__(
        self,
        base_url: str = INTRODB_BASE_URL,
        timeout: float = 10.0,
        max_retries: int = 3,
        backoff_seconds: float = 1.0,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.session = session or requests.Session()
        self.session.headers.update({"Accept": "application/json", "User-Agent": "WalacTV/IntroDB"})

    def get_segments(self, imdb_id: str, season: int, episode: int) -> list[IntroDbSegment]:
        """Obtiene los segmentos de un episodio; 404 significa que no hay datos."""
        if not imdb_id.startswith("tt"):
            raise ValueError(f"IMDb ID inválido: {imdb_id}")
        if season < 1 or episode < 1:
            raise ValueError("La temporada y el episodio deben ser positivos")

        params = {"imdb_id": imdb_id, "season": season, "episode": episode}
        url = f"{self.base_url}/segments"
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
            except requests.RequestException:
                if attempt == self.max_retries:
                    raise
                time.sleep(self.backoff_seconds * (2**attempt))
                continue

            if response.status_code == 404:
                return []
            if response.status_code == 429 or response.status_code >= 500:
                if attempt == self.max_retries:
                    response.raise_for_status()
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else self.backoff_seconds * (2**attempt)
                time.sleep(delay)
                continue

            response.raise_for_status()
            return parse_segments(response.json())

        return []
