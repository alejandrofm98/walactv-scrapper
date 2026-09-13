#!/usr/bin/env python3
"""Sincroniza la lista comunitaria de canales Acestream (hashes.json en IPNS).

Descarga el JSON publicado por davidmuma (lista de canales acestream con
title/hash/group/logo/tvg_id), normaliza y deduplica las entradas y escribe
el resultado en ``data/json/acestream_channels.json`` para que iptv-api (o
cualquier consumidor del volumen iptv-data) lo ingeste.

El nombre IPNS se resuelve contra varias gateways publicas porque algunas
(wooord, inbrowser.link) bloquean acceso server-side. Se puede sobreescribir
con ``ACESTREAM_HASHES_URLS`` (separadas por comas).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger("acestream-hashes")

# Nombre IPNS de la lista comunitaria (k51... = libp2p key en base36).
IPNS_NAME = "k51qzi5uqu5dh5qej4b9wlcr5i6vhc7rcfkekhrxqek5c9lk6gdaiik820fecs"

# Gateways verificadas: dweb.link e ipfs.io resuelven este IPNS; w3s.link
# redirige a dweb (mismo rate-limit) y 4everland no resuelve nombres IPNS
# de libp2p-keys, asi que solo se mantienen las que funcionan. pinata
# devuelve 403 permanente para este nombre.
DEFAULT_GATEWAYS = [
    f"https://dweb.link/ipns/{IPNS_NAME}/hashes.json",
    f"https://ipfs.io/ipns/{IPNS_NAME}/hashes.json",
    f"https://{IPNS_NAME}.ipns.dweb.link/hashes.json",
]

# Las gateways publicas comparten rate-limit por IP (429). Entre intentos
# se espera este numero de segundos crecientes.
RETRY_BACKOFF_SECONDS = [5, 15]

# UA identificable: algunas gateways limitan mas el UA generico de requests.
REQUEST_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "walactv-scrapper/1.0 (acestream-hashes sync)",
}

HASH_RE = re.compile(r"^[a-f0-9]{40}$")

DEFAULT_OUTPUT = Path(__file__).parent.parent / "data" / "json" / "acestream_channels.json"


def _gateway_urls() -> list[str]:
    """URLs de gateways a probar, con override por env."""
    raw = os.getenv("ACESTREAM_HASHES_URLS", "").strip()
    if raw:
        return [u.strip() for u in raw.split(",") if u.strip()]
    return DEFAULT_GATEWAYS


def fetch_payload(timeout: int = 30) -> tuple[str, dict[str, Any]]:
    """Descarga hashes.json probando gateways en orden, con reintentos.

    Las gateways publicas devuelven 429 bajo rate-limit por IP; cada gateway
    se reintenta con backoff creciente antes de pasar a la siguiente. Lanza
    si todas fallan.
    """
    last_error: Exception | None = None
    for url in _gateway_urls():
        for attempt, backoff in enumerate([0, *RETRY_BACKOFF_SECONDS], start=1):
            if backoff:
                time.sleep(backoff)
            try:
                response = requests.get(
                    url, timeout=timeout, headers=REQUEST_HEADERS
                )
                response.raise_for_status()
                payload = response.json()
            except Exception as exc:  # pasamos al siguiente intento/gateway
                logger.warning("Gateway %s (intento %d) fallo: %s", url, attempt, exc)
                last_error = exc
                continue
            if not isinstance(payload.get("hashes"), list):
                logger.warning("Gateway %s devolvio un payload inesperado", url)
                last_error = ValueError(f"payload sin 'hashes' list en {url}")
                continue
            return url, payload
    raise RuntimeError(f"Ninguna gateway respondio: {last_error}")


def normalize_entry(raw: dict[str, Any]) -> dict[str, Any] | None:
    """Normaliza una entrada del JSON original. None si es inservible.

    Tolera datos sucios de la lista comunitaria: logo numerico, hashes con
    mayusculas/espacios, titulos vacios.
    """
    raw_hash = raw.get("hash")
    if not isinstance(raw_hash, str):
        return None
    content_hash = raw_hash.strip().lower()
    if not HASH_RE.match(content_hash):
        return None

    title_raw = raw.get("title")
    title = title_raw.strip() if isinstance(title_raw, str) else ""
    if not title:
        title = f"Acestream {content_hash[:8]}"

    group_raw = raw.get("group")
    group = group_raw.strip().upper() if isinstance(group_raw, str) else ""

    logo_raw = raw.get("logo")
    logo = logo_raw.strip() if isinstance(logo_raw, str) and logo_raw.startswith("http") else ""

    tvg_raw = raw.get("tvg_id")
    tvg_id = tvg_raw.strip() if isinstance(tvg_raw, str) else ""

    return {
        "title": title,
        "hash": content_hash,
        "group": group,
        "logo": logo,
        "tvg_id": tvg_id,
    }


def normalize_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Normaliza todas las entradas, deduplica por hash y ordena estable."""
    seen: set[str] = set()
    channels: list[dict[str, Any]] = []
    for raw in payload.get("hashes", []):
        if not isinstance(raw, dict):
            continue
        entry = normalize_entry(raw)
        if entry is None or entry["hash"] in seen:
            continue
        seen.add(entry["hash"])
        channels.append(entry)
    channels.sort(key=lambda c: (c["group"], c["title"], c["hash"]))
    return channels


def write_output(
    channels: list[dict[str, Any]],
    source_url: str,
    output_path: Path,
    source_generated: str = "",
) -> None:
    """Escribe el JSON normalizado en el volumen compartido."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated": datetime.now(UTC).isoformat(),
        "source_generated": source_generated,
        "source": source_url,
        "count": len(channels),
        "channels": channels,
    }
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parsea argumentos del comando."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Ruta de salida (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument("--timeout", type=int, default=30, help="Timeout HTTP por gateway (s)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Descarga y normaliza pero no escribe el fichero",
    )
    return parser.parse_args(argv)


def main() -> int:
    """Punto de entrada del sincronizador."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = parse_args()

    try:
        source_url, payload = fetch_payload(timeout=args.timeout)
    except RuntimeError as exc:
        logger.error("No se pudo descargar la lista: %s", exc)
        return 1

    channels = normalize_payload(payload)
    source_generated = str(payload.get("generated", ""))
    logger.info(
        "Lista descargada desde %s (generated=%s): %d canales validos de %d entradas",
        source_url,
        source_generated,
        len(channels),
        len(payload.get("hashes", [])),
    )

    if args.dry_run:
        logger.info("Dry run: no se escribe %s", args.output)
        return 0

    write_output(channels, source_url, args.output, source_generated)
    logger.info("Escrito %s (%.1f KB)", args.output, args.output.stat().st_size / 1024)
    return 0


if __name__ == "__main__":
    sys.exit(main())
