"""Prueba aislada de compatibilidad con Torrentio.

No modifica PostgreSQL ni el catalogo local. Solo muestra metadatos de la respuesta.

Ejemplos:
    python -m iptv_scrapper.probe_torrentio --imdb-id tt0111161
    python -m iptv_scrapper.probe_torrentio --imdb-id tt0903747 --type series --season 1 --episode 1
"""

from __future__ import annotations

import argparse
import os

from iptv_scrapper.torrentio import TorrentioClient


def build_parser() -> argparse.ArgumentParser:
    """Construye el parser del comando."""
    parser = argparse.ArgumentParser(description="Prueba de integracion con Torrentio")
    parser.add_argument(
        "--imdb-id", required=True, help="Identificador IMDb, por ejemplo tt0111161"
    )
    parser.add_argument("--type", choices=("movie", "series"), default="movie")
    parser.add_argument("--season", type=int)
    parser.add_argument("--episode", type=int)
    parser.add_argument(
        "--base-url",
        default=os.getenv("TORRENTIO_BASE_URL", "https://torrentio.strem.fun"),
        help="URL de la instancia Torrentio",
    )
    return parser


def main() -> int:
    """Consulta Torrentio y muestra un resumen no sensible."""
    args = build_parser().parse_args()
    client = TorrentioClient(args.base_url)

    manifest = client.get_manifest()
    resources = manifest.get("resources", [])
    stream_resource = next(
        (resource for resource in resources if resource.get("name") == "stream"),
        None,
    )
    print(f"Instancia: {client.base_url}")
    print(f"Version: {manifest.get('version', 'desconocida')}")
    print(f"Catalogos declarados: {len(manifest.get('catalogs', []))}")
    print(f"Recurso stream: {'si' if stream_resource else 'no'}")

    streams = client.get_streams(
        imdb_id=args.imdb_id,
        content_type=args.type,
        season=args.season,
        episode=args.episode,
    )
    summaries = client.summarize_streams(streams)
    print(f"Streams devueltos: {len(summaries)}")
    for index, stream in enumerate(summaries[:10], start=1):
        print(
            f"  {index}. name={stream.name!r} title={stream.title!r} "
            f"url={'si' if stream.has_url else 'no'} "
            f"info_hash={'si' if stream.has_info_hash else 'no'}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
