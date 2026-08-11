#!/usr/bin/env python3
"""Sincroniza segmentos de IntroDB para episodios con IMDb conocido.

La tabla ``video_segments`` debe existir en ``iptv-db`` antes de ejecutar este script.
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from iptv_db.engine import build_url, get_sync_engine, get_sync_session_factory
from sqlalchemy import text

from introdb import IntroDbClient, IntroDbSegment

logger = logging.getLogger("introdb-sync")


def _build_session():
    """Crea una sesión síncrona usando DATABASE_URL o PG_*."""
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        engine = get_sync_engine(database_url)
    else:
        engine = get_sync_engine(
            build_url(
                host=os.getenv("PG_HOST", "localhost"),
                port=int(os.getenv("PG_PORT", "5432")),
                database=os.getenv("PG_DATABASE", "postgres"),
                user=os.getenv("PG_USER", "postgres"),
                password=os.getenv("PG_PASSWORD", ""),
                async_driver=False,
            )
        )
    return get_sync_session_factory(engine)()


def _load_episodes(session, limit: int | None, ttl_days: int) -> list[dict[str, Any]]:
    """Carga episodios que no tienen segmentos o cuyo caché caducó."""
    params: dict[str, Any] = {"expires_before": datetime.now(UTC) - timedelta(days=ttl_days)}
    limit_sql = ""
    if limit is not None:
        params["limit"] = limit
        limit_sql = "LIMIT :limit"

    result = session.execute(
        text(f"""
            SELECT se.id, se.imdb_id, se.season_number, se.episode_number
            FROM series_episodes se
            LEFT JOIN video_segment_sync vss
              ON vss.episode_id = se.id AND vss.source = 'introdb'
            WHERE se.imdb_id IS NOT NULL
              AND (vss.episode_id IS NULL OR vss.expires_at < :expires_before)
            GROUP BY se.id, se.imdb_id, se.season_number, se.episode_number
            ORDER BY se.id
            {limit_sql}
        """),
        params,
    )
    return [dict(row) for row in result.mappings()]


def _save_segments(session, episode_id: Any, segments: list[IntroDbSegment]) -> int:
    """Reemplaza el caché IntroDB de un episodio dentro de la transacción actual."""
    session.execute(
        text("DELETE FROM video_segments WHERE episode_id = :episode_id AND source = 'introdb'"),
        {"episode_id": episode_id},
    )
    session.execute(
        text("""
            INSERT INTO video_segment_sync (episode_id, source, fetched_at, expires_at, not_found)
            VALUES (:episode_id, 'introdb', NOW(), NOW() + INTERVAL '30 days', :not_found)
            ON CONFLICT (episode_id, source) DO UPDATE SET
                fetched_at = EXCLUDED.fetched_at,
                expires_at = EXCLUDED.expires_at,
                not_found = EXCLUDED.not_found
        """),
        {"episode_id": episode_id, "not_found": not segments},
    )
    for segment in segments:
        session.execute(
            text("""
                INSERT INTO video_segments (
                    id, episode_id, segment_type, start_ms, end_ms, confidence,
                    submission_count, source, source_updated_at, fetched_at, expires_at
                ) VALUES (
                    gen_random_uuid(), :episode_id, :segment_type, :start_ms, :end_ms,
                    :confidence, :submission_count, 'introdb', :source_updated_at,
                    NOW(), NOW() + INTERVAL '30 days'
                )
            """),
            {
                "episode_id": episode_id,
                "segment_type": segment.segment_type,
                "start_ms": segment.start_ms,
                "end_ms": segment.end_ms,
                "confidence": segment.confidence,
                "submission_count": segment.submission_count,
                "source_updated_at": segment.source_updated_at,
            },
        )
    return len(segments)


def sync_segments(
    client: IntroDbClient,
    *,
    limit: int | None = None,
    ttl_days: int = 30,
    delay_seconds: float = 0.1,
    dry_run: bool = False,
) -> dict[str, int]:
    """Sincroniza episodios pendientes y devuelve estadísticas."""
    stats = {"episodes": 0, "requests": 0, "segments": 0, "not_found": 0}
    with _build_session() as session:
        episodes = _load_episodes(session, limit, ttl_days)
        stats["episodes"] = len(episodes)
        for episode in episodes:
            segments = client.get_segments(
                episode["imdb_id"], episode["season_number"], episode["episode_number"]
            )
            stats["requests"] += 1
            if not segments:
                stats["not_found"] += 1
            elif not dry_run:
                stats["segments"] += _save_segments(session, episode["id"], segments)
            else:
                stats["segments"] += len(segments)
            if not dry_run:
                session.commit()
            time.sleep(delay_seconds)
    return stats


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parsea argumentos del comando."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, help="Máximo de episodios a consultar")
    parser.add_argument("--ttl-days", type=int, default=30)
    parser.add_argument("--delay", type=float, default=0.1)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main() -> None:
    """Punto de entrada del sincronizador."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = parse_args()
    stats = sync_segments(
        IntroDbClient(),
        limit=args.limit,
        ttl_days=args.ttl_days,
        delay_seconds=args.delay,
        dry_run=args.dry_run,
    )
    logger.info("IntroDB sync: %s", stats)


if __name__ == "__main__":
    main()
