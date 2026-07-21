#!/usr/bin/env python3
"""
Importa ratings de IMDb desde datasets.imdbws.com/title.ratings.tsv.gz
y actualiza las columnas imdb_rating / imdb_votes en movies_metadata y series_metadata.

Uso:
    python scripts/import_imdb_ratings.py
    python scripts/import_imdb_ratings.py --dry-run
    python scripts/import_imdb_ratings.py --from-file /tmp/title.ratings.tsv.gz
    python scripts/import_imdb_ratings.py --batch-size 10000
"""

import argparse
import gzip
import logging
import os
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv

    env_path = Path(__file__).parent.parent / "docker" / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass

from iptv_db.engine import build_url, get_sync_engine, get_sync_session_factory
from iptv_db.models import MovieMetadata, SeriesMetadata
from sqlalchemy import select, text

logger = logging.getLogger("imdb-ratings")
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler(sys.stderr)
_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
)
logger.addHandler(_handler)


def _build_session():
    """Build a sync SQLAlchemy session from PG_* or DATABASE_URL env vars."""
    url = os.getenv("DATABASE_URL")
    if url:
        engine = get_sync_engine(url)
    else:
        host = os.getenv("PG_HOST", "localhost")
        port = int(os.getenv("PG_PORT", "5432"))
        database = os.getenv("PG_DATABASE", "postgres")
        user = os.getenv("PG_USER", "postgres")
        password = os.getenv("PG_PASSWORD", "")
        url = build_url(host, port, database, user, password, async_driver=False)
        engine = get_sync_engine(url)
    Session = get_sync_session_factory(engine)
    return Session()


def _load_imdb_ids(session) -> tuple[set[str], set[str], set[str]]:
    """Load all non-null imdb_id values from movies_metadata, series_metadata, and series_episodes."""
    movies: set[str] = set()
    series: set[str] = set()
    episodes: set[str] = set()

    for (imdb_id,) in session.execute(
        select(MovieMetadata.imdb_id).where(MovieMetadata.imdb_id.isnot(None))
    ).all():
        movies.add(imdb_id)

    for (imdb_id,) in session.execute(
        select(SeriesMetadata.imdb_id).where(SeriesMetadata.imdb_id.isnot(None))
    ).all():
        series.add(imdb_id)

    for (imdb_id,) in session.execute(
        text("SELECT imdb_id FROM series_episodes WHERE imdb_id IS NOT NULL")
    ).all():
        episodes.add(imdb_id)

    logger.info(
        "Loaded %d movie imdb_ids, %d series imdb_ids, %d episode imdb_ids",
        len(movies),
        len(series),
        len(episodes),
    )
    return movies, series, episodes


def _open_source(source_url: str | None, from_file: str | None):
    """Open the TSV source (URL or local file) as a gzip stream."""
    if from_file:
        logger.info("Reading from local file: %s", from_file)
        return gzip.open(from_file, "rt", encoding="utf-8")
    logger.info("Downloading: %s", source_url)
    assert source_url is not None  # guaranteed by mutually exclusive group
    resp = urllib.request.urlopen(source_url)
    return gzip.open(resp, "rt", encoding="utf-8")


def _update_batch(session, table: str, rows: list[tuple[str, float | None, int | None]]) -> int:
    """Execute an UPDATE batch for one table using executemany. Returns rowcount."""
    if not rows:
        return 0
    params = [
        {"rating": rating, "votes": votes, "imdb_id": imdb_id} for imdb_id, rating, votes in rows
    ]
    result = session.execute(
        text(f"""
            UPDATE {table}
            SET imdb_rating = :rating, imdb_votes = :votes, updated_at = NOW()
            WHERE imdb_id = :imdb_id
        """),
        params,
    )
    return result.rowcount


def run_import(
    source_url: str,
    from_file: str | None,
    batch_size: int,
    db_url: str | None,
    dry_run: bool,
) -> dict[str, Any]:
    """Main import routine. Returns a stats dict."""
    stats: dict[str, Any] = {
        "downloaded": 0,
        "parsed": 0,
        "matched_movies": 0,
        "matched_series": 0,
        "matched_episodes": 0,
        "updated_movies": 0,
        "updated_series": 0,
        "updated_episodes": 0,
        "duration_seconds": 0.0,
    }
    start_time = time.time()

    if db_url:
        engine = get_sync_engine(os.getenv(db_url, ""))
        Session = get_sync_session_factory(engine)
        session = Session()
    else:
        session = _build_session()

    try:
        movies_set, series_set, episodes_set = _load_imdb_ids(session)
        if not movies_set and not series_set and not episodes_set:
            logger.warning("No imdb_ids found in database. Nothing to do.")
            stats["duration_seconds"] = time.time() - start_time
            return stats

        fh = _open_source(source_url, from_file)
        batch_movies: list[tuple[str, float | None, int | None]] = []
        batch_series: list[tuple[str, float | None, int | None]] = []
        batch_episodes: list[tuple[str, float | None, int | None]] = []
        total_matched = 0
        log_interval = max(1, 50000 // batch_size)  # Log every ~50k matched

        try:
            for line_no, line in enumerate(fh, start=1):
                if line_no == 1:
                    # Skip header
                    continue

                line = line.strip()
                if not line:
                    continue

                stats["parsed"] += 1
                parts = line.split("\t")
                if len(parts) < 3:
                    continue

                tconst = parts[0].strip()
                try:
                    rating = float(parts[1].strip())
                except (ValueError, IndexError):
                    rating = None
                try:
                    votes = int(parts[2].strip())
                except (ValueError, IndexError):
                    votes = None

                if tconst in movies_set:
                    stats["matched_movies"] += 1
                    if not dry_run:
                        batch_movies.append((tconst, rating, votes))
                    total_matched += 1
                elif tconst in series_set:
                    stats["matched_series"] += 1
                    if not dry_run:
                        batch_series.append((tconst, rating, votes))
                    total_matched += 1
                elif tconst in episodes_set:
                    stats["matched_episodes"] += 1
                    if not dry_run:
                        batch_episodes.append((tconst, rating, votes))
                    total_matched += 1

                # Flush batches
                if not dry_run and (
                    len(batch_movies) >= batch_size
                    or len(batch_series) >= batch_size
                    or len(batch_episodes) >= batch_size
                ):
                    _flush_batches(session, batch_movies, batch_series, batch_episodes, stats)
                    batch_movies.clear()
                    batch_series.clear()
                    batch_episodes.clear()

                if total_matched > 0 and total_matched % log_interval == 0:
                    logger.info(
                        "Progress: parsed=%d matched=%d (movies=%d series=%d episodes=%d)",
                        stats["parsed"],
                        total_matched,
                        stats["matched_movies"],
                        stats["matched_series"],
                        stats["matched_episodes"],
                    )

        except KeyboardInterrupt:
            logger.warning("Interrupted by user — flushing remaining batches...")
        finally:
            # Flush remaining
            if not dry_run and (batch_movies or batch_series or batch_episodes):
                _flush_batches(session, batch_movies, batch_series, batch_episodes, stats)
            fh.close()
    finally:
        session.close()

    stats["duration_seconds"] = time.time() - start_time

    if dry_run:
        logger.info(
            "DRY RUN — would update %d movies, %d series, %d episodes",
            stats["matched_movies"],
            stats["matched_series"],
            stats["matched_episodes"],
        )
    else:
        logger.info(
            "Updated %d movies, %d series, %d episodes in %.1f seconds",
            stats["updated_movies"],
            stats["updated_series"],
            stats["updated_episodes"],
            stats["duration_seconds"],
        )

    return stats


def _flush_batches(
    session,
    batch_movies: list[tuple[str, float | None, int | None]],
    batch_series: list[tuple[str, float | None, int | None]],
    batch_episodes: list[tuple[str, float | None, int | None]],
    stats: dict[str, Any],
) -> None:
    """Flush pending batches to DB in a single transaction each."""
    try:
        if batch_movies:
            rc = _update_batch(session, "movies_metadata", batch_movies)
            stats["updated_movies"] += rc or len(batch_movies)
        if batch_series:
            rc = _update_batch(session, "series_metadata", batch_series)
            stats["updated_series"] += rc or len(batch_series)
        if batch_episodes:
            rc = _update_batch(session, "series_episodes", batch_episodes)
            stats["updated_episodes"] += rc or len(batch_episodes)
        session.commit()
    except Exception:
        session.rollback()
        raise


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Importa ratings de IMDb a movies_metadata y series_metadata",
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--source",
        default="https://datasets.imdbws.com/title.ratings.tsv.gz",
        help="URL del archivo TSV.GZ de IMDb (default: datasets.imdbws.com)",
    )
    source.add_argument(
        "--from-file",
        help="Ruta local al archivo TSV.GZ (alternativa a --source)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo analiza y muestra estadisticas, no escribe en BD",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Registros por batch de UPDATE (default: 5000)",
    )
    parser.add_argument(
        "--db-url",
        help="Variable de entorno con la URL de BD (default: DATABASE_URL o PG_*)",
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    source_url = args.source if not args.from_file else ""
    stats = run_import(
        source_url=source_url,
        from_file=args.from_file,
        batch_size=args.batch_size,
        db_url=args.db_url,
        dry_run=args.dry_run,
    )
    # Print final summary to stdout
    print(f"download_url={args.source or args.from_file or ''}")
    print(f"parsed_lines={stats['parsed']}")
    print(f"matched_movies={stats['matched_movies']}")
    print(f"matched_series={stats['matched_series']}")
    print(f"matched_episodes={stats['matched_episodes']}")
    print(f"updated_movies={stats['updated_movies']}")
    print(f"updated_series={stats['updated_series']}")
    print(f"updated_episodes={stats['updated_episodes']}")
    print(f"duration_seconds={stats['duration_seconds']:.1f}")
    sys.exit(0)


if __name__ == "__main__":
    main()
