#!/usr/bin/env python3
"""
Importa episodios de IMDb desde datasets.imdbws.com/title.episode.tsv.gz
y actualiza series_episodes.imdb_id con el tconst de cada episodio.

El matching se hace por (series_metadata.imdb_id = parentTconst) +
(series_episodes.season_number = seasonNumber) +
(series_episodes.episode_number = episodeNumber).

Uso:
    python scripts/populate_episode_imdb_ids.py
    python scripts/populate_episode_imdb_ids.py --dry-run
    python scripts/populate_episode_imdb_ids.py --from-file /tmp/title.episode.tsv.gz
    python scripts/populate_episode_imdb_ids.py --batch-size 10000
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

import psycopg2

logger = logging.getLogger("imdb-episode-ids")
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler(sys.stderr)
_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
)
logger.addHandler(_handler)


def _build_dsn(env_var: str | None) -> str:
    """Build a PostgreSQL DSN from environment or env_var name."""
    if env_var:
        url = os.getenv(env_var)
        if url:
            return url
    # Try DATABASE_URL first
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    # Fall back to PG_* env vars
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    database = os.getenv("PG_DATABASE", "postgres")
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def _load_parent_to_catalog(conn) -> dict[str, str]:
    """Load mapping of series_metadata.imdb_id (parentTconst) to series_catalog.id."""
    mapping: dict[str, str] = {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT sm.imdb_id, sc.id "
            "FROM series_metadata sm "
            "JOIN series_catalog sc ON sc.id = sm.catalog_id "
            "WHERE sm.imdb_id IS NOT NULL"
        )
        for imdb_id, catalog_id in cur.fetchall():
            mapping[imdb_id] = catalog_id
    logger.info("Loaded %d parent imdb_id -> catalog_id mappings", len(mapping))
    return mapping


def _open_source(source_url: str | None, from_file: str | None):
    """Open the TSV source (URL or local file) as a gzip stream."""
    if from_file:
        logger.info("Reading from local file: %s", from_file)
        return gzip.open(from_file, "rt", encoding="utf-8")
    logger.info("Downloading: %s", source_url)
    assert source_url is not None
    resp = urllib.request.urlopen(source_url)
    return gzip.open(resp, "rt", encoding="utf-8")


def _update_batch(
    conn, rows: list[tuple[str, str, int, int]]
) -> int:
    """Execute an UPDATE batch for series_episodes using executemany. Returns rowcount."""
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            "UPDATE series_episodes "
            "SET imdb_id = %s "
            "WHERE catalog_id = %s AND season_number = %s AND episode_number = %s",
            rows,
        )
        return cur.rowcount


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
        "matched": 0,
        "updated": 0,
        "duration_seconds": 0.0,
    }
    start_time = time.time()

    conn = psycopg2.connect(_build_dsn(db_url))

    try:
        parent_to_catalog = _load_parent_to_catalog(conn)
        if not parent_to_catalog:
            logger.warning("No parent imdb_ids found in series_metadata. Nothing to do.")
            stats["duration_seconds"] = time.time() - start_time
            return stats

        fh = _open_source(source_url, from_file)
        batch: list[tuple[str, str, int, int]] = []
        total_matched = 0
        log_interval = max(1, 50000 // batch_size)

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
                if len(parts) < 4:
                    continue

                tconst = parts[0].strip()
                parent_tconst = parts[1].strip()
                season_str = parts[2].strip()
                episode_str = parts[3].strip()

                # Skip rows with null season/episode
                if season_str == "\\N" or episode_str == "\\N":
                    continue

                try:
                    season_number = int(season_str)
                    episode_number = int(episode_str)
                except (ValueError, IndexError):
                    continue

                if parent_tconst in parent_to_catalog:
                    catalog_id = parent_to_catalog[parent_tconst]
                    stats["matched"] += 1
                    if not dry_run:
                        batch.append((tconst, catalog_id, season_number, episode_number))
                    total_matched += 1

                    # Flush batch
                    if not dry_run and len(batch) >= batch_size:
                        _flush_batch(conn, batch, stats)
                        batch.clear()

                    if total_matched > 0 and total_matched % log_interval == 0:
                        logger.info(
                            "Progress: parsed=%d matched=%d",
                            stats["parsed"],
                            total_matched,
                        )

        except KeyboardInterrupt:
            logger.warning("Interrupted by user — flushing remaining batch...")
        finally:
            # Flush remaining
            if not dry_run and batch:
                _flush_batch(conn, batch, stats)
            fh.close()
    finally:
        conn.close()

    stats["duration_seconds"] = time.time() - start_time

    if dry_run:
        logger.info(
            "DRY RUN — would update %d series_episodes rows",
            stats["matched"],
        )
    else:
        logger.info(
            "Updated %d series_episodes rows in %.1f seconds",
            stats["updated"],
            stats["duration_seconds"],
        )

    return stats


def _flush_batch(
    conn,
    batch: list[tuple[str, str, int, int]],
    stats: dict[str, Any],
) -> None:
    """Flush pending batch to DB in a single transaction."""
    try:
        rc = _update_batch(conn, batch)
        stats["updated"] += rc or len(batch)
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Importa episodios de IMDb a series_episodes.imdb_id",
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--source",
        default="https://datasets.imdbws.com/title.episode.tsv.gz",
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
    print(f"matched={stats['matched']}")
    print(f"updated={stats['updated']}")
    print(f"duration_seconds={stats['duration_seconds']:.1f}")
    sys.exit(0)


if __name__ == "__main__":
    main()
