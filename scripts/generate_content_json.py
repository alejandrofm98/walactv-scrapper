"""
Genera archivo JSON con todos los canales para cache del cliente TV.
Se ejecuta al final de sync_iptv.py para mantener actualizada la cache local.

Usage:
    python generate_channels_json.py

O importado desde sync_iptv.py:
    await generar_channels_json(pool)
"""
import gzip
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Añadir el directorio padre al path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import DatabasePG
from utils.constants import (
    CHANNELS_TABLE,
    SYNC_METADATA_TABLE,
    SYNC_METADATA_ID,
)


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


async def generar_channels_json(pool=None, close_pool=True):
    """
    Genera el archivo channels.json.gz con todos los canales.

    Args:
        pool: Pool de conexiones PostgreSQL existente (opcional)
        close_pool: Si True, cierra el pool al terminar

    Returns:
        dict con informacion del archivo generado o None si hay error
    """
    inicio = time.time()

    print("\n" + "=" * 60)
    print("📦 GENERANDO JSON DE CANALES PARA CACHE TV")
    print("=" * 60)

    pool_to_close = None
    try:
        if pool is None:
            pool = await DatabasePG.get_pool()
            pool_to_close = pool

        query = """
            SELECT
                id,
                COALESCE(numero, 0) as numero,
                COALESCE(provider_id, '') as provider_id,
                COALESCE(logo, '') as logo,
                COALESCE(country, '') as country,
                COALESCE(nombre_normalizado, '') as nombre_normalizado,
                COALESCE(grupo_normalizado, '') as grupo_normalizado
            FROM channels
            ORDER BY numero ASC
        """

        rows = await pool.fetch(query)
        canales = []

        for row in rows:
            canales.append({
                "id": str(row['id']),
                "numero": row['numero'],
                "provider_id": row['provider_id'],
                "logo": row['logo'],
                "country": row['country'],
                "nombre_normalizado": row['nombre_normalizado'],
                "grupo_normalizado": row['grupo_normalizado']
            })

        total = len(canales)
        generated_at = datetime.now()

        payload = {
            "channels": canales,
            "total": total,
            "generated_at": generated_at
        }

        json_dir = Path(__file__).parent.parent / "data" / "json"
        json_dir.mkdir(parents=True, exist_ok=True)

        json_path = json_dir / "channels.json"
        gz_path = json_dir / "channels.json.gz"

        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, cls=DateTimeEncoder)

        with open(gz_path, 'wb') as f:
            with gzip.open(f, 'wt', encoding='utf-8') as gz:
                gz.write(json.dumps(payload, ensure_ascii=False, cls=DateTimeEncoder))

        json_size_mb = json_path.stat().st_size / (1024 * 1024)
        gz_size_mb = gz_path.stat().st_size / (1024 * 1024)

        await pool.execute("""
            INSERT INTO sync_metadata (id, channels_generated_at, channels_json_size_mb)
            VALUES ($1, $2, $3)
            ON CONFLICT (id) DO UPDATE SET
                channels_generated_at = EXCLUDED.channels_generated_at,
                channels_json_size_mb = EXCLUDED.channels_json_size_mb
        """, SYNC_METADATA_ID, generated_at, round(gz_size_mb, 2))

        duracion = time.time() - inicio

        print(f"  📺 Canales:     {total:,}")
        print(f"  📄 JSON:        {json_size_mb:.2f} MB")
        print(f"  🗜️  Gzip:        {gz_size_mb:.2f} MB")
        print(f"  ⏱️  Duración:    {duracion:.2f}s")
        print("=" * 60)
        print("✅ JSON de canales generado correctamente")

        return {
            "total": total,
            "json_path": str(json_path),
            "gz_path": str(gz_path),
            "json_size_mb": json_size_mb,
            "gz_size_mb": gz_size_mb,
            "generated_at": generated_at,
            "duracion": duracion
        }

    except Exception as e:
        print(f"❌ Error al generar JSON de canales: {e}")
        import traceback
        traceback.print_exc()
        return None

    finally:
        if pool_to_close and close_pool:
            await DatabasePG.close()


async def generar_movies_json(pool=None, close_pool=True):
    """
    Genera el archivo movies.json.gz con todas las peliculas.

    Args:
        pool: Pool de conexiones PostgreSQL existente (opcional)
        close_pool: Si True, cierra el pool al terminar

    Returns:
        dict con informacion del archivo generado o None si hay error
    """
    inicio = time.time()

    print("\n" + "=" * 60)
    print("🎬 GENERANDO JSON DE PELICULAS PARA CACHE TV")
    print("=" * 60)

    pool_to_close = None
    try:
        if pool is None:
            pool = await DatabasePG.get_pool()
            pool_to_close = pool

        query = """
            SELECT
                id,
                COALESCE(provider_id, '') as provider_id,
                COALESCE(logo, '') as logo,
                COALESCE(country, '') as country,
                COALESCE(nombre_normalizado, '') as nombre_normalizado,
                COALESCE(grupo_normalizado, '') as grupo_normalizado
            FROM movies
            ORDER BY nombre_normalizado ASC
        """

        rows = await pool.fetch(query)
        movies = []

        for row in rows:
            movies.append({
                "id": str(row['id']),
                "provider_id": row['provider_id'],
                "logo": row['logo'],
                "country": row['country'],
                "nombre_normalizado": row['nombre_normalizado'],
                "grupo_normalizado": row['grupo_normalizado']
            })

        total = len(movies)
        generated_at = datetime.now()

        payload = {
            "movies": movies,
            "total": total,
            "generated_at": generated_at
        }

        json_dir = Path(__file__).parent.parent / "data" / "json"
        json_dir.mkdir(parents=True, exist_ok=True)

        json_path = json_dir / "movies.json"
        gz_path = json_dir / "movies.json.gz"

        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, cls=DateTimeEncoder)

        with open(gz_path, 'wb') as f:
            with gzip.open(f, 'wt', encoding='utf-8') as gz:
                gz.write(json.dumps(payload, ensure_ascii=False, cls=DateTimeEncoder))

        json_size_mb = json_path.stat().st_size / (1024 * 1024)
        gz_size_mb = gz_path.stat().st_size / (1024 * 1024)

        await pool.execute("""
            INSERT INTO sync_metadata (id, movies_generated_at, movies_json_size_mb)
            VALUES ($1, $2, $3)
            ON CONFLICT (id) DO UPDATE SET
                movies_generated_at = EXCLUDED.movies_generated_at,
                movies_json_size_mb = EXCLUDED.movies_json_size_mb
        """, SYNC_METADATA_ID, generated_at, round(gz_size_mb, 2))

        duracion = time.time() - inicio

        print(f"  🎬 Peliculas:   {total:,}")
        print(f"  📄 JSON:        {json_size_mb:.2f} MB")
        print(f"  🗜️  Gzip:        {gz_size_mb:.2f} MB")
        print(f"  ⏱️  Duración:    {duracion:.2f}s")
        print("=" * 60)
        print("✅ JSON de peliculas generado correctamente")

        return {
            "total": total,
            "json_path": str(json_path),
            "gz_path": str(gz_path),
            "json_size_mb": json_size_mb,
            "gz_size_mb": gz_size_mb,
            "generated_at": generated_at,
            "duracion": duracion
        }

    except Exception as e:
        print(f"❌ Error al generar JSON de peliculas: {e}")
        import traceback
        traceback.print_exc()
        return None

    finally:
        if pool_to_close and close_pool:
            await DatabasePG.close()


async def generar_series_json(pool=None, close_pool=True):
    """
    Genera el archivo series.json.gz con todas las series.

    Args:
        pool: Pool de conexiones PostgreSQL existente (opcional)
        close_pool: Si True, cierra el pool al terminar

    Returns:
        dict con informacion del archivo generado o None si hay error
    """
    inicio = time.time()

    print("\n" + "=" * 60)
    print("📺 GENERANDO JSON DE SERIES PARA CACHE TV")
    print("=" * 60)

    pool_to_close = None
    try:
        if pool is None:
            pool = await DatabasePG.get_pool()
            pool_to_close = pool

        query = """
            SELECT
                id,
                COALESCE(provider_id, '') as provider_id,
                COALESCE(logo, '') as logo,
                COALESCE(country, '') as country,
                COALESCE(temporada, '0') as temporada,
                COALESCE(episodio, '0') as episodio,
                COALESCE(serie_name, '') as serie_name,
                COALESCE(nombre_normalizado, '') as nombre_normalizado,
                COALESCE(grupo_normalizado, '') as grupo_normalizado
            FROM series
            ORDER BY serie_name ASC, temporada ASC, episodio ASC
        """

        rows = await pool.fetch(query)
        series = []

        for row in rows:
            series.append({
                "id": str(row['id']),
                "provider_id": row['provider_id'],
                "logo": row['logo'],
                "country": row['country'],
                "temporada": row['temporada'],
                "episodio": row['episodio'],
                "serie_name": row['serie_name'],
                "nombre_normalizado": row['nombre_normalizado'],
                "grupo_normalizado": row['grupo_normalizado']
            })

        total = len(series)
        generated_at = datetime.now()

        payload = {
            "series": series,
            "total": total,
            "generated_at": generated_at
        }

        json_dir = Path(__file__).parent.parent / "data" / "json"
        json_dir.mkdir(parents=True, exist_ok=True)

        json_path = json_dir / "series.json"
        gz_path = json_dir / "series.json.gz"

        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, cls=DateTimeEncoder)

        with open(gz_path, 'wb') as f:
            with gzip.open(f, 'wt', encoding='utf-8') as gz:
                gz.write(json.dumps(payload, ensure_ascii=False, cls=DateTimeEncoder))

        json_size_mb = json_path.stat().st_size / (1024 * 1024)
        gz_size_mb = gz_path.stat().st_size / (1024 * 1024)

        await pool.execute("""
            INSERT INTO sync_metadata (id, series_generated_at, series_json_size_mb)
            VALUES ($1, $2, $3)
            ON CONFLICT (id) DO UPDATE SET
                series_generated_at = EXCLUDED.series_generated_at,
                series_json_size_mb = EXCLUDED.series_json_size_mb
        """, SYNC_METADATA_ID, generated_at, round(gz_size_mb, 2))

        duracion = time.time() - inicio

        print(f"  📺 Series:      {total:,}")
        print(f"  📄 JSON:        {json_size_mb:.2f} MB")
        print(f"  🗜️  Gzip:        {gz_size_mb:.2f} MB")
        print(f"  ⏱️  Duración:    {duracion:.2f}s")
        print("=" * 60)
        print("✅ JSON de series generado correctamente")

        return {
            "total": total,
            "json_path": str(json_path),
            "gz_path": str(gz_path),
            "json_size_mb": json_size_mb,
            "gz_size_mb": gz_size_mb,
            "generated_at": generated_at,
            "duracion": duracion
        }

    except Exception as e:
        print(f"❌ Error al generar JSON de series: {e}")
        import traceback
        traceback.print_exc()
        return None

    finally:
        if pool_to_close and close_pool:
            await DatabasePG.close()


async def generar_todos_json(pool=None, close_pool=True):
    """
    Genera los tres archivos JSON (channels, movies, series).
    Usa un solo pool de conexiones para eficiencia.

    Args:
        pool: Pool de conexiones existente (opcional)
        close_pool: Si True, cierra el pool al terminar
    """
    print("\n" + "=" * 60)
    print("📦 GENERANDO TODOS LOS JSONS PARA CACHE TV")
    print("=" * 60)

    pool_to_close = None
    try:
        if pool is None:
            pool = await DatabasePG.get_pool()
            pool_to_close = pool

        results = {}

        result_channels = await generar_channels_json(pool, close_pool=False)
        results['channels'] = result_channels

        result_movies = await generar_movies_json(pool, close_pool=False)
        results['movies'] = result_movies

        result_series = await generar_series_json(pool, close_pool=False)
        results['series'] = result_series

        print("\n" + "=" * 60)
        print("✅ TODOS LOS JSONS GENERADOS")
        print("=" * 60)

        return results

    except Exception as e:
        print(f"❌ Error al generar JSONs: {e}")
        import traceback
        traceback.print_exc()
        return None

    finally:
        if pool_to_close and close_pool:
            await DatabasePG.close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(generar_todos_json())