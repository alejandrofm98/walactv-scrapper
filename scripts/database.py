"""
Módulo de base de datos usando PostgreSQL con asyncpg
Esquema: channel_mappings y channel_variants (reemplazan 4 tablas antiguas)

F3a: ahora usa iptv_db.engine internamente. API publica preservada
para backward compat. asyncpg sigue siendo el driver subyacente para
los consumidores existentes.
"""

import json
import os
import pathlib
from datetime import date, datetime
from typing import Any

import asyncpg
from asyncpg import Pool
from iptv_db.engine import build_url, get_async_engine, get_async_session_factory

try:
    from dotenv import load_dotenv

    env_path = pathlib.Path(__file__).parent.parent / "docker" / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    print("⚠️  python-dotenv no instalado, usando solo variables de entorno del sistema")


class DatabasePG:
    """
    Cliente singleton de PostgreSQL.
    F3a: internamente usa iptv_db.engine + asyncpg pool (backward compat).
    """

    _pool: Pool | None = None
    _engine = None  # iptv-db async engine
    _session_factory = None  # iptv-db async session factory
    _host: str | None = None
    _port: int | None = None
    _user: str | None = None
    _password: str | None = None
    _database: str | None = None

    @classmethod
    async def initialize(cls) -> Pool:
        """Inicializa el pool asyncpg + el engine iptv-db (F3a)."""
        if cls._pool is not None:
            return cls._pool

        cls._host = os.getenv("PG_HOST")
        cls._port = int(os.getenv("PG_PORT", "5432"))
        cls._user = os.getenv("PG_USER")
        cls._password = os.getenv("PG_PASSWORD")
        cls._database = os.getenv("PG_DATABASE", "postgres")

        if not cls._host or not cls._user or not cls._password:
            raise ValueError(
                "❌ No se encontraron las variables de entorno PostgreSQL.\n"
                "Asegúrate de tener un archivo .env con:\n"
                "PG_HOST=tu-host\n"
                "PG_USER=tu-user\n"
                "PG_PASSWORD=tu-password\n"
                "PG_DATABASE=tu-db"
            )

        try:
            # --- F3a: iptv-db engine (aun sin consumidores, preparado para F3b-F3d) ---
            url = build_url(
                host=cls._host,
                port=cls._port,
                database=cls._database,
                user=cls._user,
                password=cls._password,
                async_driver=True,
            )
            cls._engine = get_async_engine(url, pool_size=5, max_overflow=15)
            cls._session_factory = get_async_session_factory(cls._engine)

            # --- asyncpg pool para backward compat con consumidores existentes ---
            cls._pool = await asyncpg.create_pool(
                host=cls._host,
                port=cls._port,
                user=cls._user,
                password=cls._password,
                database=cls._database,
                min_size=5,
                max_size=20,
            )
            print("🔥 PostgreSQL inicializado correctamente")
            return cls._pool
        except Exception as e:
            print(f"❌ Error al conectar con PostgreSQL: {e}")
            raise

    @classmethod
    async def get_pool(cls) -> Pool:
        """Obtiene el pool de conexiones asyncpg (backward compat)."""
        if cls._pool is None:
            await cls.initialize()
        return cls._pool

    @classmethod
    def get_session_factory(cls):
        """Devuelve el async session factory de iptv-db. NUEVO en F3a."""
        if cls._session_factory is None:
            raise RuntimeError("DatabasePG no inicializado. Llama a initialize() primero.")
        return cls._session_factory

    @classmethod
    async def close(cls):
        """Cierra el pool asyncpg y libera el engine iptv-db."""
        if cls._pool is not None:
            await cls._pool.close()
            cls._pool = None
        if cls._engine is not None:
            await cls._engine.dispose()
            cls._engine = None
            cls._session_factory = None

    @classmethod
    def reset(cls):
        """Resetea la instancia (útil para testing)."""
        cls._pool = None
        cls._engine = None
        cls._session_factory = None


class ConfigManager:
    """
    Gestor de configuración usando la tabla config de PostgreSQL
    """

    @staticmethod
    async def get_config(key: str) -> str | None:
        """Obtiene un valor de configuración por su key"""
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                result = await conn.fetchrow("SELECT value FROM config WHERE key = $1", key)
                if result:
                    return result["value"]
                return None
        except Exception as e:
            print(f"❌ Error obteniendo config '{key}': {e}")
            return None


class ChannelMappingManager:
    """
    Gestor de mapeos simplificado
    Tablas: channel_mappings + channel_variants
    """

    @staticmethod
    def get_all_mappings_with_channels_sync() -> dict[str, list[str]]:
        """Versión sincronica para compatibilidad con código sync"""
        import asyncio

        return asyncio.run(ChannelMappingManager.get_all_mappings_with_channels())

    @staticmethod
    async def upsert_mapping(
        source_name: str,
        display_name: str,
        channel_ids: list[str] = None,
        qualities: list[str] = None,
    ) -> int | None:
        """
        Inserta o actualiza un mapeo completo con sus variantes
        """
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                async with conn.transaction():
                    mapping_data = {"source_name": source_name, "display_name": display_name}

                    result = await conn.fetchrow(
                        """
                        INSERT INTO channel_mappings (source_name, display_name)
                        VALUES ($1, $2)
                        ON CONFLICT (source_name) DO UPDATE
                        SET display_name = EXCLUDED.display_name
                        RETURNING id
                        """,
                        source_name,
                        display_name,
                    )

                    if not result:
                        return None

                    mapping_id = result["id"]

                    if channel_ids:
                        await conn.execute(
                            "DELETE FROM channel_variants WHERE mapping_id = $1", mapping_id
                        )

                        for i, channel_id in enumerate(channel_ids):
                            quality = qualities[i] if qualities and i < len(qualities) else "HD"
                            await conn.execute(
                                """
                                INSERT INTO channel_variants (mapping_id, channel_id, quality, priority)
                                VALUES ($1, $2, $3, $4)
                                """,
                                mapping_id,
                                channel_id,
                                quality,
                                i,
                            )

                    return mapping_id

        except Exception as e:
            print(f"❌ Error guardando mapeo '{source_name}': {e}")
            return None

    @staticmethod
    async def get_mapping_by_source(source_name: str) -> dict | None:
        """Obtiene un mapeo por su nombre de origen (futbolenlatv)"""
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                result = await conn.fetchrow(
                    "SELECT * FROM channel_mappings WHERE source_name = $1", source_name
                )

                if result:
                    mapping = dict(result)
                    variants = await conn.fetch(
                        "SELECT * FROM channel_variants WHERE mapping_id = $1 ORDER BY priority",
                        mapping["id"],
                    )
                    mapping["variants"] = [dict(v) for v in variants]
                    return mapping
                return None
        except Exception as e:
            print(f"❌ Error obteniendo mapeo '{source_name}': {e}")
            return None

    @staticmethod
    async def get_channel_ids_from_source(source_name: str) -> list[str]:
        """Obtiene lista de channel_ids desde un nombre de origen"""
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                result = await conn.fetchrow(
                    "SELECT id FROM channel_mappings WHERE source_name = $1", source_name
                )

                if not result:
                    return []

                variants = await conn.fetch(
                    "SELECT channel_id FROM channel_variants WHERE mapping_id = $1 ORDER BY priority",
                    result["id"],
                )

                return [v["channel_id"] for v in variants if v["channel_id"]]
        except Exception as e:
            print(f"❌ Error obteniendo channel_ids para '{source_name}': {e}")
            return []

    @staticmethod
    async def get_all_mappings() -> list[dict]:
        """Obtiene todos los mapeos con sus variantes"""
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch("SELECT * FROM channel_mappings")

                if not rows:
                    return []

                mappings = [dict(row) for row in rows]
                for mapping in mappings:
                    variants = await conn.fetch(
                        "SELECT * FROM channel_variants WHERE mapping_id = $1 ORDER BY priority",
                        mapping["id"],
                    )
                    mapping["variants"] = [dict(v) for v in variants]

                return mappings
        except Exception as e:
            print(f"❌ Error obteniendo mapeos: {e}")
            return []

    @staticmethod
    async def get_all_mappings_simple() -> dict[str, str]:
        """
        Obtiene mapeo simple: source_name -> display_name
        """
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch("SELECT source_name, display_name FROM channel_mappings")

                if rows:
                    return {row["source_name"]: row["display_name"] for row in rows}
                return {}
        except Exception as e:
            print(f"❌ Error obteniendo mapeos simples: {e}")
            return {}

    @staticmethod
    async def get_all_mappings_with_channels() -> dict[str, list[str]]:
        """
        Obtiene mapeo completo: source_name -> [channel_id, channel_id, ...]
        Filtra variantes con estado_stream = 'error'.
        Si todas las variantes están muertas, incluye todas (fallback).
        """
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT cm.source_name, cv.channel_id, cv.priority, ch.estado_stream
                    FROM channel_mappings cm
                    LEFT JOIN channel_variants cv ON cm.id = cv.mapping_id
                    LEFT JOIN channels ch ON cv.channel_id = ch.id
                    ORDER BY cm.source_name, cv.priority
                    """
                )

                all_by_source: dict[str, list[str]] = {}
                ok_by_source: dict[str, list[str]] = {}
                for row in rows:
                    source_name = row["source_name"]
                    channel_id = row["channel_id"]
                    if not channel_id:
                        continue
                    if source_name not in all_by_source:
                        all_by_source[source_name] = []
                        ok_by_source[source_name] = []
                    all_by_source[source_name].append(channel_id)
                    estado = row["estado_stream"]
                    if estado is None or estado == "ok":
                        ok_by_source[source_name].append(channel_id)

                mappings: dict[str, list[str]] = {}
                for sn in all_by_source:
                    mappings[sn] = ok_by_source[sn] if ok_by_source[sn] else all_by_source[sn]

                return mappings
        except Exception as e:
            print(f"❌ Error obteniendo mapeos con canales: {e}")
            return {}

    @staticmethod
    async def ensure_health_columns():
        """Añade columnas de health check a la tabla channels si no existen"""
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                await conn.execute("""
                    ALTER TABLE channels
                    ADD COLUMN IF NOT EXISTS estado_stream TEXT,
                    ADD COLUMN IF NOT EXISTS ultimo_chequeo TIMESTAMPTZ,
                    ADD COLUMN IF NOT EXISTS tiempo_respuesta_ms INTEGER
                """)
        except Exception as e:
            print(f"⚠️ Error añadiendo columnas health check: {e}")

    @staticmethod
    async def update_channel_health(channel_id: str, estado: str, tiempo_ms: int = 0):
        """Actualiza estado de salud de un canal"""
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE channels
                    SET estado_stream = $1, tiempo_respuesta_ms = $2, ultimo_chequeo = NOW()
                    WHERE id = $3
                    """,
                    estado,
                    tiempo_ms,
                    channel_id,
                )
        except Exception:
            pass

    @staticmethod
    async def get_variants_for_source_names(source_names: list[str]) -> dict[str, list[dict]]:
        """
        Obtiene variantes con stream_url para una lista de source_names.
        Returns: {source_name: [{channel_id, quality, priority, stream_url}, ...]}
        """
        if not source_names:
            return {}
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT cm.source_name, cv.channel_id, cv.quality, cv.priority,
                           ch.stream_url
                    FROM channel_mappings cm
                    INNER JOIN channel_variants cv ON cm.id = cv.mapping_id
                    LEFT JOIN channels ch ON cv.channel_id = ch.id
                    WHERE cm.source_name = ANY($1)
                    ORDER BY cm.source_name, cv.priority
                    """,
                    list(source_names),
                )

                result: dict[str, list[dict]] = {}
                for row in rows:
                    sn = row["source_name"]
                    if sn not in result:
                        result[sn] = []
                    cid = row["channel_id"]
                    if cid:
                        result[sn].append(
                            {
                                "channel_id": cid,
                                "quality": row["quality"],
                                "priority": row["priority"],
                                "stream_url": row["stream_url"] or "",
                            }
                        )
                return result
        except Exception as e:
            print(f"❌ Error obteniendo variantes: {e}")
            return {}


class CalendarioAcestreamManager:
    """
    Gestor de calendario de acestream
    """

    @staticmethod
    async def upsert_partido(
        fecha: date,
        hora: str,
        equipos: str,
        competicion: str = None,
        canales: list[str] = None,
        categoria: str = None,
        imagen_evento: str = None,
        subtitulo_competicion: str = None,
    ) -> bool:
        """Inserta o actualiza un partido del calendario"""
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    ALTER TABLE calendario
                    ADD COLUMN IF NOT EXISTS imagen_evento TEXT,
                    ADD COLUMN IF NOT EXISTS subtitulo_competicion TEXT,
                    DROP COLUMN IF EXISTS imagen_local,
                    DROP COLUMN IF EXISTS imagen_visitante
                    """
                )

                data = {
                    "fecha": fecha.isoformat(),
                    "hora": hora,
                    "equipos": equipos,
                    "competicion": competicion,
                    "canales": canales or [],
                    "categoria": categoria,
                    "imagen_evento": imagen_evento,
                    "subtitulo_competicion": subtitulo_competicion,
                }

                existing = await conn.fetchrow(
                    "SELECT id FROM calendario WHERE fecha = $1 AND hora = $2 AND equipos = $3",
                    fecha.isoformat(),
                    hora,
                    equipos,
                )

                if existing:
                    await conn.execute(
                        """
                        UPDATE calendario
                        SET hora = $1, competicion = $2, canales = $3, categoria = $4,
                            imagen_evento = $5, subtitulo_competicion = $6
                        WHERE id = $7
                        """,
                        hora,
                        competicion,
                        canales or [],
                        categoria,
                        imagen_evento,
                        subtitulo_competicion,
                        existing["id"],
                    )
                else:
                    await conn.execute(
                        """
                        INSERT INTO calendario (
                            fecha, hora, equipos, competicion, canales, categoria,
                            imagen_evento, subtitulo_competicion
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        """,
                        fecha.isoformat(),
                        hora,
                        equipos,
                        competicion,
                        canales or [],
                        categoria,
                        imagen_evento,
                        subtitulo_competicion,
                    )

                return True
        except Exception as e:
            print(f"❌ Error guardando partido '{equipos}': {e}")
            return False

    @staticmethod
    async def get_partidos_by_fecha(fecha: date) -> list[dict]:
        """Obtiene partidos de una fecha específica"""
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT * FROM calendario WHERE fecha = $1 ORDER BY hora", fecha.isoformat()
                )
                return [dict(row) for row in rows]
        except Exception as e:
            print(f"❌ Error obteniendo partidos de {fecha}: {e}")
            return []

    @staticmethod
    async def get_partidos_with_channels(fecha: date) -> list[dict]:
        """Obtiene partidos con canales resueltos usando la función SQL"""
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT * FROM get_eventos_fecha_con_channels($1)", fecha.isoformat()
                )
                return [dict(row) for row in rows]
        except Exception as e:
            print(f"❌ Error obteniendo partidos con canales: {e}")
            return []


class ReplayManager:
    """
    Gestor de replays UFC y otras repeticiones externas
    """

    @staticmethod
    async def upsert_replays(replays: list[dict[str, Any]]) -> int:
        """
        Inserta o actualiza replays usando el slug como clave única.
        """
        if not replays:
            return 0

        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn, conn.transaction():
                inserted = 0
                for replay in replays:
                    event_date = replay.get("event_date")
                    if isinstance(event_date, str):
                        try:
                            event_date = datetime.strptime(event_date[:10], "%Y-%m-%d").date()
                        except (ValueError, TypeError):
                            event_date = None

                    result = await conn.fetchrow(
                        """
                            INSERT INTO replays (
                                slug, source_site, title, event_name, event_type,
                                event_date, post_url, featured_image_url, description,
                                video_sources, match_card
                            )
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                            ON CONFLICT (slug) DO UPDATE SET
                                source_site = EXCLUDED.source_site,
                                title = EXCLUDED.title,
                                event_name = EXCLUDED.event_name,
                                event_type = EXCLUDED.event_type,
                                event_date = EXCLUDED.event_date,
                                post_url = EXCLUDED.post_url,
                                featured_image_url = EXCLUDED.featured_image_url,
                                description = EXCLUDED.description,
                                video_sources = EXCLUDED.video_sources,
                                match_card = EXCLUDED.match_card
                            RETURNING slug
                            """,
                        replay.get("slug"),
                        replay.get("source_site"),
                        replay.get("title"),
                        replay.get("event_name"),
                        replay.get("event_type"),
                        event_date,
                        replay.get("post_url"),
                        replay.get("featured_image_url"),
                        replay.get("description"),
                        json.dumps(replay.get("video_sources", [])),
                        replay.get("match_card"),
                    )
                    if result:
                        inserted += 1
                return inserted
        except Exception as e:
            print(f"❌ Error guardando replays: {e}")
            return 0


class DataManagerSupabase:
    """Gestor de calendario — interfaz legacy usada por scrapper.py."""

    @staticmethod
    def guardar_calendario_sync(eventos: dict, fecha_str: str) -> bool:
        import asyncio

        return asyncio.run(DataManagerSupabase.guardar_calendario_async(eventos, fecha_str))

    @staticmethod
    async def guardar_calendario_async(eventos: dict, fecha_str: str) -> bool:
        try:
            fecha = datetime.strptime(fecha_str, "%d/%m/%Y").date()
        except Exception:
            fecha = date.today()

        partidos_validos = [p for p in eventos.values() if isinstance(p, dict)]
        if not partidos_validos:
            return True

        pool = await DatabasePG.get_pool()
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute("DELETE FROM calendario WHERE fecha = $1", fecha)
            for partido in partidos_validos:
                await conn.execute(
                    """
                        INSERT INTO calendario (
                            fecha, hora, equipos, competicion, canales, categoria,
                            imagen_evento, subtitulo_competicion
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        ON CONFLICT (fecha, hora, equipos) DO UPDATE SET
                            hora = EXCLUDED.hora,
                            competicion = EXCLUDED.competicion,
                            canales = EXCLUDED.canales,
                            categoria = EXCLUDED.categoria,
                            imagen_evento = EXCLUDED.imagen_evento,
                            subtitulo_competicion = EXCLUDED.subtitulo_competicion
                        """,
                    fecha,
                    partido.get("hora", "00:00"),
                    partido.get("equipos", ""),
                    partido.get("competicion", ""),
                    partido.get("canales", []),
                    partido.get("categoria", ""),
                    partido.get("imagen_evento", ""),
                    partido.get("subtitulo_competicion", ""),
                )
        return True

    @staticmethod
    def guardar_calendario(eventos: dict, fecha_str: str) -> bool:
        return DataManagerSupabase.guardar_calendario_sync(eventos, fecha_str)


__all__ = [
    "CalendarioAcestreamManager",
    "ChannelMappingManager",
    "ConfigManager",
    "DataManagerSupabase",
    "DatabasePG",
    "ReplayManager",
]
