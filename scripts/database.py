"""
Módulo de base de datos PostgreSQL.
F3a: iptv_db.engine interno. F3d4a: todas las clases migradas a iptv-db.
asyncpg pool eliminado en F3d4b (0 callers externos).
"""

import os
import pathlib
from datetime import date, datetime

from iptv_db.engine import (
    build_url,
    get_async_engine,
    get_async_session_factory,
    get_sync_engine,
    get_sync_session_factory,
)
from iptv_db.models import Config
from sqlalchemy import select, text

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
    F3d4a: iptv-db engines (async + sync).
    """

    _engine = None  # iptv-db async engine
    _session_factory = None  # iptv-db async session factory
    _sync_engine = None  # iptv-db sync engine
    _sync_session_factory = None  # iptv-db sync session factory
    _host: str | None = None
    _port: int | None = None
    _user: str | None = None
    _password: str | None = None
    _database: str | None = None

    @classmethod
    async def initialize(cls) -> None:
        """Inicializa los engines iptv-db."""
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
            url_async = build_url(
                host=cls._host,
                port=cls._port,
                database=cls._database,
                user=cls._user,
                password=cls._password,
                async_driver=True,
            )
            cls._engine = get_async_engine(url_async, pool_size=5, max_overflow=15)
            cls._session_factory = get_async_session_factory(cls._engine)

            url_sync = build_url(
                host=cls._host,
                port=cls._port,
                database=cls._database,
                user=cls._user,
                password=cls._password,
                async_driver=False,
            )
            cls._sync_engine = get_sync_engine(url_sync, pool_size=5, max_overflow=15)
            cls._sync_session_factory = get_sync_session_factory(cls._sync_engine)

            print("🔥 PostgreSQL inicializado correctamente")
        except Exception as e:
            print(f"❌ Error al conectar con PostgreSQL: {e}")
            raise

    @classmethod
    def get_session_factory(cls):
        """Devuelve el async session factory de iptv-db."""
        if cls._session_factory is None:
            raise RuntimeError("DatabasePG no inicializado. Llama a initialize() primero.")
        return cls._session_factory

    @classmethod
    def get_sync_session_factory(cls):
        """Devuelve el sync session factory de iptv-db. NUEVO en F3d4a."""
        if cls._sync_session_factory is None:
            raise RuntimeError("DatabasePG no inicializado. Llama a initialize() primero.")
        return cls._sync_session_factory

    @classmethod
    async def close(cls):
        """Dispose los engines iptv-db."""
        if cls._engine is not None:
            await cls._engine.dispose()
            cls._engine = None
            cls._session_factory = None
        if cls._sync_engine is not None:
            cls._sync_engine.dispose()
            cls._sync_engine = None
            cls._sync_session_factory = None

    @classmethod
    def reset(cls):
        """Resetea la instancia (útil para testing)."""
        cls._engine = None
        cls._session_factory = None
        cls._sync_engine = None
        cls._sync_session_factory = None


class ConfigManager:
    """
    Gestor de configuración usando la tabla config de PostgreSQL.
    F3d4a: migrado a iptv-db async.
    """

    @staticmethod
    async def get_config(key: str) -> str | None:
        """Obtiene un valor de configuración por su key"""
        try:
            factory = DatabasePG.get_session_factory()
            async with factory() as session:
                stmt = select(Config.value).where(Config.key == key)
                result = await session.execute(stmt)
                row = result.one_or_none()
                return str(row.value) if row else None
        except Exception as e:
            print(f"❌ Error obteniendo config '{key}': {e}")
            return None


class ChannelMappingManager:
    """
    Gestor de mapeos simplificado.
    Tablas: channel_mappings + channel_variants.
    F3d4a: migrado a iptv-db async.
    """

    @staticmethod
    async def upsert_mapping(
        source_name: str,
        display_name: str,
        channel_ids: list[str] = None,
        qualities: list[str] = None,
    ) -> int | None:
        """Inserta o actualiza un mapeo completo con sus variantes"""
        try:
            factory = DatabasePG.get_session_factory()
            async with factory() as session:
                result = await session.execute(
                    text("""
                        INSERT INTO channel_mappings (source_name, display_name)
                        VALUES (:source_name, :display_name)
                        ON CONFLICT (source_name) DO UPDATE
                        SET display_name = EXCLUDED.display_name
                        RETURNING id
                    """),
                    {"source_name": source_name, "display_name": display_name},
                )
                row = result.one_or_none()
                if not row:
                    return None

                mapping_id = row[0]

                if channel_ids:
                    await session.execute(
                        text("DELETE FROM channel_variants WHERE mapping_id = :mid"),
                        {"mid": mapping_id},
                    )

                    for i, channel_id in enumerate(channel_ids):
                        quality = qualities[i] if qualities and i < len(qualities) else "HD"
                        await session.execute(
                            text("""
                                INSERT INTO channel_variants (mapping_id, channel_id, quality, priority)
                                VALUES (:mid, :cid, :qual, :pri)
                            """),
                            {"mid": mapping_id, "cid": channel_id, "qual": quality, "pri": i},
                        )

                await session.commit()
                return mapping_id

        except Exception as e:
            print(f"❌ Error guardando mapeo '{source_name}': {e}")
            return None

    @staticmethod
    async def get_mapping_by_source(source_name: str) -> dict | None:
        """Obtiene un mapeo por su nombre de origen (futbolenlatv)"""
        try:
            factory = DatabasePG.get_session_factory()
            async with factory() as session:
                rows = (
                    (
                        await session.execute(
                            text("SELECT * FROM channel_mappings WHERE source_name = :sn"),
                            {"sn": source_name},
                        )
                    )
                    .mappings()
                    .all()
                )
                if not rows:
                    return None
                mapping = dict(rows[0])
                variants = (
                    (
                        await session.execute(
                            text(
                                "SELECT * FROM channel_variants WHERE mapping_id = :mid ORDER BY priority"
                            ),
                            {"mid": mapping["id"]},
                        )
                    )
                    .mappings()
                    .all()
                )
                mapping["variants"] = [dict(v) for v in variants]
                return mapping
        except Exception as e:
            print(f"❌ Error obteniendo mapeo '{source_name}': {e}")
            return None

    @staticmethod
    async def get_channel_ids_from_source(source_name: str) -> list[str]:
        """Obtiene lista de channel_ids desde un nombre de origen"""
        try:
            factory = DatabasePG.get_session_factory()
            async with factory() as session:
                row = (
                    (
                        await session.execute(
                            text("SELECT id FROM channel_mappings WHERE source_name = :sn"),
                            {"sn": source_name},
                        )
                    )
                    .mappings()
                    .first()
                )
                if not row:
                    return []
                variants = (
                    (
                        await session.execute(
                            text(
                                "SELECT channel_id FROM channel_variants WHERE mapping_id = :mid ORDER BY priority"
                            ),
                            {"mid": row["id"]},
                        )
                    )
                    .mappings()
                    .all()
                )
                return [v["channel_id"] for v in variants if v.get("channel_id")]
        except Exception as e:
            print(f"❌ Error obteniendo channel_ids para '{source_name}': {e}")
            return []

    @staticmethod
    async def get_all_mappings() -> list[dict]:
        """Obtiene todos los mapeos con sus variantes"""
        try:
            factory = DatabasePG.get_session_factory()
            async with factory() as session:
                rows = (
                    (
                        await session.execute(
                            text("SELECT * FROM channel_mappings"),
                        )
                    )
                    .mappings()
                    .all()
                )
                if not rows:
                    return []
                mappings = [dict(row) for row in rows]
                for mapping in mappings:
                    variants = (
                        (
                            await session.execute(
                                text(
                                    "SELECT * FROM channel_variants WHERE mapping_id = :mid ORDER BY priority"
                                ),
                                {"mid": mapping["id"]},
                            )
                        )
                        .mappings()
                        .all()
                    )
                    mapping["variants"] = [dict(v) for v in variants]
                return mappings
        except Exception as e:
            print(f"❌ Error obteniendo mapeos: {e}")
            return []

    @staticmethod
    async def get_all_mappings_simple() -> dict[str, str]:
        """Obtiene mapeo simple: source_name -> display_name"""
        try:
            factory = DatabasePG.get_session_factory()
            async with factory() as session:
                rows = (
                    (
                        await session.execute(
                            text("SELECT source_name, display_name FROM channel_mappings"),
                        )
                    )
                    .mappings()
                    .all()
                )
                return {row["source_name"]: row["display_name"] for row in rows} if rows else {}
        except Exception as e:
            print(f"❌ Error obteniendo mapeos simples: {e}")
            return {}

    @staticmethod
    async def get_all_mappings_with_channels() -> dict[str, list[str]]:
        """
        Obtiene mapeo completo: source_name -> [channel_id, ...]
        Filtra variantes con estado_stream = 'error'.
        Si todas las variantes están muertas, incluye todas (fallback).
        """
        try:
            factory = DatabasePG.get_session_factory()
            async with factory() as session:
                rows = (
                    (
                        await session.execute(
                            text("""
                        SELECT cm.source_name, cv.channel_id, cv.priority, ch.estado_stream
                        FROM channel_mappings cm
                        LEFT JOIN channel_variants cv ON cm.id = cv.mapping_id
                        LEFT JOIN channels ch ON cv.channel_id = ch.id
                        ORDER BY cm.source_name, cv.priority
                    """),
                        )
                    )
                    .mappings()
                    .all()
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
            factory = DatabasePG.get_session_factory()
            async with factory() as session:
                await session.execute(
                    text("""
                    ALTER TABLE channels
                    ADD COLUMN IF NOT EXISTS estado_stream TEXT,
                    ADD COLUMN IF NOT EXISTS ultimo_chequeo TIMESTAMPTZ,
                    ADD COLUMN IF NOT EXISTS tiempo_respuesta_ms INTEGER
                """)
                )
                await session.commit()
        except Exception as e:
            print(f"⚠️ Error añadiendo columnas health check: {e}")

    @staticmethod
    async def update_channel_health(channel_id: str, estado: str, tiempo_ms: int = 0):
        """Actualiza estado de salud de un canal"""
        try:
            factory = DatabasePG.get_session_factory()
            async with factory() as session:
                await session.execute(
                    text("""
                        UPDATE channels
                        SET estado_stream = :estado, tiempo_respuesta_ms = :ms, ultimo_chequeo = NOW()
                        WHERE id = :cid
                    """),
                    {"estado": estado, "ms": tiempo_ms, "cid": channel_id},
                )
                await session.commit()
        except Exception:
            pass

    @staticmethod
    async def get_variants_for_source_names(source_names: list[str]) -> dict[str, list[dict]]:
        """Obtiene variantes con stream_url para una lista de source_names."""
        if not source_names:
            return {}
        try:
            factory = DatabasePG.get_session_factory()
            async with factory() as session:
                rows = (
                    (
                        await session.execute(
                            text("""
                        SELECT cm.source_name, cv.channel_id, cv.quality, cv.priority,
                               ch.stream_url
                        FROM channel_mappings cm
                        INNER JOIN channel_variants cv ON cm.id = cv.mapping_id
                        LEFT JOIN channels ch ON cv.channel_id = ch.id
                        WHERE cm.source_name = ANY(:sns)
                        ORDER BY cm.source_name, cv.priority
                    """),
                            {"sns": list(source_names)},
                        )
                    )
                    .mappings()
                    .all()
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
    Gestor de calendario de acestream. F3d4a: migrado a iptv-db async.
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
            factory = DatabasePG.get_session_factory()
            async with factory() as session:
                await session.execute(
                    text("""
                        ALTER TABLE calendario
                        ADD COLUMN IF NOT EXISTS imagen_evento TEXT,
                        ADD COLUMN IF NOT EXISTS subtitulo_competicion TEXT,
                        DROP COLUMN IF EXISTS imagen_local,
                        DROP COLUMN IF EXISTS imagen_visitante
                    """),
                )

                existing = (
                    (
                        await session.execute(
                            text(
                                "SELECT id FROM calendario WHERE fecha = :fec AND hora = :h AND equipos = :eq"
                            ),
                            {"fec": fecha.isoformat(), "h": hora, "eq": equipos},
                        )
                    )
                    .mappings()
                    .first()
                )

                if existing:
                    await session.execute(
                        text("""
                            UPDATE calendario
                            SET hora = :h, competicion = :comp, canales = :can, categoria = :cat,
                                imagen_evento = :img, subtitulo_competicion = :sub
                            WHERE id = :id
                        """),
                        {
                            "h": hora,
                            "comp": competicion,
                            "can": canales or [],
                            "cat": categoria,
                            "img": imagen_evento,
                            "sub": subtitulo_competicion,
                            "id": existing["id"],
                        },
                    )
                else:
                    await session.execute(
                        text("""
                            INSERT INTO calendario (
                                fecha, hora, equipos, competicion, canales, categoria,
                                imagen_evento, subtitulo_competicion
                            )
                            VALUES (:fec, :h, :eq, :comp, :can, :cat, :img, :sub)
                        """),
                        {
                            "fec": fecha.isoformat(),
                            "h": hora,
                            "eq": equipos,
                            "comp": competicion,
                            "can": canales or [],
                            "cat": categoria,
                            "img": imagen_evento,
                            "sub": subtitulo_competicion,
                        },
                    )

                await session.commit()
                return True
        except Exception as e:
            print(f"❌ Error guardando partido '{equipos}': {e}")
            return False

    @staticmethod
    async def get_partidos_by_fecha(fecha: date) -> list[dict]:
        """Obtiene partidos de una fecha específica"""
        try:
            factory = DatabasePG.get_session_factory()
            async with factory() as session:
                rows = (
                    (
                        await session.execute(
                            text("SELECT * FROM calendario WHERE fecha = :fec ORDER BY hora"),
                            {"fec": fecha.isoformat()},
                        )
                    )
                    .mappings()
                    .all()
                )
                return [dict(row) for row in rows]
        except Exception as e:
            print(f"❌ Error obteniendo partidos de {fecha}: {e}")
            return []

    @staticmethod
    async def get_partidos_with_channels(fecha: date) -> list[dict]:
        """Obtiene partidos con canales resueltos usando la función SQL"""
        try:
            factory = DatabasePG.get_session_factory()
            async with factory() as session:
                rows = (
                    (
                        await session.execute(
                            text("SELECT * FROM get_eventos_fecha_con_channels(:fec)"),
                            {"fec": fecha.isoformat()},
                        )
                    )
                    .mappings()
                    .all()
                )
                return [dict(row) for row in rows]
        except Exception as e:
            print(f"❌ Error obteniendo partidos con canales: {e}")
            return []


class DataManagerSupabase:
    """Gestor de calendario. F3d4a: migrado a iptv-db sync."""

    @staticmethod
    def guardar_calendario(eventos: dict, fecha_str: str) -> bool:
        """Guarda calendario usando sync session (llamado desde scrapper sync)."""
        try:
            try:
                fecha = datetime.strptime(fecha_str, "%d/%m/%Y").date()
            except Exception:
                fecha = date.today()

            partidos_validos = [p for p in eventos.values() if isinstance(p, dict)]
            if not partidos_validos:
                return True

            factory = DatabasePG.get_sync_session_factory()
            with factory() as session:
                session.execute(
                    text("DELETE FROM calendario WHERE fecha = :fec"),
                    {"fec": fecha},
                )
                for partido in partidos_validos:
                    session.execute(
                        text("""
                            INSERT INTO calendario (
                                fecha, hora, equipos, competicion, canales, categoria,
                                imagen_evento, subtitulo_competicion
                            )
                            VALUES (:fec, :h, :eq, :comp, :can, :cat, :img, :sub)
                            ON CONFLICT (fecha, hora, equipos) DO UPDATE SET
                                hora = EXCLUDED.hora,
                                competicion = EXCLUDED.competicion,
                                canales = EXCLUDED.canales,
                                categoria = EXCLUDED.categoria,
                                imagen_evento = EXCLUDED.imagen_evento,
                                subtitulo_competicion = EXCLUDED.subtitulo_competicion
                        """),
                        {
                            "fec": fecha,
                            "h": partido.get("hora", "00:00"),
                            "eq": partido.get("equipos", ""),
                            "comp": partido.get("competicion", ""),
                            "can": partido.get("canales", []),
                            "cat": partido.get("categoria", ""),
                            "img": partido.get("imagen_evento", ""),
                            "sub": partido.get("subtitulo_competicion", ""),
                        },
                    )
                session.commit()
            return True
        except Exception as e:
            print(f"❌ Error guardando calendario: {e}")
            return False


__all__ = [
    "CalendarioAcestreamManager",
    "ChannelMappingManager",
    "ConfigManager",
    "DataManagerSupabase",
    "DatabasePG",
]
