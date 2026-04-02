"""
Módulo de base de datos usando PostgreSQL con asyncpg
Esquema: channel_mappings y channel_variants (reemplazan 4 tablas antiguas)
"""
import json
import os
import pathlib
from datetime import datetime, date
from typing import Dict, Optional, Any, List
from uuid import UUID

import asyncpg
from asyncpg import Pool

try:
    from dotenv import load_dotenv
    env_path = pathlib.Path(__file__).parent.parent / 'docker' / '.env'
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    print("⚠️  python-dotenv no instalado, usando solo variables de entorno del sistema")


class DatabasePG:
    """
    Cliente singleton de PostgreSQL usando asyncpg
    """
    _pool: Optional[Pool] = None
    _host: Optional[str] = None
    _port: Optional[int] = None
    _user: Optional[str] = None
    _password: Optional[str] = None
    _database: Optional[str] = None

    @classmethod
    async def initialize(cls) -> Pool:
        """Inicializa el pool de conexiones PostgreSQL"""
        if cls._pool is not None:
            return cls._pool

        cls._host = os.getenv('PG_HOST')
        cls._port = int(os.getenv('PG_PORT', '5432'))
        cls._user = os.getenv('PG_USER')
        cls._password = os.getenv('PG_PASSWORD')
        cls._database = os.getenv('PG_DATABASE', 'postgres')

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
        """Obtiene el pool de conexiones (inicializa si es necesario)"""
        if cls._pool is None:
            await cls.initialize()
        return cls._pool

    @classmethod
    async def close(cls):
        """Cierra el pool de conexiones"""
        if cls._pool is not None:
            await cls._pool.close()
            cls._pool = None

    @classmethod
    def reset(cls):
        """Resetea la instancia (útil para testing)"""
        cls._pool = None


class ConfigManager:
    """
    Gestor de configuración usando la tabla config de PostgreSQL
    """

    @staticmethod
    async def get_config(key: str) -> Optional[str]:
        """Obtiene un valor de configuración por su key"""
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                result = await conn.fetchrow(
                    "SELECT value FROM config WHERE key = $1",
                    key
                )
                if result:
                    return result['value']
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
    def get_all_mappings_with_channels_sync() -> Dict[str, List[str]]:
        """Versión sincronica para compatibilidad con código sync"""
        import asyncio
        return asyncio.run(ChannelMappingManager.get_all_mappings_with_channels())

    @staticmethod
    async def upsert_mapping(
        source_name: str,
        display_name: str,
        channel_ids: List[str] = None,
        qualities: List[str] = None
    ) -> Optional[int]:
        """
        Inserta o actualiza un mapeo completo con sus variantes
        """
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                async with conn.transaction():
                    mapping_data = {
                        'source_name': source_name,
                        'display_name': display_name
                    }

                    result = await conn.fetchrow(
                        """
                        INSERT INTO channel_mappings (source_name, display_name)
                        VALUES ($1, $2)
                        ON CONFLICT (source_name) DO UPDATE
                        SET display_name = EXCLUDED.display_name
                        RETURNING id
                        """,
                        source_name, display_name
                    )

                    if not result:
                        return None

                    mapping_id = result['id']

                    if channel_ids:
                        await conn.execute(
                            "DELETE FROM channel_variants WHERE mapping_id = $1",
                            mapping_id
                        )

                        for i, channel_id in enumerate(channel_ids):
                            quality = qualities[i] if qualities and i < len(qualities) else 'HD'
                            await conn.execute(
                                """
                                INSERT INTO channel_variants (mapping_id, channel_id, quality, priority)
                                VALUES ($1, $2, $3, $4)
                                """,
                                mapping_id, channel_id, quality, i
                            )

                    return mapping_id

        except Exception as e:
            print(f"❌ Error guardando mapeo '{source_name}': {e}")
            return None

    @staticmethod
    async def get_mapping_by_source(source_name: str) -> Optional[Dict]:
        """Obtiene un mapeo por su nombre de origen (futbolenlatv)"""
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                result = await conn.fetchrow(
                    "SELECT * FROM channel_mappings WHERE source_name = $1",
                    source_name
                )

                if result:
                    mapping = dict(result)
                    variants = await conn.fetch(
                        "SELECT * FROM channel_variants WHERE mapping_id = $1 ORDER BY priority",
                        mapping['id']
                    )
                    mapping['variants'] = [dict(v) for v in variants]
                    return mapping
                return None
        except Exception as e:
            print(f"❌ Error obteniendo mapeo '{source_name}': {e}")
            return None

    @staticmethod
    async def get_channel_ids_from_source(source_name: str) -> List[str]:
        """Obtiene lista de channel_ids desde un nombre de origen"""
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                result = await conn.fetchrow(
                    "SELECT id FROM channel_mappings WHERE source_name = $1",
                    source_name
                )

                if not result:
                    return []

                variants = await conn.fetch(
                    "SELECT channel_id FROM channel_variants WHERE mapping_id = $1 ORDER BY priority",
                    result['id']
                )

                return [v['channel_id'] for v in variants if v['channel_id']]
        except Exception as e:
            print(f"❌ Error obteniendo channel_ids para '{source_name}': {e}")
            return []

    @staticmethod
    async def get_all_mappings() -> List[Dict]:
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
                        mapping['id']
                    )
                    mapping['variants'] = [dict(v) for v in variants]

                return mappings
        except Exception as e:
            print(f"❌ Error obteniendo mapeos: {e}")
            return []

    @staticmethod
    async def get_all_mappings_simple() -> Dict[str, str]:
        """
        Obtiene mapeo simple: source_name -> display_name
        """
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT source_name, display_name FROM channel_mappings"
                )

                if rows:
                    return {row['source_name']: row['display_name'] for row in rows}
                return {}
        except Exception as e:
            print(f"❌ Error obteniendo mapeos simples: {e}")
            return {}

    @staticmethod
    async def get_all_mappings_with_channels() -> Dict[str, List[str]]:
        """
        Obtiene mapeo completo: source_name -> [channel_id, channel_id, ...]
        """
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT cm.source_name, cv.channel_id
                    FROM channel_mappings cm
                    LEFT JOIN channel_variants cv ON cm.id = cv.mapping_id
                    ORDER BY cm.source_name, cv.priority
                    """
                )

                mappings: Dict[str, List[str]] = {}
                for row in rows:
                    source_name = row['source_name']
                    channel_id = row['channel_id']
                    if source_name not in mappings:
                        mappings[source_name] = []
                    if channel_id:
                        mappings[source_name].append(channel_id)

                return mappings
        except Exception as e:
            print(f"❌ Error obteniendo mapeos con canales: {e}")
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
        canales: List[str] = None,
        categoria: str = None
    ) -> bool:
        """Inserta o actualiza un partido del calendario"""
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                data = {
                    'fecha': fecha.isoformat(),
                    'hora': hora,
                    'equipos': equipos,
                    'competicion': competicion,
                    'canales': canales or [],
                    'categoria': categoria
                }

                existing = await conn.fetchrow(
                    "SELECT id FROM calendario WHERE fecha = $1 AND hora = $2 AND equipos = $3",
                    fecha.isoformat(), hora, equipos
                )

                if existing:
                    await conn.execute(
                        """
                        UPDATE calendario
                        SET hora = $1, competicion = $2, canales = $3, categoria = $4
                        WHERE id = $5
                        """,
                        hora, competicion, canales or [], categoria, existing['id']
                    )
                else:
                    await conn.execute(
                        """
                        INSERT INTO calendario (fecha, hora, equipos, competicion, canales, categoria)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        """,
                        fecha.isoformat(), hora, equipos, competicion, canales or [], categoria
                    )

                return True
        except Exception as e:
            print(f"❌ Error guardando partido '{equipos}': {e}")
            return False

    @staticmethod
    async def get_partidos_by_fecha(fecha: date) -> List[Dict]:
        """Obtiene partidos de una fecha específica"""
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT * FROM calendario WHERE fecha = $1 ORDER BY hora",
                    fecha.isoformat()
                )
                return [dict(row) for row in rows]
        except Exception as e:
            print(f"❌ Error obteniendo partidos de {fecha}: {e}")
            return []

    @staticmethod
    async def get_partidos_with_channels(fecha: date) -> List[Dict]:
        """Obtiene partidos con canales resueltos usando la función SQL"""
        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT * FROM get_eventos_fecha_con_channels($1)",
                    fecha.isoformat()
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
    async def upsert_replays(replays: List[Dict[str, Any]]) -> int:
        """
        Inserta o actualiza replays usando el slug como clave única.
        """
        if not replays:
            return 0

        try:
            pool = await DatabasePG.get_pool()
            async with pool.acquire() as conn:
                async with conn.transaction():
                    inserted = 0
                    for replay in replays:
                        event_date = replay.get('event_date')
                        if isinstance(event_date, str):
                            try:
                                event_date = datetime.strptime(event_date[:10], '%Y-%m-%d').date()
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
                            replay.get('slug'),
                            replay.get('source_site'),
                            replay.get('title'),
                            replay.get('event_name'),
                            replay.get('event_type'),
                            event_date,
                            replay.get('post_url'),
                            replay.get('featured_image_url'),
                            replay.get('description'),
                            json.dumps(replay.get('video_sources', [])),
                            replay.get('match_card')
                        )
                        if result:
                            inserted += 1
                    return inserted
        except Exception as e:
            print(f"❌ Error guardando replays: {e}")
            return 0


class Database:
    """
    Clase Database compatible con la interfaz anterior
    """

    def __init__(self, table_name: str, document_name: str, json_document: Optional[str] = None):
        self.table_name = table_name
        self.document_name = document_name
        self.json_document = json_document

    async def add_data_firebase(self) -> bool:
        """Guarda datos manteniendo compatibilidad"""
        try:
            if not self.json_document:
                return False

            data = json.loads(self.json_document)

            if self.table_name == 'mapeo_canales':
                for source_name, info in data.items():
                    if isinstance(info, dict):
                        display_name = info.get('display_name', source_name)
                        channel_ids = info.get('channel_ids', [])
                        qualities = info.get('qualities', [])
                        await ChannelMappingManager.upsert_mapping(
                            source_name, display_name, channel_ids, qualities
                        )
                return True

            elif self.table_name == 'calendario':
                fecha_str = self.document_name.replace('calendario_', '').replace('.', '/')
                try:
                    fecha = datetime.strptime(fecha_str, '%d/%m/%Y').date()
                except:
                    fecha = date.today()

                for key, partido in data.items():
                    if isinstance(partido, dict):
                        await CalendarioAcestreamManager.upsert_partido(
                            fecha=fecha,
                            hora=partido.get('hora', '00:00'),
                            equipos=partido.get('equipos', ''),
                            competicion=partido.get('competicion', ''),
                            canales=partido.get('canales', []),
                            categoria=partido.get('categoria', '')
                        )
                return True

            return False

        except Exception as e:
            print(f"❌ Error en add_data_firebase: {e}")
            return False

    async def get_doc_firebase(self):
        """Obtiene documento manteniendo compatibilidad"""
        from database import PGDocumentSnapshot

        try:
            if self.table_name == 'mapeo_canales':
                mappings = await ChannelMappingManager.get_all_mappings_simple()
                return PGDocumentSnapshot(mappings, exists=True)

            elif self.table_name == 'calendario':
                fecha_str = self.document_name.replace('calendario_', '').replace('.', '/')
                try:
                    fecha = datetime.strptime(fecha_str, '%d/%m/%Y').date()
                except:
                    fecha = date.today()

                partidos = await CalendarioAcestreamManager.get_partidos_by_fecha(fecha)
                data = {}
                for idx, partido in enumerate(partidos, 1):
                    data[str(idx)] = {
                        'hora': partido['hora'],
                        'equipos': partido['equipos'],
                        'competicion': partido['competicion'],
                        'canales': partido['canales'],
                        'categoria': partido.get('categoria', '')
                    }
                return PGDocumentSnapshot(data, exists=len(data) > 0)

            return PGDocumentSnapshot({}, exists=False)

        except Exception as e:
            print(f"❌ Error en get_doc_firebase: {e}")
            return PGDocumentSnapshot({}, exists=False)

    def check_if_document_exist(self) -> bool:
        """Verifica existencia manteniendo compatibilidad"""
        return False


class PGDocumentSnapshot:
    """
    Clase de compatibilidad que simula DocumentSnapshot
    """

    def __init__(self, data: Dict, exists: bool = True):
        self._data = data
        self._exists = exists

    def exists(self) -> bool:
        return self._exists

    def to_dict(self) -> Dict:
        if not self._exists or not self._data:
            return {}
        return dict(self._data)

    def get(self, field: str, default=None):
        if not self._exists:
            return default
        return self._data.get(field, default)


class DataManagerSupabase:
    """
    Gestor de datos con interfaz compatible
    """

    @staticmethod
    def obtener_fechas() -> str:
        return datetime.now().strftime("%d/%m/%Y")

    @staticmethod
    def generate_document_name(prefix: str = "eventos") -> str:
        return f"{prefix}_" + DataManagerSupabase.obtener_fechas().replace("/", ".")

    @staticmethod
    def obtener_mapeo_canales() -> Dict[str, List[str]]:
        """Obtiene mapeo de canales: source_name -> [channel_ids]"""
        return ChannelMappingManager.get_all_mappings_with_channels_sync()

    @staticmethod
    async def obtener_mapeo_web(fuente: str = 'futbolenlatv') -> Dict[str, str]:
        """Obtiene mapeo web: source_name -> display_name"""
        return await ChannelMappingManager.get_all_mappings_simple()

    @staticmethod
    def guardar_calendario_sync(eventos: Dict, fecha_str: str) -> bool:
        """Guarda calendario de partidos (sync)"""
        import asyncio
        return asyncio.run(DataManagerSupabase.guardar_calendario_async(eventos, fecha_str))

    @staticmethod
    async def guardar_calendario_async(eventos: Dict, fecha_str: str) -> bool:
        """Guarda calendario de partidos"""
        try:
            fecha = datetime.strptime(fecha_str, '%d/%m/%Y').date()
        except:
            fecha = date.today()

        for key, partido in eventos.items():
            if isinstance(partido, dict):
                await CalendarioAcestreamManager.upsert_partido(
                    fecha=fecha,
                    hora=partido.get('hora', '00:00'),
                    equipos=partido.get('equipos', ''),
                    competicion=partido.get('competicion', ''),
                    canales=partido.get('canales', []),
                    categoria=partido.get('categoria', '')
                )
        return True

    @staticmethod
    def guardar_calendario(eventos: Dict, fecha_str: str) -> bool:
        """Wrapper sync para guardar_calendario_async"""
        return DataManagerSupabase.guardar_calendario_sync(eventos, fecha_str)

    @staticmethod
    async def obtener_calendario(fecha_str: str) -> Dict:
        """Obtiene calendario de partidos"""
        try:
            fecha = datetime.strptime(fecha_str, '%d/%m/%Y').date()
        except:
            fecha = date.today()

        partidos = await CalendarioAcestreamManager.get_partidos_by_fecha(fecha)
        data = {}
        for idx, partido in enumerate(partidos, 1):
            data[str(idx)] = {
                'hora': partido['hora'],
                'equipos': partido['equipos'],
                'competicion': partido['competicion'],
                'canales': partido['canales'],
                'categoria': partido.get('categoria', '')
            }
        return data


__all__ = [
    'DatabasePG',
    'ConfigManager',
    'ChannelMappingManager',
    'CalendarioAcestreamManager',
    'ReplayManager',
    'Database',
    'DataManagerSupabase',
    'PGDocumentSnapshot',
    'ChannelMappingManager',
]
