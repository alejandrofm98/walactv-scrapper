"""
Módulo de base de datos usando Supabase con esquema relacional normalizado
Reemplaza el enfoque JSON por tablas relacionales
"""
import json
import os
import pathlib
from datetime import datetime, date
from typing import Dict, Optional, Any, List
from uuid import UUID

# Cargar variables de entorno desde .env si existe (para desarrollo local)
try:
    from dotenv import load_dotenv

    env_path = pathlib.Path(__file__).parent.parent / 'docker' / '.env'
    env_loaded = False
    if env_path.exists():
        load_dotenv(env_path)
        env_loaded = True

    if not env_loaded:
        print("ℹ️  No se encontró archivo .env (puede estar en variables de entorno del sistema)")

except ImportError:
    print("⚠️  python-dotenv no instalado, usando solo variables de entorno del sistema")

# Importar Supabase
try:
    from supabase import create_client, Client
except ImportError:
    print("❌ Error: supabase no está instalado. Ejecuta: pip install supabase")
    raise


class SupabaseDB:
    """
    Cliente singleton de Supabase
    """
    _instance: Optional[Client] = None
    _supabase_url: Optional[str] = None
    _supabase_key: Optional[str] = None

    @classmethod
    def initialize(cls) -> Client:
        """Inicializa el cliente de Supabase"""
        if cls._instance is not None:
            return cls._instance

        cls._supabase_url = os.getenv('SUPABASE_URL')
        cls._supabase_key = os.getenv('SUPABASE_KEY')

        if not cls._supabase_url or not cls._supabase_key:
            raise ValueError(
                "❌ No se encontraron las variables de entorno SUPABASE_URL o SUPABASE_KEY.\n"
                "Asegúrate de tener un archivo .env con:\n"
                "SUPABASE_URL=https://tu-proyecto.supabase.co\n"
                "SUPABASE_KEY=tu-api-key"
            )

        try:
            cls._instance = create_client(cls._supabase_url, cls._supabase_key)
            print("🔥 Supabase inicializado correctamente")
            return cls._instance
        except Exception as e:
            print(f"❌ Error al conectar con Supabase: {e}")
            raise

    @classmethod
    def get_client(cls) -> Client:
        """Obtiene el cliente de Supabase (inicializa si es necesario)"""
        if cls._instance is None:
            return cls.initialize()
        return cls._instance

    @classmethod
    def reset(cls):
        """Resetea la instancia (útil para testing)"""
        cls._instance = None


class ConfigManager:
    """
    Gestor de configuración usando la tabla config de Supabase
    Estructura: key (TEXT PRIMARY KEY), value (TEXT), description (TEXT), updated_at (TIMESTAMPTZ)
    """

    @staticmethod
    def get_config(key: str) -> Optional[str]:
        """Obtiene un valor de configuración por su key"""
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('config').select('value').eq('key', key).execute()
            if result.data and len(result.data) > 0:
                return result.data[0]['value']
            return None
        except Exception as e:
            print(f"❌ Error obteniendo config '{key}': {e}")
            return None

    @staticmethod
    def get_proxy_config() -> Dict:
        """Obtiene la configuración del proxy de forma específica"""
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('config').select('key, value').or_(
                'key.eq.PROXY_IP,key.eq.PROXY_PORT,key.eq.PROXY_USER,key.eq.PROXY_PASS'
            ).execute()

            config = {}
            if result.data:
                for item in result.data:
                    key = item['key'].lower().replace('proxy_', '')
                    config[key] = item['value']

            return config

        except Exception as e:
            print(f"❌ Error obteniendo config proxy: {e}")
            return {}


class CanalManager:
    """
    Gestor de canales IPTV
    Usa la tabla 'channels' existente
    """

    @staticmethod
    def upsert_canal(id_canal: str, nombre: str, url: str, grupo: str = None, 
                     provider_id: str = None, logo: str = None, 
                     country: str = None, numero: int = None) -> Optional[str]:
        """
        Inserta o actualiza un canal en la tabla 'channels'
        
        Args:
            id_canal: ID único del canal (ej: "dazn1_hd")
            nombre: Nombre del canal
            url: URL del stream
            grupo: Grupo/categoría (ej: "Deportes")
            provider_id: ID del proveedor
            logo: URL del logo
            country: Código de país (ej: "ES")
            numero: Número de canal
        """
        try:
            supabase = SupabaseDB.get_client()
            data = {
                'id': id_canal,
                'nombre': nombre,
                'url': url,
                'grupo': grupo,
                'provider_id': provider_id,
                'logo': logo,
                'country': country,
                'numero': numero
            }
            # Eliminar valores None
            data = {k: v for k, v in data.items() if v is not None}
            
            result = supabase.table('channels').upsert(data, on_conflict='id').execute()
            if result.data:
                return result.data[0]['id']
            return None
        except Exception as e:
            print(f"❌ Error guardando canal '{nombre}': {e}")
            return None

    @staticmethod
    def get_canal_by_id(id_canal: str) -> Optional[Dict]:
        """Obtiene un canal por su ID"""
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('channels').select('*').eq('id', id_canal).execute()
            if result.data and len(result.data) > 0:
                return result.data[0]
            return None
        except Exception as e:
            print(f"❌ Error obteniendo canal '{id_canal}': {e}")
            return None

    @staticmethod
    def get_canal_by_nombre(nombre: str) -> Optional[Dict]:
        """Obtiene un canal por su nombre exacto"""
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('channels').select('*').eq('nombre', nombre).execute()
            if result.data and len(result.data) > 0:
                return result.data[0]
            return None
        except Exception as e:
            print(f"❌ Error obteniendo canal '{nombre}': {e}")
            return None

    @staticmethod
    def get_canales_by_grupo(grupo: str) -> List[Dict]:
        """Obtiene todos los canales de un grupo"""
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('channels').select('*').eq('grupo', grupo).execute()
            return result.data or []
        except Exception as e:
            print(f"❌ Error obteniendo canales del grupo '{grupo}': {e}")
            return []

    @staticmethod
    def get_all_canales() -> List[Dict]:
        """Obtiene todos los canales"""
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('channels').select('*').execute()
            return result.data or []
        except Exception as e:
            print(f"❌ Error obteniendo canales: {e}")
            return []

    @staticmethod
    def bulk_insert_canales(canales: List[Dict]) -> int:
        """
        Inserta múltiples canales a la vez
        Cada canal debe tener al menos: id, nombre, url
        """
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('channels').upsert(canales, on_conflict='id').execute()
            return len(result.data) if result.data else 0
        except Exception as e:
            print(f"❌ Error en bulk insert de canales: {e}")
            return 0


class MapeoCanalesManager:
    """
    Gestor de mapeo de canales con estructura relacionada:
    - canales_walactv: Canal padre referencia a channels (ej: "DAZN 1")
    - canales_calidades: Variaciones con calidades (ej: "ES| DAZN 1 FHD")
    - mapeo_futbolenlatv: Mapeo desde futbolenlatv
    """

    @staticmethod
    def _generate_id(nombre: str) -> str:
        """Genera un ID tipo slug a partir del nombre"""
        import re
        # Eliminar prefijos como "ES|", "UK|", etc.
        id_limpio = re.sub(r'^[A-Z]{2}\|\s*', '', nombre)
        # Convertir a minúsculas y reemplazar caracteres especiales
        id_limpio = id_limpio.lower()
        # Reemplazar espacios y caracteres especiales con guiones bajos
        id_limpio = re.sub(r'[^a-z0-9]+', '_', id_limpio)
        # Eliminar guiones bajos múltiples
        id_limpio = re.sub(r'_+', '_', id_limpio)
        # Eliminar guiones bajos al inicio y final
        return id_limpio.strip('_')

    @staticmethod
    def _extraer_calidad(nombre_iptv: str) -> str:
        """Extrae la calidad del nombre IPTV"""
        import re
        calidades = ['FHD', 'HD', 'SD', '4K', 'UHD', 'RAW', 'LOW', 'HEVC']
        nombre_upper = nombre_iptv.upper()
        for calidad in calidades:
            if calidad in nombre_upper:
                return calidad
        return 'HD'

    # ============ Métodos para tabla canales_walactv ============

    @staticmethod
    def upsert_canal_walactv(nombre: str, channel_id: str = None) -> Optional[int]:
        """
        Inserta o actualiza un canal walactv
        Retorna el ID numérico generado (BIGSERIAL)
        """
        try:
            supabase = SupabaseDB.get_client()
            
            # Verificar si ya existe por nombre
            existing = supabase.table('canales_walactv').select('id').eq('nombre', nombre).execute()
            
            if existing.data and len(existing.data) > 0:
                # Actualizar
                canal_id = existing.data[0]['id']
                data = {'channel_id': channel_id} if channel_id else {}
                if data:
                    supabase.table('canales_walactv').update(data).eq('id', canal_id).execute()
                return canal_id
            else:
                # Insertar nuevo - no especificar id, será autogenerado
                data = {
                    'nombre': nombre,
                    'channel_id': channel_id
                }
                data = {k: v for k, v in data.items() if v is not None}
                
                result = supabase.table('canales_walactv').insert(data).execute()
                
                if result.data:
                    return result.data[0]['id']
                return None
        except Exception as e:
            print(f"❌ Error guardando canal walactv '{nombre}': {e}")
            return None

    @staticmethod
    def get_canal_walactv_id(nombre: str) -> Optional[int]:
        """Obtiene el ID numérico de un canal walactv por su nombre"""
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('canales_walactv').select('id').eq(
                'nombre', nombre
            ).execute()
            
            if result.data and len(result.data) > 0:
                return result.data[0]['id']
            return None
        except Exception as e:
            print(f"❌ Error obteniendo ID de canal walactv '{nombre}': {e}")
            return None

    @staticmethod
    def get_canal_walactv_by_id(canal_id: int) -> Optional[Dict]:
        """Obtiene un canal walactv por su ID numérico"""
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('canales_walactv').select('*').eq('id', canal_id).execute()
            
            if result.data and len(result.data) > 0:
                return result.data[0]
            return None
        except Exception as e:
            print(f"❌ Error obteniendo canal walactv '{canal_id}': {e}")
            return None

    @staticmethod
    def get_all_canales_walactv() -> List[Dict]:
        """Obtiene todos los canales walactv"""
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('canales_walactv').select('*').execute()
            return result.data or []
        except Exception as e:
            print(f"❌ Error obteniendo canales walactv: {e}")
            return []

    # ============ Métodos para tabla canales_calidades ============

    @staticmethod
    def upsert_calidad(canal_walactv_id: int, nombre_iptv: str, 
                       channel_id: str = None, calidad: str = None, orden: int = 0) -> bool:
        """
        Inserta o actualiza una calidad de canal
        """
        try:
            supabase = SupabaseDB.get_client()
            
            # Extraer calidad si no se proporciona
            if not calidad:
                calidad = MapeoCanalesManager._extraer_calidad(nombre_iptv)
            
            # Verificar si ya existe esta combinación
            existing = supabase.table('canales_calidades').select('id').eq(
                'canal_walactv_id', canal_walactv_id
            ).eq('nombre_iptv', nombre_iptv).execute()
            
            data = {
                'canal_walactv_id': canal_walactv_id,
                'nombre_iptv': nombre_iptv,
                'calidad': calidad,
                'orden': orden
            }
            
            if channel_id:
                data['channel_id'] = channel_id
            
            if existing.data and len(existing.data) > 0:
                # Actualizar
                calidad_id = existing.data[0]['id']
                result = supabase.table('canales_calidades').update(data).eq('id', calidad_id).execute()
            else:
                # Insertar nuevo - id autogenerado
                result = supabase.table('canales_calidades').insert(data).execute()
            
            return bool(result.data)
        except Exception as e:
            print(f"❌ Error guardando calidad '{nombre_iptv}': {e}")
            return False

    @staticmethod
    def get_calidades_por_canal(nombre: str) -> List[Dict]:
        """
        Obtiene todas las calidades de un canal walactv
        Retorna: [{"nombre_iptv": "...", "calidad": "...", ...}, ...]
        """
        try:
            supabase = SupabaseDB.get_client()
            # Primero obtener el ID del canal
            canal_id = MapeoCanalesManager.get_canal_walactv_id(nombre)
            if not canal_id:
                return []
            
            # Luego obtener las calidades
            result = supabase.table('canales_calidades').select('*').eq(
                'canal_walactv_id', canal_id
            ).order('orden').execute()
            
            return result.data or []
        except Exception as e:
            print(f"❌ Error obteniendo calidades de '{nombre}': {e}")
            return []

    @staticmethod
    def get_calidades_por_canal_id(canal_walactv_id: int) -> List[Dict]:
        """
        Obtiene todas las calidades de un canal walactv por su ID numérico
        """
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('canales_calidades').select('*').eq(
                'canal_walactv_id', canal_walactv_id
            ).order('orden').execute()
            
            return result.data or []
        except Exception as e:
            print(f"❌ Error obteniendo calidades: {e}")
            return []

    @staticmethod
    def buscar_calidad_por_nombre(nombre_iptv: str) -> Optional[Dict]:
        """
        Busca una calidad por su nombre IPTV exacto
        """
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('canales_calidades').select(
                '*, canales_walactv(nombre)'
            ).eq('nombre_iptv', nombre_iptv).execute()
            
            if result.data and len(result.data) > 0:
                return result.data[0]
            return None
        except Exception as e:
            print(f"❌ Error buscando calidad '{nombre_iptv}': {e}")
            return None

    # ============ Métodos para tabla mapeo_futbolenlatv (relación N:M) ============

    @staticmethod
    def upsert_mapeo_futboltv(nombre_futboltv: str) -> Optional[int]:
        """
        Inserta o actualiza un mapeo desde futbolenlatv
        Retorna el ID numérico del mapeo creado (BIGSERIAL)
        """
        try:
            supabase = SupabaseDB.get_client()
            
            # Verificar si ya existe
            existing = supabase.table('mapeo_futbolenlatv').select('id').eq(
                'nombre_futboltv', nombre_futboltv
            ).execute()
            
            if existing.data and len(existing.data) > 0:
                return existing.data[0]['id']
            
            # Insertar nuevo - id autogenerado
            data = {
                'nombre_futboltv': nombre_futboltv
            }
            result = supabase.table('mapeo_futbolenlatv').insert(data).execute()
            
            if result.data:
                return result.data[0]['id']
            return None
        except Exception as e:
            print(f"❌ Error guardando mapeo futboltv '{nombre_futboltv}': {e}")
            return None

    @staticmethod
    def asociar_canal_a_mapeo(mapeo_futbolenlatv_id: int, canal_walactv_id: int, orden: int = 0) -> bool:
        """
        Asocia un canal walactv a un mapeo de futbolenlatv
        """
        try:
            supabase = SupabaseDB.get_client()
            data = {
                'mapeo_futbolenlatv_id': mapeo_futbolenlatv_id,
                'canal_walactv_id': canal_walactv_id,
                'orden': orden
            }
            result = supabase.table('mapeo_futbolenlatv_canales').upsert(data).execute()
            return bool(result.data)
        except Exception as e:
            print(f"❌ Error asociando canal '{canal_walactv_id}' a mapeo '{mapeo_futbolenlatv_id}': {e}")
            return False

    @staticmethod
    def get_mapeo_futboltv_id(nombre_futboltv: str) -> Optional[int]:
        """
        Obtiene el ID numérico de un mapeo por su nombre
        """
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('mapeo_futbolenlatv').select('id').eq(
                'nombre_futboltv', nombre_futboltv
            ).execute()
            
            if result.data and len(result.data) > 0:
                return result.data[0]['id']
            return None
        except Exception as e:
            print(f"❌ Error obteniendo ID de mapeo '{nombre_futboltv}': {e}")
            return None

    @staticmethod
    def get_canales_walactv_por_futboltv(nombre_futboltv: str) -> List[Dict]:
        """
        Obtiene todos los canales walactv asociados a un nombre de futbolenlatv
        Retorna: [{"id": "...", "nombre": "...", "orden": 0}, ...]
        """
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('mapeo_futbolenlatv_canales').select(
                'canal_walactv_id, orden, canales_walactv(id, nombre)'
            ).eq('mapeo_futbolenlatv.nombre_futboltv', nombre_futboltv).execute()
            
            if result.data:
                return [
                    {
                        'id': item['canales_walactv']['id'],
                        'nombre': item['canales_walactv']['nombre'],
                        'orden': item['orden']
                    }
                    for item in result.data
                ]
            return []
        except Exception as e:
            print(f"❌ Error obteniendo canales para '{nombre_futboltv}': {e}")
            return []

    @staticmethod
    def get_all_mapeos_futboltv() -> Dict[str, List[str]]:
        """
        Obtiene todos los mapeos de futbolenlatv con sus canales asociados
        Retorna: {"DAZN 1 HD": ["dazn_1", "dazn_1_bar"], ...}
        """
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('mapeo_futbolenlatv').select(
                'nombre_futboltv, mapeo_futbolenlatv_canales(canal_walactv_id, canales_walactv(nombre))'
            ).execute()
            
            if result.data:
                mapeos = {}
                for item in result.data:
                    nombre_futboltv = item['nombre_futboltv']
                    canales = [
                        canal['canales_walactv']['nombre']
                        for canal in item['mapeo_futbolenlatv_canales']
                    ]
                    mapeos[nombre_futboltv] = canales
                return mapeos
            return {}
        except Exception as e:
            print(f"❌ Error obteniendo mapeos futboltv: {e}")
            return {}

    @staticmethod
    def resolver_canal_futboltv(nombre_futboltv: str) -> List[Dict]:
        """
        Resolución completa: futbolenlatv -> Calidades IPTV de todos los canales asociados
        Ej: "DAZN 1 HD" -> [{"nombre": "ES| DAZN 1 FHD", "calidad": "FHD"}, ...]
        """
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.rpc('resolver_canal_futboltv', {
                'nombre_futboltv': nombre_futboltv
            }).execute()
            
            return result.data or []
        except Exception as e:
            print(f"❌ Error resolviendo canal '{nombre_futboltv}': {e}")
            return []

    @staticmethod
    def get_all_mapeos_web(fuente: str = 'futbolenlatv') -> Dict[str, str]:
        """
        Obtiene mapeo web: {nombre_web: nombre_comercial}
        """
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('mapeo_futbolenlatv').select(
                'nombre_futboltv, mapeo_futbolenlatv_canales(canales_walactv(nombre))'
            ).execute()
            
            if result.data:
                mapeos = {}
                for item in result.data:
                    nombre_web = item['nombre_futboltv']
                    # Tomar el primer canal asociado como nombre comercial
                    if item.get('mapeo_futbolenlatv_canales'):
                        nombre_comercial = item['mapeo_futbolenlatv_canales'][0]['canales_walactv']['nombre']
                        mapeos[nombre_web] = nombre_comercial
                return mapeos
            return {}
        except Exception as e:
            print(f"❌ Error obteniendo mapeos web: {e}")
            return {}

    # ============ Métodos de conveniencia ============

    @staticmethod
    def importar_mapeo_completo(nombre_comercial: str, variaciones: List[Dict], channel_id: str = None) -> bool:
        """
        Importa un mapeo completo: canal walactv + todas sus calidades
        variaciones: [{"nombre": "ES| DAZN 1 FHD"}, ...]
        """
        try:
            # 1. Crear canal walactv
            canal_id = MapeoCanalesManager.upsert_canal_walactv(nombre_comercial, channel_id)
            if not canal_id:
                return False
            
            # 2. Crear calidades
            for idx, var in enumerate(variaciones):
                if isinstance(var, dict) and 'nombre' in var:
                    nombre_iptv = var['nombre']
                    calidad = var.get('calidad') or MapeoCanalesManager._extraer_calidad(nombre_iptv)
                    
                    MapeoCanalesManager.upsert_calidad(
                        canal_id, 
                        nombre_iptv, 
                        calidad=calidad,
                        orden=idx
                    )
            
            return True
        except Exception as e:
            print(f"❌ Error importando mapeo completo '{nombre_comercial}': {e}")
            return False


class EventoManager:
    """
    Gestor de eventos deportivos
    """

    @staticmethod
    def upsert_evento(fecha: date, hora: str, titulo: str, competicion: str = None, 
                      categoria: str = None) -> Optional[str]:
        """Inserta o actualiza un evento"""
        try:
            supabase = SupabaseDB.get_client()
            data = {
                'fecha': fecha.isoformat(),
                'hora': hora,
                'titulo': titulo,
                'competicion': competicion,
                'categoria': categoria
            }
            # Buscar si existe por fecha+hora+titulo
            existing = supabase.table('eventos').select('id').eq('fecha', fecha.isoformat()).eq('hora', hora).eq('titulo', titulo).execute()
            
            if existing.data and len(existing.data) > 0:
                # Actualizar
                evento_id = existing.data[0]['id']
                result = supabase.table('eventos').update(data).eq('id', evento_id).execute()
                return evento_id
            else:
                # Insertar
                result = supabase.table('eventos').insert(data).execute()
                if result.data:
                    return result.data[0]['id']
            return None
        except Exception as e:
            print(f"❌ Error guardando evento '{titulo}': {e}")
            return None

    @staticmethod
    def add_enlace_evento(evento_id: UUID, channel_id: str, tipo: str = 'acestream', orden: int = 0) -> bool:
        """
        Añade un enlace entre un evento y un canal
        
        Args:
            evento_id: UUID del evento
            channel_id: ID del canal (VARCHAR) de la tabla channels
            tipo: Tipo de enlace (acestream, directo, etc.)
            orden: Orden de preferencia
        """
        try:
            supabase = SupabaseDB.get_client()
            data = {
                'evento_id': str(evento_id),
                'channel_id': channel_id,  # VARCHAR, no UUID
                'tipo': tipo,
                'orden': orden
            }
            result = supabase.table('enlaces_evento').upsert(data).execute()
            return bool(result.data)
        except Exception as e:
            print(f"❌ Error añadiendo enlace: {e}")
            return False

    @staticmethod
    def get_eventos_by_fecha(fecha: date) -> List[Dict]:
        """Obtiene eventos de una fecha específica"""
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('eventos').select('*').eq('fecha', fecha.isoformat()).order('hora').execute()
            return result.data or []
        except Exception as e:
            print(f"❌ Error obteniendo eventos de {fecha}: {e}")
            return []

    @staticmethod
    def get_eventos_con_canales(fecha: date) -> List[Dict]:
        """Obtiene eventos con sus canales usando la función de PostgreSQL"""
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.rpc('get_eventos_con_canales', {'fecha_consulta': fecha.isoformat()}).execute()
            return result.data or []
        except Exception as e:
            print(f"❌ Error obteniendo eventos con canales: {e}")
            return []

    @staticmethod
    def guardar_eventos_completos(eventos_data: Dict, fecha: date = None) -> bool:
        """
        Guarda eventos completos desde el formato antiguo (para compatibilidad)
        eventos_data = {'dia': '...', 'fecha': '...', 'eventos': [{...}]}
        """
        try:
            if fecha is None:
                fecha = date.today()
            
            eventos = eventos_data.get('eventos', [])
            
            for evento in eventos:
                titulo = evento.get('titulo', evento.get('equipos', 'Sin título'))
                hora = evento.get('hora', '00:00')
                competicion = evento.get('competicion', '')
                categoria = evento.get('categoria', '')
                
                # Crear evento
                evento_id = EventoManager.upsert_evento(fecha, hora, titulo, competicion, categoria)
                
                if evento_id:
                    # Añadir enlaces de canales
                    canales = evento.get('canales', [])
                    for idx, canal_nombre in enumerate(canales):
                        canal = CanalManager.get_canal_by_nombre(canal_nombre)
                        if canal:
                            EventoManager.add_enlace_evento(evento_id, canal['id'], orden=idx)
            
            return True
        except Exception as e:
            print(f"❌ Error guardando eventos completos: {e}")
            return False


class CalendarioAcestreamManager:
    """
    Gestor de calendario de acestream (scraping de futbolenlatv)
    """

    @staticmethod
    def upsert_partido(fecha: date, hora: str, equipos: str, competicion: str = None,
                       canales: List[str] = None, acestream_ids: List[str] = None) -> bool:
        """Inserta o actualiza un partido del calendario acestream"""
        try:
            supabase = SupabaseDB.get_client()
            data = {
                'fecha': fecha.isoformat(),
                'hora': hora,
                'equipos': equipos,
                'competicion': competicion,
                'canales': canales or [],
                'acestream_ids': acestream_ids or []
            }
            
            # Buscar si existe
            existing = supabase.table('calendario_acestream').select('id').eq('fecha', fecha.isoformat()).eq('hora', hora).eq('equipos', equipos).execute()
            
            if existing.data and len(existing.data) > 0:
                # Actualizar
                partido_id = existing.data[0]['id']
                result = supabase.table('calendario_acestream').update(data).eq('id', partido_id).execute()
            else:
                # Insertar
                result = supabase.table('calendario_acestream').insert(data).execute()
            
            return bool(result.data)
        except Exception as e:
            print(f"❌ Error guardando partido '{equipos}': {e}")
            return False

    @staticmethod
    def get_partidos_by_fecha(fecha: date) -> List[Dict]:
        """Obtiene partidos de una fecha específica"""
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('calendario_acestream').select('*').eq('fecha', fecha.isoformat()).order('hora').execute()
            return result.data or []
        except Exception as e:
            print(f"❌ Error obteniendo partidos de {fecha}: {e}")
            return []

    @staticmethod
    def buscar_partidos(equipo: str, fecha: date = None) -> List[Dict]:
        """Busca partidos por nombre de equipo"""
        try:
            supabase = SupabaseDB.get_client()
            query = supabase.table('calendario_acestream').select('*')
            
            if fecha:
                query = query.eq('fecha', fecha.isoformat())
            
            # Búsqueda de texto en equipos
            query = query.ilike('equipos', f'%{equipo}%')
            
            result = query.execute()
            return result.data or []
        except Exception as e:
            print(f"❌ Error buscando partidos: {e}")
            return []


# Clases de compatibilidad hacia atrás

class Database:
    """
    Clase Database compatible con la interfaz anterior
    pero usando el nuevo esquema relacional
    """

    def __init__(self, table_name: str, document_name: str, json_document: Optional[str] = None):
        """
        Constructor mantenido por compatibilidad
        """
        self.table_name = table_name
        self.document_name = document_name
        self.json_document = json_document

    def add_data_firebase(self) -> bool:
        """Guarda datos manteniendo compatibilidad"""
        try:
            if not self.json_document:
                return False

            data = json.loads(self.json_document)

            if self.table_name == 'canales':
                # Insertar canales desde items
                items = data.get('items', {})
                canales_list = []
                for nombre, url in items.items():
                    canales_list.append({
                        'nombre': nombre,
                        'url': url,
                        'grupo': 'General'
                    })
                count = CanalManager.bulk_insert_canales(canales_list)
                return count > 0

            elif self.table_name == 'mapeo_canales':
                # Insertar mapeos
                for nombre_comercial, variaciones in data.items():
                    if isinstance(variaciones, list):
                        MapeoCanalesManager.upsert_mapeo(nombre_comercial, variaciones)
                return True

            elif self.table_name == 'eventos_tv':
                # Guardar eventos completos
                fecha_str = data.get('fecha', '')
                try:
                    fecha = datetime.strptime(fecha_str, '%d/%m/%Y').date()
                except:
                    fecha = date.today()
                return EventoManager.guardar_eventos_completos(data, fecha)

            elif self.table_name == 'calendario':
                # Guardar calendario acestream
                fecha_str = self.document_name.replace('calendario_', '').replace('.', '/')
                try:
                    fecha = datetime.strptime(fecha_str, '%d/%m/%Y').date()
                except:
                    fecha = date.today()
                
                for key, partido in data.items():
                    if isinstance(partido, dict):
                        CalendarioAcestreamManager.upsert_partido(
                            fecha=fecha,
                            hora=partido.get('hora', '00:00'),
                            equipos=partido.get('equipos', ''),
                            competicion=partido.get('competicion', ''),
                            canales=partido.get('canales', [])
                        )
                return True

            return False

        except Exception as e:
            print(f"❌ Error en add_data_firebase: {e}")
            return False

    def get_doc_firebase(self) -> 'SupabaseDocumentSnapshot':
        """Obtiene documento manteniendo compatibilidad"""
        try:
            if self.table_name == 'canales':
                canales = CanalManager.get_canales_by_grupo('General')
                items = {c['nombre']: c['url'] for c in canales}
                return SupabaseDocumentSnapshot({'items': items}, exists=True)

            elif self.table_name == 'mapeo_canales':
                mapeos = MapeoCanalesManager.get_all_mapeos()
                return SupabaseDocumentSnapshot(mapeos, exists=True)

            elif self.table_name == 'calendario':
                fecha_str = self.document_name.replace('calendario_', '').replace('.', '/')
                try:
                    fecha = datetime.strptime(fecha_str, '%d/%m/%Y').date()
                except:
                    fecha = date.today()
                
                partidos = CalendarioAcestreamManager.get_partidos_by_fecha(fecha)
                data = {}
                for idx, partido in enumerate(partidos, 1):
                    data[str(idx)] = {
                        'hora': partido['hora'],
                        'equipos': partido['equipos'],
                        'competicion': partido['competicion'],
                        'canales': partido['canales']
                    }
                return SupabaseDocumentSnapshot(data, exists=len(data) > 0)

            return SupabaseDocumentSnapshot({}, exists=False)

        except Exception as e:
            print(f"❌ Error en get_doc_firebase: {e}")
            return SupabaseDocumentSnapshot({}, exists=False)

    def check_if_document_exist(self) -> bool:
        """Verifica existencia manteniendo compatibilidad"""
        snapshot = self.get_doc_firebase()
        return snapshot.exists()


class SupabaseDocumentSnapshot:
    """
    Clase de compatibilidad que simula DocumentSnapshot de Firebase
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
    def guardar_eventos(eventos: Dict) -> bool:
        """Guarda eventos en el nuevo esquema relacional"""
        fecha_str = eventos.get('fecha', '')
        try:
            fecha = datetime.strptime(fecha_str, '%d/%m/%Y').date()
        except:
            fecha = date.today()
        return EventoManager.guardar_eventos_completos(eventos, fecha)

    @staticmethod
    def obtener_mapeo_canales() -> Dict[str, List[Dict]]:
        """Obtiene mapeo de canales IPTV: {nombre_comercial: [{"nombre": "..."}, ...]}"""
        return MapeoCanalesManager.get_all_mapeos_futboltv()

    @staticmethod
    def obtener_mapeo_web(fuente: str = 'futbolenlatv') -> Dict[str, str]:
        """Obtiene mapeo web: {nombre_web: nombre_comercial}"""
        return MapeoCanalesManager.get_all_mapeos_web(fuente)

    @staticmethod
    def obtener_enlaces_canales() -> Dict[str, Dict]:
        """
        Obtiene todos los canales como diccionario
        Retorna: {id_canal: {nombre: "...", url: "...", grupo: "...", ...}}
        """
        canales = CanalManager.get_all_canales()
        return {c['id']: c for c in canales}

    @staticmethod
    def guardar_calendario(eventos: Dict, fecha_str: str) -> bool:
        """Guarda calendario de partidos"""
        try:
            fecha = datetime.strptime(fecha_str, '%d/%m/%Y').date()
        except:
            fecha = date.today()
        
        for key, partido in eventos.items():
            if isinstance(partido, dict):
                CalendarioAcestreamManager.upsert_partido(
                    fecha=fecha,
                    hora=partido.get('hora', '00:00'),
                    equipos=partido.get('equipos', ''),
                    competicion=partido.get('competicion', ''),
                    canales=partido.get('canales', [])
                )
        return True

    @staticmethod
    def obtener_calendario(fecha_str: str) -> Dict:
        """Obtiene calendario de partidos"""
        try:
            fecha = datetime.strptime(fecha_str, '%d/%m/%Y').date()
        except:
            fecha = date.today()
        
        partidos = CalendarioAcestreamManager.get_partidos_by_fecha(fecha)
        data = {}
        for idx, partido in enumerate(partidos, 1):
            data[str(idx)] = {
                'hora': partido['hora'],
                'equipos': partido['equipos'],
                'competicion': partido['competicion'],
                'canales': partido['canales']
            }
        return data


# Exportar todo
__all__ = [
    'SupabaseDB',
    'ConfigManager',
    'CanalManager',
    'MapeoCanalesManager',
    'EventoManager',
    'CalendarioAcestreamManager',
    'Database',
    'DataManagerSupabase',
    'SupabaseDocumentSnapshot'
]
