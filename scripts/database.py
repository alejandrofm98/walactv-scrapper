"""
Módulo de base de datos usando Supabase con esquema simplificado
Tablas: channel_mappings y channel_variants (reemplazan 4 tablas antiguas)
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


class ChannelMappingManager:
    """
    Gestor de mapeos simplificado
    Tablas: channel_mappings + channel_variants
    """

    @staticmethod
    def upsert_mapping(source_name: str, display_name: str, channel_ids: List[str] = None, qualities: List[str] = None) -> Optional[int]:
        """
        Inserta o actualiza un mapeo completo con sus variantes
        
        Args:
            source_name: Nombre en futbolenlatv (ej: "DAZN 1 HD")
            display_name: Nombre en la web (ej: "DAZN 1")
            channel_ids: Lista de IDs de la tabla channels (ej: ["dazn1_fhd", "dazn1_hd"])
            qualities: Lista de calidades (ej: ["FHD", "HD"])
        
        Returns:
            ID del mapeo creado/actualizado
        """
        try:
            supabase = SupabaseDB.get_client()
            
            # 1. Insertar o actualizar mapeo
            mapping_data = {
                'source_name': source_name,
                'display_name': display_name
            }
            
            result = supabase.table('channel_mappings').upsert(
                mapping_data, 
                on_conflict='source_name'
            ).execute()
            
            if not result.data:
                return None
                
            mapping_id = result.data[0]['id']
            
            # 2. Si hay channel_ids, actualizar variantes
            if channel_ids:
                # Eliminar variantes antiguas
                supabase.table('channel_variants').delete().eq('mapping_id', mapping_id).execute()
                
                # Insertar nuevas variantes
                variants = []
                for i, channel_id in enumerate(channel_ids):
                    quality = qualities[i] if qualities and i < len(qualities) else 'HD'
                    variants.append({
                        'mapping_id': mapping_id,
                        'channel_id': channel_id,
                        'quality': quality,
                        'priority': i
                    })
                
                if variants:
                    supabase.table('channel_variants').insert(variants).execute()
            
            return mapping_id
            
        except Exception as e:
            print(f"❌ Error guardando mapeo '{source_name}': {e}")
            return None

    @staticmethod
    def get_mapping_by_source(source_name: str) -> Optional[Dict]:
        """Obtiene un mapeo por su nombre de origen (futbolenlatv)"""
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('channel_mappings').select('*').eq('source_name', source_name).execute()
            
            if result.data and len(result.data) > 0:
                mapping = result.data[0]
                # Obtener variantes
                variants = supabase.table('channel_variants').select('*').eq('mapping_id', mapping['id']).order('priority').execute()
                mapping['variants'] = variants.data or []
                return mapping
            return None
        except Exception as e:
            print(f"❌ Error obteniendo mapeo '{source_name}': {e}")
            return None

    @staticmethod
    def get_channel_ids_from_source(source_name: str) -> List[str]:
        """Obtiene lista de channel_ids desde un nombre de origen"""
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('channel_mappings').select('id').eq('source_name', source_name).execute()
            
            if not result.data:
                return []
            
            mapping_id = result.data[0]['id']
            variants = supabase.table('channel_variants').select('channel_id').eq('mapping_id', mapping_id).order('priority').execute()
            
            return [v['channel_id'] for v in variants.data if v['channel_id']]
        except Exception as e:
            print(f"❌ Error obteniendo channel_ids para '{source_name}': {e}")
            return []

    @staticmethod
    def get_all_mappings() -> List[Dict]:
        """Obtiene todos los mapeos con sus variantes"""
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('channel_mappings').select('*').execute()
            
            if not result.data:
                return []
            
            mappings = result.data
            for mapping in mappings:
                variants = supabase.table('channel_variants').select('*').eq('mapping_id', mapping['id']).order('priority').execute()
                mapping['variants'] = variants.data or []
            
            return mappings
        except Exception as e:
            print(f"❌ Error obteniendo mapeos: {e}")
            return []

    @staticmethod
    def get_all_mappings_simple() -> Dict[str, str]:
        """
        Obtiene mapeo simple: source_name -> display_name
        Útil para compatibilidad con código antiguo
        """
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('channel_mappings').select('source_name, display_name').execute()
            
            if result.data:
                return {m['source_name']: m['display_name'] for m in result.data}
            return {}
        except Exception as e:
            print(f"❌ Error obteniendo mapeos simples: {e}")
            return {}

    @staticmethod
    def get_all_mappings_with_channels() -> Dict[str, List[str]]:
        """
        Obtiene mapeo completo: source_name -> [channel_id, channel_id, ...]
        Útil para resolver canales
        """
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('channel_mappings').select('*, channel_variants(channel_id, priority)').execute()
            
            if result.data:
                mappings = {}
                for m in result.data:
                    channel_ids = [v['channel_id'] for v in m.get('channel_variants', []) if v['channel_id']]
                    mappings[m['source_name']] = channel_ids
                return mappings
            return {}
        except Exception as e:
            print(f"❌ Error obteniendo mapeos con canales: {e}")
            return {}


class CalendarioAcestreamManager:
    """
    Gestor de calendario de acestream
    """

    @staticmethod
    def upsert_partido(fecha: date, hora: str, equipos: str, competicion: str = None,
                       canales: List[str] = None, categoria: str = None) -> bool:
        """Inserta o actualiza un partido del calendario"""
        try:
            supabase = SupabaseDB.get_client()
            data = {
                'fecha': fecha.isoformat(),
                'hora': hora,
                'equipos': equipos,
                'competicion': competicion,
                'canales': canales or [],
                'categoria': categoria
            }
            
            # Buscar si existe
            existing = supabase.table('calendario').select('id').eq('fecha', fecha.isoformat()).eq('hora', hora).eq('equipos', equipos).execute()
            
            if existing.data and len(existing.data) > 0:
                # Actualizar
                partido_id = existing.data[0]['id']
                result = supabase.table('calendario').update(data).eq('id', partido_id).execute()
            else:
                # Insertar
                result = supabase.table('calendario').insert(data).execute()
            
            return bool(result.data)
        except Exception as e:
            print(f"❌ Error guardando partido '{equipos}': {e}")
            return False

    @staticmethod
    def get_partidos_by_fecha(fecha: date) -> List[Dict]:
        """Obtiene partidos de una fecha específica"""
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.table('calendario').select('*').eq('fecha', fecha.isoformat()).order('hora').execute()
            return result.data or []
        except Exception as e:
            print(f"❌ Error obteniendo partidos de {fecha}: {e}")
            return []

    @staticmethod
    def get_partidos_with_channels(fecha: date) -> List[Dict]:
        """Obtiene partidos con canales resueltos usando la función SQL"""
        try:
            supabase = SupabaseDB.get_client()
            result = supabase.rpc('get_eventos_fecha_con_channels', {'p_fecha': fecha.isoformat()}).execute()
            return result.data or []
        except Exception as e:
            print(f"❌ Error obteniendo partidos con canales: {e}")
            return []


# Clases de compatibilidad hacia atrás

class Database:
    """
    Clase Database compatible con la interfaz anterior
    """

    def __init__(self, table_name: str, document_name: str, json_document: Optional[str] = None):
        self.table_name = table_name
        self.document_name = document_name
        self.json_document = json_document

    def add_data_firebase(self) -> bool:
        """Guarda datos manteniendo compatibilidad"""
        try:
            if not self.json_document:
                return False

            data = json.loads(self.json_document)

            if self.table_name == 'mapeo_canales':
                # Migrar datos antiguos al nuevo formato
                for source_name, info in data.items():
                    if isinstance(info, dict):
                        display_name = info.get('display_name', source_name)
                        channel_ids = info.get('channel_ids', [])
                        qualities = info.get('qualities', [])
                        ChannelMappingManager.upsert_mapping(source_name, display_name, channel_ids, qualities)
                return True

            elif self.table_name == 'calendario':
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
                            canales=partido.get('canales', []),
                            categoria=partido.get('categoria', '')
                        )
                return True

            return False

        except Exception as e:
            print(f"❌ Error en add_data_firebase: {e}")
            return False

    def get_doc_firebase(self):
        """Obtiene documento manteniendo compatibilidad"""
        from database import SupabaseDocumentSnapshot
        
        try:
            if self.table_name == 'mapeo_canales':
                mappings = ChannelMappingManager.get_all_mappings_simple()
                return SupabaseDocumentSnapshot(mappings, exists=True)

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
                        'canales': partido['canales'],
                        'categoria': partido.get('categoria', '')
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
    def obtener_mapeo_canales() -> Dict[str, List[str]]:
        """Obtiene mapeo de canales: source_name -> [channel_ids]"""
        return ChannelMappingManager.get_all_mappings_with_channels()

    @staticmethod
    def obtener_mapeo_web(fuente: str = 'futbolenlatv') -> Dict[str, str]:
        """Obtiene mapeo web: source_name -> display_name"""
        return ChannelMappingManager.get_all_mappings_simple()

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
                    canales=partido.get('canales', []),
                    categoria=partido.get('categoria', '')
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
                'canales': partido['canales'],
                'categoria': partido.get('categoria', '')
            }
        return data


# Exportar todo
__all__ = [
    'SupabaseDB',
    'ConfigManager',
    'ChannelMappingManager',
    'CalendarioAcestreamManager',
    'Database',
    'DataManagerSupabase',
    'SupabaseDocumentSnapshot'
]
