"""
Servicio de gestión de dispositivos y sesiones
"""
import hashlib
import re
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from supabase import Client

from utils.config import get_settings
from utils.models import DeviceType


class DeviceService:
    """Servicio para gestión de dispositivos y sesiones"""

    def __init__(self, supabase: Client):
        self.supabase = supabase
        self.settings = get_settings()

    def _generate_device_id(self, user_agent: str, ip_address: str) -> str:
        """Genera un ID único para el dispositivo"""
        # Combinamos User-Agent e IP para crear un identificador único
        raw = f"{user_agent}:{ip_address}"
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    def _parse_device_info(self, user_agent: str) -> Tuple[str, DeviceType]:
        """
        Parsea el User-Agent para obtener nombre y tipo de dispositivo

        Returns:
            (device_name, device_type)
        """
        ua_lower = user_agent.lower()

        # Detectar apps IPTV comunes
        iptv_apps = {
            'tivimate': ('TiviMate', DeviceType.TV),
            'iptv smarters': ('IPTV Smarters', DeviceType.MOBILE),
            'smarters': ('IPTV Smarters', DeviceType.MOBILE),
            'xciptv': ('XCIPTV', DeviceType.MOBILE),
            'ott navigator': ('OTT Navigator', DeviceType.TV),
            'perfect player': ('Perfect Player', DeviceType.TV),
            'kodi': ('Kodi', DeviceType.TV),
            'vlc': ('VLC Media Player', DeviceType.DESKTOP),
            'mpv': ('MPV Player', DeviceType.DESKTOP),
            'iptv pro': ('IPTV Pro', DeviceType.MOBILE),
            'gse': ('GSE Smart IPTV', DeviceType.MOBILE),
            'implayer': ('implayer', DeviceType.TV),
            'duplex': ('Duplex IPTV', DeviceType.TV),
            'ibo player': ('iBO Player', DeviceType.TV),
            'lazy iptv': ('Lazy IPTV', DeviceType.TV),
        }

        for key, (name, dtype) in iptv_apps.items():
            if key in ua_lower:
                return (name, dtype)

        # Detectar Smart TVs
        tv_patterns = [
            (r'smarttv', 'Smart TV'),
            (r'smart-tv', 'Smart TV'),
            (r'webos', 'LG Smart TV'),
            (r'tizen', 'Samsung Smart TV'),
            (r'roku', 'Roku'),
            (r'fire tv', 'Amazon Fire TV'),
            (r'firetv', 'Amazon Fire TV'),
            (r'androidtv', 'Android TV'),
            (r'chromecast', 'Chromecast'),
            (r'apple\s*tv', 'Apple TV'),
            (r'playstation', 'PlayStation'),
            (r'xbox', 'Xbox'),
        ]

        for pattern, name in tv_patterns:
            if re.search(pattern, ua_lower):
                return (name, DeviceType.TV)

        # Detectar móviles
        mobile_patterns = [
            (r'iphone', 'iPhone'),
            (r'ipad', 'iPad'),
            (r'android.*mobile', 'Android Phone'),
            (r'android', 'Android Device'),
        ]

        for pattern, name in mobile_patterns:
            if re.search(pattern, ua_lower):
                return (name, DeviceType.MOBILE)

        # Detectar navegadores desktop
        browser_patterns = [
            (r'chrome', 'Chrome'),
            (r'firefox', 'Firefox'),
            (r'safari', 'Safari'),
            (r'edge', 'Edge'),
            (r'opera', 'Opera'),
        ]

        for pattern, name in browser_patterns:
            if re.search(pattern, ua_lower):
                # Detectar SO
                os_name = 'Desktop'
                if 'windows' in ua_lower:
                    os_name = 'Windows'
                elif 'mac' in ua_lower:
                    os_name = 'macOS'
                elif 'linux' in ua_lower:
                    os_name = 'Linux'

                return (f"{name} - {os_name}", DeviceType.DESKTOP)

        return ('Dispositivo desconocido', DeviceType.UNKNOWN)

    def register_or_update_session(
        self,
        user_id: str,
        user_agent: str,
        ip_address: str,
        max_connections: int
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """
        Registra o actualiza una sesión de dispositivo

        Returns:
            (success, message, session_data)
        """
        device_id = self._generate_device_id(user_agent, ip_address)
        device_name, device_type = self._parse_device_info(user_agent)

        # Buscar sesión existente para este dispositivo
        existing = self.supabase.table('active_sessions').select('*').eq(
            'user_id', user_id
        ).eq('device_id', device_id).execute()

        now = datetime.utcnow().isoformat()

        if existing.data:
            # Actualizar last_activity
            result = self.supabase.table('active_sessions').update({
                'last_activity': now,
                'ip_address': ip_address,
                'user_agent': user_agent
            }).eq('id', existing.data[0]['id']).execute()

            return (True, "Sesión actualizada", result.data[0] if result.data else None)

        # Verificar límite de conexiones
        sessions_count = self.supabase.table('active_sessions').select(
            'id', count='exact'
        ).eq('user_id', user_id).execute()

        current_count = sessions_count.count or 0

        if current_count >= max_connections:
            return (
                False,
                f"Límite de dispositivos alcanzado ({current_count}/{max_connections})",
                None
            )

        # Crear nueva sesión
        session_data = {
            'user_id': user_id,
            'device_id': device_id,
            'device_name': device_name,
            'device_type': device_type.value,
            'ip_address': ip_address,
            'user_agent': user_agent,
            'last_activity': now
        }

        result = self.supabase.table('active_sessions').insert(session_data).execute()

        if result.data:
            return (True, "Nueva sesión registrada", result.data[0])

        return (False, "Error al crear sesión", None)

    def get_user_devices(self, user_id: str) -> List[Dict[str, Any]]:
        """Obtiene todos los dispositivos activos de un usuario"""
        result = self.supabase.table('active_sessions').select(
            'id, device_id, device_name, device_type, ip_address, last_activity, created_at'
        ).eq('user_id', user_id).order('last_activity', desc=True).execute()

        return result.data or []

    def disconnect_device(self, user_id: str, device_id: str) -> bool:
        """Desconecta un dispositivo específico"""
        result = self.supabase.table('active_sessions').delete().eq(
            'user_id', user_id
        ).eq('device_id', device_id).execute()

        return bool(result.data)

    def disconnect_all_devices(self, user_id: str) -> int:
        """Desconecta todos los dispositivos de un usuario"""
        result = self.supabase.table('active_sessions').delete().eq(
            'user_id', user_id
        ).execute()

        return len(result.data) if result.data else 0

    def cleanup_inactive_sessions(self, timeout_minutes: int = None) -> int:
        """
        Limpia sesiones inactivas

        Returns:
            Número de sesiones eliminadas
        """
        if timeout_minutes is None:
            timeout_minutes = self.settings.session_timeout_minutes

        threshold = datetime.utcnow() - timedelta(minutes=timeout_minutes)

        result = self.supabase.table('active_sessions').delete().lt(
            'last_activity', threshold.isoformat()
        ).execute()

        return len(result.data) if result.data else 0

    def is_device_allowed(
        self,
        user_id: str,
        user_agent: str,
        ip_address: str,
        max_connections: int
    ) -> Tuple[bool, str]:
        """
        Verifica si un dispositivo puede conectarse

        Returns:
            (allowed, message)
        """
        device_id = self._generate_device_id(user_agent, ip_address)

        # Verificar si ya está registrado
        existing = self.supabase.table('active_sessions').select('id').eq(
            'user_id', user_id
        ).eq('device_id', device_id).execute()

        if existing.data:
            return (True, "Dispositivo registrado")

        # Verificar límite
        sessions_count = self.supabase.table('active_sessions').select(
            'id', count='exact'
        ).eq('user_id', user_id).execute()

        current_count = sessions_count.count or 0

        if current_count >= max_connections:
            return (
                False,
                f"Límite de dispositivos alcanzado ({current_count}/{max_connections})"
            )

        return (True, "Dispositivo permitido")

    def get_all_sessions(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Obtiene todas las sesiones activas (para admin)"""
        result = self.supabase.table('active_sessions').select(
            '*, users!inner(username)'
        ).order('last_activity', desc=True).limit(limit).execute()

        sessions = []
        for session in (result.data or []):
            session['username'] = session.pop('users', {}).get('username', 'Unknown')
            sessions.append(session)

        return sessions
