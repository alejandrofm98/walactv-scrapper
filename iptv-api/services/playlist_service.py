"""
Servicio de generación de playlists M3U dinámicas
"""
import os
import re
import hashlib
from datetime import datetime
from typing import Optional, List, Dict, Any, Iterator
from urllib.parse import urlparse, urlencode
from supabase import Client

from utils.config import get_settings


class PlaylistService:
    """Servicio para generación de playlists M3U"""

    def __init__(self, supabase: Client):
        self.supabase = supabase
        self.settings = get_settings()
        self._template_cache = None
        self._template_path = "/app/data/m3u/playlist_template.m3u"
        self._load_template()
    
    def _load_template(self):
        """Carga el template M3U en memoria para acceso rápido"""
        try:
            if os.path.exists(self._template_path):
                with open(self._template_path, 'r', encoding='utf-8') as f:
                    self._template_cache = f.read()
                print(f"✅ Template M3U cargado en memoria: {len(self._template_cache):,} caracteres")
            else:
                print(f"⚠️  Template no encontrado: {self._template_path}")
                self._template_cache = None
        except Exception as e:
            print(f"❌ Error cargando template: {e}")
            self._template_cache = None
    
    def reload_template(self):
        """Recarga el template (útil después de sincronización)"""
        self._load_template()

    def _extract_stream_id(self, url: str) -> str:
        """
        Extrae un ID único del stream a partir de la URL original

        La URL original puede ser algo como:
        http://servidor.com/live/admin/pass123/12345.ts

        Retornamos un hash corto para usarlo como identificador
        """
        # Crear hash de la URL
        return hashlib.md5(url.encode()).hexdigest()[:16]

    def _build_proxy_url(
        self,
        original_url: str,
        username: str,
        password: str,
        content_type: str = 'live'
    ) -> str:
        """
        Construye la URL proxificada para el stream

        Args:
            original_url: URL original del stream
            username: Usuario IPTV
            password: Contraseña del usuario
            content_type: 'live', 'movie' o 'series'

        Returns:
            URL proxificada: http://domain/live/user/pass/stream_id.ts
        """
        stream_id = self._extract_stream_id(original_url)

        # Determinar extensión
        parsed = urlparse(original_url)
        path = parsed.path.lower()

        if '.m3u8' in path:
            ext = '.m3u8'
        elif '.ts' in path:
            ext = '.ts'
        else:
            ext = '.ts'  # Default

        base_url = self.settings.public_domain.rstrip('/')

        return f"{base_url}/{content_type}/{username}/{password}/{stream_id}{ext}"

    def generate_m3u(
        self,
        username: str,
        password: str,
        include_channels: bool = True,
        include_movies: bool = True,
        include_series: bool = True,
        group_filter: Optional[str] = None,
        country_filter: Optional[str] = None
    ) -> str:
        """
        Genera playlist M3U usando template pre-procesado con placeholders.
        Ultra-rápido: usa template cacheado en memoria + string.replace()
        
        Args:
            username: Nombre de usuario del cliente
            password: Contraseña del cliente
            include_channels, include_movies, include_series: Parámetros obsoletos (mantenidos por compatibilidad)
            group_filter, country_filter: Parámetros obsoletos (mantenidos por compatibilidad)
        
        Returns:
            Contenido M3U completo como string (mucho más rápido que streaming)
        """
        public_domain = self.settings.public_domain.rstrip('/')
        
        # Verificar que el template está cargado
        if self._template_cache is None:
            # Intentar recargar por si acaso
            self._load_template()
            
            if self._template_cache is None:
                # Fallback: devolver error
                return "#EXTM3U\n#EXTINF:-1,Error\n# Error: No se encontró el archivo template. Ejecuta sync_iptv.py primero.\n"
        
        # Hacer copia del template y aplicar reemplazos (muy rápido en memoria)
        # Solo 3 operaciones de replace en todo el string de 354MB
        content = self._template_cache
        content = content.replace('{{DOMAIN}}', public_domain)
        content = content.replace('{{USERNAME}}', username)
        content = content.replace('{{PASSWORD}}', password)
        
        return content

    def _build_extinf(self, item: Dict[str, Any], content_type: str) -> str:
        """
        Construye la línea #EXTINF para un item

        Args:
            item: Datos del canal/película/serie
            content_type: 'channel', 'movie' o 'series'

        Returns:
            Línea #EXTINF formateada
        """
        # Extraer datos
        name = item.get('nombre', 'Unknown')
        logo = item.get('logo', '')
        group = item.get('grupo', '')
        tvg_id = item.get('tvg_id', '')

        # Construir atributos
        attrs = []

        if tvg_id:
            attrs.append(f'tvg-id="{tvg_id}"')

        attrs.append(f'tvg-name="{name}"')

        if logo:
            attrs.append(f'tvg-logo="{logo}"')

        if group:
            attrs.append(f'group-title="{group}"')

        attrs_str = ' '.join(attrs)

        return f'#EXTINF:-1 {attrs_str},{name}'

    def get_playlist_stats(self) -> Dict[str, int]:
        """Obtiene estadísticas de contenido disponible"""
        channels = self.supabase.table('channels').select(
            'id', count='exact'
        ).execute()
        movies = self.supabase.table('movies').select(
            'id', count='exact'
        ).execute()
        series = self.supabase.table('series').select(
            'id', count='exact'
        ).execute()

        return {
            'total_channels': channels.count or 0,
            'total_movies': movies.count or 0,
            'total_series': series.count or 0
        }

    def get_available_groups(self) -> List[str]:
        """Obtiene lista de grupos disponibles"""
        result = self.supabase.table('channels').select('grupo').execute()
        groups = set()

        for item in (result.data or []):
            if item.get('grupo'):
                groups.add(item['grupo'])

        return sorted(list(groups))

    def get_available_countries(self) -> List[str]:
        """Obtiene lista de países disponibles"""
        result = self.supabase.table('channels').select('country').execute()
        countries = set()

        for item in (result.data or []):
            if item.get('country'):
                countries.add(item['country'])

        return sorted(list(countries))
