"""
Configuración centralizada para IPTV
Carga configuración IPTV desde Supabase y variables de entorno locales
"""
import os
import warnings
from pathlib import Path
from functools import lru_cache
from typing import Optional
from dotenv import load_dotenv
from supabase import create_client, Client

# Suprimir warnings de storage endpoint
warnings.filterwarnings('ignore',
                        message='.*Storage endpoint URL.*trailing slash.*')


def _load_environment() -> None:
  """Carga variables de entorno desde múltiples ubicaciones"""
  env_paths = [
    Path(__file__).parent / '.env',
    Path(__file__).parent.parent / 'docker' / '.env',
    Path(__file__).parent.parent / '.env',
  ]

  for env_path in env_paths:
    if env_path.exists():
      load_dotenv(env_path)
      return


# Cargar .env al importar el módulo
_load_environment()


class Settings:
  """
  Configuración centralizada de la aplicación

  - Variables de entorno locales (de .env)
  - Configuración dinámica (desde Supabase tabla config)
  """

  # ===== Supabase (desde .env) =====
  supabase_url: str = os.getenv("SUPABASE_URL", "")
  supabase_key: str = os.getenv("SUPABASE_KEY", "")

  # ===== API (desde .env) =====
  api_secret_key: str = os.getenv("API_SECRET_KEY",
                                  "your-secret-key-change-in-production")

  # ===== Configuración dinámica (desde Supabase - cargado dinámicamente) =====
  # IPTV
  iptv_user: Optional[str] = None
  iptv_pass: Optional[str] = None
  iptv_base_url: Optional[str] = None
  iptv_source_url: Optional[str] = None  # URL completa generada

  # Servidor
  session_timeout_minutes: int = 30  # Default, se sobrescribe desde BD
  cleanup_interval_minutes: int = 5  # Default, se sobrescribe desde BD

  # ===== Estado interno =====
  _config_loaded: bool = False
  _client_cache: Optional[Client] = None

  def __init__(self):
    """Inicializa y carga configuración desde Supabase"""
    self._ensure_supabase_url()
    self._load_config()

  def _ensure_supabase_url(self) -> None:
    """Asegura que la URL de Supabase termine con /"""
    if self.supabase_url and not self.supabase_url.endswith('/'):
      self.supabase_url = self.supabase_url + '/'

  def _load_config(self) -> None:
    """Carga configuración dinámica desde tabla config en Supabase"""
    if not self.is_supabase_configured():
      return

    try:
      client = self.get_supabase_client()
      response = client.table('config').select('key, value').execute()

      if not response.data:
        return

      # Crear diccionario de configuración
      config = {item['key']: item['value'] for item in response.data}

      # Cargar valores IPTV
      self.iptv_user = config.get('IPTV_USER')
      self.iptv_pass = config.get('IPTV_PASS')
      self.iptv_base_url = config.get('IPTV_BASE_URL')

      # Cargar configuración de servidor
      if 'SESSION_TIMEOUT_MINUTES' in config:
        self.session_timeout_minutes = int(config['SESSION_TIMEOUT_MINUTES'])

      if 'CLEANUP_INTERVAL_MINUTES' in config:
        self.cleanup_interval_minutes = int(config['CLEANUP_INTERVAL_MINUTES'])

      # Generar URL de playlist
      if self.iptv_user and self.iptv_pass:
        self.iptv_source_url = (
          f"{self.iptv_base_url}/get.php?"
          f"username={self.iptv_user}&"
          f"password={self.iptv_pass}&"
          f"type=m3u_plus&output=ts"
        )
        self._config_loaded = True

    except Exception as e:
      # Silencioso por defecto, usar validate() para diagnóstico
      pass

  def reload_config(self) -> bool:
    """
    Recarga configuración desde Supabase

    Returns:
        bool: True si se cargó correctamente
    """
    self._config_loaded = False
    self._load_config()
    return self._config_loaded

  def is_supabase_configured(self) -> bool:
    """Verifica si Supabase está configurado"""
    return bool(self.supabase_url and self.supabase_key)

  def is_iptv_configured(self) -> bool:
    """Verifica si IPTV está configurado"""
    return bool(self.iptv_user and self.iptv_pass and self.iptv_base_url)

  def is_valid(self) -> bool:
    """Verifica que toda la configuración sea válida"""
    return self.is_supabase_configured() and self.is_iptv_configured()

  def get_supabase_client(self) -> Client:
    """
    Obtiene cliente de Supabase (cacheado)

    Returns:
        Client: Cliente de Supabase configurado
    """
    if self._client_cache is not None:
      return self._client_cache

    if not self.is_supabase_configured():
      raise ValueError(
        "Supabase no está configurado. Revisa SUPABASE_URL y SUPABASE_KEY")

    try:
      # Construir storage URL explícitamente
      base_url = self.supabase_url.rstrip('/')
      storage_url = f"{base_url}/storage/v1/"

      try:
        from supabase import ClientOptions
        options = ClientOptions(storage={"url": storage_url})
        client = create_client(self.supabase_url, self.supabase_key, options)
      except (ImportError, TypeError):
        client = create_client(self.supabase_url, self.supabase_key)

      self._client_cache = client
      return client

    except Exception as e:
      raise RuntimeError(f"Error al crear cliente Supabase: {e}")

  def validate(self, verbose: bool = True) -> bool:
    """
    Valida la configuración y muestra errores

    Args:
        verbose: Si True, imprime información de diagnóstico

    Returns:
        bool: True si la configuración es válida
    """
    errors = []
    warnings_list = []

    # Validar Supabase
    if not self.supabase_url:
      errors.append("SUPABASE_URL no configurada")
    if not self.supabase_key:
      errors.append("SUPABASE_KEY no configurada")

    # Validar IPTV
    if not self.iptv_user:
      errors.append("IPTV_USER no encontrado en tabla config")
    if not self.iptv_pass:
      errors.append("IPTV_PASS no encontrado en tabla config")
    if not self.iptv_base_url:
      warnings_list.append("IPTV_BASE_URL no configurado")

    # Validaciones adicionales
    if self.api_secret_key == "your-secret-key-change-in-production":
      warnings_list.append(
        "API_SECRET_KEY usando valor por defecto (cámbialo en producción)")

    if verbose:
      if errors:
        print("\n❌ Errores de configuración:")
        for error in errors:
          print(f"   - {error}")

      if warnings_list:
        print("\n⚠️  Advertencias:")
        for warning in warnings_list:
          print(f"   - {warning}")

      if not errors and not warnings_list:
        print("\n✅ Configuración válida")

    return len(errors) == 0

  def __repr__(self) -> str:
    """Representación de la configuración"""
    is_docker = os.path.exists("/.dockerenv") or os.getenv(
      "DOCKER_CONTAINER") == "true"
    mode = "🐳 Docker" if is_docker else "💻 Local"

    return (
      f"Settings(\n"
      f"  Modo: {mode}\n"
      f"  Supabase: {'✓' if self.is_supabase_configured() else '✗'}\n"
      f"  IPTV User: {self.iptv_user or '✗'}\n"
      f"  IPTV Config: {'✓' if self.is_iptv_configured() else '✗'}\n"
      f"  Session Timeout: {self.session_timeout_minutes}min\n"
      f"  Cleanup Interval: {self.cleanup_interval_minutes}min\n"
      f")"
    )


@lru_cache()
def get_settings() -> Settings:
  """
  Obtiene configuración cacheada (singleton)

  Returns:
      Settings: Instancia única de configuración
  """
  return Settings()

