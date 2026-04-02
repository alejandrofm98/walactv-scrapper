"""
Configuración centralizada para IPTV
Carga configuración IPTV desde PostgreSQL y variables de entorno locales
"""
import os
import asyncio
import warnings
from pathlib import Path
from functools import lru_cache
from typing import Optional
from dotenv import load_dotenv
import asyncpg


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
  - Configuración dinámica (desde PostgreSQL tabla config)
  """

  pg_host: str = os.getenv("PG_HOST", "")
  pg_port: int = int(os.getenv("PG_PORT", "5432"))
  pg_user: str = os.getenv("PG_USER", "")
  pg_password: str = os.getenv("PG_PASSWORD", "")
  pg_database: str = os.getenv("PG_DATABASE", "postgres")

  api_secret_key: str = os.getenv("API_SECRET_KEY",
                                  "your-secret-key-change-in-production")

  iptv_user: Optional[str] = None
  iptv_pass: Optional[str] = None
  iptv_base_url: Optional[str] = None
  iptv_source_url: Optional[str] = None

  session_timeout_minutes: int = 30
  cleanup_interval_minutes: int = 5
  public_domain: str = "http://localhost:8000"

  _config_loaded: bool = False
  _pool_cache: Optional[asyncpg.Pool] = None

  def __init__(self):
    """Inicializa configuración (sin carga async)"""
    pass

  def _ensure_pg_config(self) -> bool:
    """Verifica que la configuración PostgreSQL esté completa"""
    return bool(self.pg_host and self.pg_user and self.pg_password)

  async def _get_pool(self) -> asyncpg.Pool:
    """Obtiene o crea el pool de conexiones"""
    if self._pool_cache is not None:
      return self._pool_cache

    if not self._ensure_pg_config():
      raise ValueError(
        "PostgreSQL no está configurado. Revisa PG_HOST, PG_USER y PG_PASSWORD")

    self._pool_cache = await asyncpg.create_pool(
      host=self.pg_host,
      port=self.pg_port,
      user=self.pg_user,
      password=self.pg_password,
      database=self.pg_database,
      min_size=2,
      max_size=10,
    )
    return self._pool_cache

  async def _load_config(self) -> None:
    """Carga configuración dinámica desde tabla config en PostgreSQL"""
    if not self._ensure_pg_config():
      return

    try:
      pool = await self._get_pool()
      async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT key, value FROM config")

        if not rows:
          return

        config = {row['key']: row['value'] for row in rows}

        self.iptv_user = config.get('IPTV_USER')
        self.iptv_pass = config.get('IPTV_PASS')
        self.iptv_base_url = config.get('IPTV_BASE_URL')

        if 'SESSION_TIMEOUT_MINUTES' in config:
          self.session_timeout_minutes = int(config['SESSION_TIMEOUT_MINUTES'])

        if 'CLEANUP_INTERVAL_MINUTES' in config:
          self.cleanup_interval_minutes = int(config['CLEANUP_INTERVAL_MINUTES'])

        if 'PUBLIC_DOMAIN' in config:
          self.public_domain = config['PUBLIC_DOMAIN']
        elif os.getenv('PUBLIC_DOMAIN'):
          self.public_domain = os.getenv('PUBLIC_DOMAIN')

        if self.iptv_user and self.iptv_pass and self.iptv_base_url:
          self.iptv_source_url = (
            f"{self.iptv_base_url}/get.php?"
            f"username={self.iptv_user}&"
            f"password={self.iptv_pass}&"
            f"type=m3u_plus&output=ts"
          )
          self._config_loaded = True

    except Exception as e:
      pass

  async def reload_config(self) -> bool:
    """Recarga configuración desde PostgreSQL"""
    self._config_loaded = False
    await self._load_config()
    return self._config_loaded

  def is_postgres_configured(self) -> bool:
    """Verifica si PostgreSQL está configurado"""
    return self._ensure_pg_config()

  def is_iptv_configured(self) -> bool:
    """Verifica si IPTV está configurado"""
    return bool(self.iptv_user and self.iptv_pass and self.iptv_base_url)

  def is_valid(self) -> bool:
    """Verifica que toda la configuración sea válida"""
    return self.is_postgres_configured() and self.is_iptv_configured()

  def validate(self, verbose: bool = True) -> bool:
    """Valida la configuración y muestra errores"""
    errors = []
    warnings_list = []

    if not self.pg_host:
      errors.append("PG_HOST no configurada")
    if not self.pg_user:
      errors.append("PG_USER no configurada")
    if not self.pg_password:
      errors.append("PG_PASSWORD no configurada")

    if not self.iptv_user:
      errors.append("IPTV_USER no encontrado en tabla config")
    if not self.iptv_pass:
      errors.append("IPTV_PASS no encontrado en tabla config")
    if not self.iptv_base_url:
      warnings_list.append("IPTV_BASE_URL no configurado")

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
      f"  PostgreSQL: {'✓' if self.is_postgres_configured() else '✗'}\n"
      f"  IPTV User: {self.iptv_user or '✗'}\n"
      f"  IPTV Config: {'✓' if self.is_iptv_configured() else '✗'}\n"
      f"  Session Timeout: {self.session_timeout_minutes}min\n"
      f"  Cleanup Interval: {self.cleanup_interval_minutes}min\n"
      f"  Public Domain: {self.public_domain}\n"
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

