"""
Módulo de configuración común para scripts IPTV
Centraliza la carga de variables de entorno y configuración de clientes
"""

import os
import warnings
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional
from supabase import create_client, Client

# Suprimir warning específico de storage endpoint
# Este warning es inofensivo pero molesto en versiones específicas de supabase-py
warnings.filterwarnings('ignore', message='.*Storage endpoint URL.*trailing slash.*')


def load_environment() -> bool:
  """
  Carga variables de entorno intentando múltiples ubicaciones

  Returns:
      bool: True si se cargó desde archivo, False si usa variables del sistema
  """
  # Ubicaciones a intentar (en orden de prioridad)
  env_paths = [
    Path(__file__).parent / '.env',  # Directorio actual
    Path(__file__).parent.parent / 'docker' / '.env',  # ../docker/.env (local)
    Path(__file__).parent.parent / '.env',  # Directorio padre
  ]

  loaded = False
  for env_path in env_paths:
    if env_path.exists():
      print(f"✓ Cargando variables de entorno desde: {env_path}")
      load_dotenv(env_path)
      loaded = True
      break

  if not loaded:
    print(
      "⚠ No se encontró archivo .env, usando variables de entorno del sistema")

  return loaded


def get_iptv_credentials() -> tuple[Optional[str], Optional[str]]:
  """
  Obtiene las credenciales IPTV desde variables de entorno

  Returns:
      tuple: (username, password) o (None, None) si no están configuradas
  """
  username = os.getenv("IPTV_USER")
  password = os.getenv("IPTV_PASS")

  if not username or not password:
    print("❌ Error: No se encontraron IPTV_USER o IPTV_PASS")
    return None, None

  return username, password


def get_supabase_client() -> Optional[Client]:
  """
  Crea y retorna un cliente de Supabase configurado

  Returns:
      Client: Cliente de Supabase o None si falta configuración
  """
  url = os.getenv("SUPABASE_URL")
  key = os.getenv("SUPABASE_KEY")

  if not url or not key:
    print("❌ Error: No se encontraron SUPABASE_URL o SUPABASE_KEY")
    return None

  # Asegurar que la URL termine con /
  if not url.endswith('/'):
    url = url + '/'

  try:
    # Construir storage URL explícitamente
    base_url = url.rstrip('/')
    storage_url = f"{base_url}/storage/v1/"

    # Intentar usar ClientOptions para configurar storage
    try:
      from supabase import ClientOptions

      options = ClientOptions(
        storage={"url": storage_url}
      )

      client = create_client(url, key, options)
      print(f"✓ Cliente Supabase con storage URL: {storage_url}")

    except (ImportError, TypeError):
      # Fallback a cliente normal
      client = create_client(url, key)
      print("✓ Cliente Supabase inicializado correctamente")

    return client

  except Exception as e:
    print(f"❌ Error al crear cliente Supabase: {e}")
    return None


def get_iptv_playlist_url() -> Optional[str]:
  """
  Construye la URL de la playlist IPTV

  Returns:
      str: URL de la playlist o None si faltan credenciales
  """
  username, password = get_iptv_credentials()

  if not username or not password:
    return None

  # URL base del servidor IPTV
  base_url = os.getenv("IPTV_BASE_URL", "http://line.ultra-8k.xyz")

  return f"{base_url}/get.php?username={username}&password={password}&type=m3u_plus&output=ts"


class Config:
  """
  Clase de configuración centralizada
  Permite acceso fácil a todas las configuraciones del sistema
  """

  def __init__(self):
    """Inicializa la configuración cargando variables de entorno"""
    load_environment()

    # Credenciales IPTV
    self.iptv_user, self.iptv_pass = get_iptv_credentials()
    self.iptv_url = get_iptv_playlist_url()

    # Credenciales Supabase
    self.supabase_url = os.getenv("SUPABASE_URL")
    self.supabase_key = os.getenv("SUPABASE_KEY")

    # Asegurar que la URL termine con /
    if self.supabase_url and not self.supabase_url.endswith('/'):
      self.supabase_url = self.supabase_url + '/'

    # Configuración de directorios
    # Detectar si estamos en Docker o local
    is_docker = os.path.exists("/.dockerenv") or os.getenv("DOCKER_CONTAINER") == "true"

    if is_docker:
      # Rutas para Docker
      default_repo_images = "/repo-images"
      default_logos_dir = "/app/resources/images"
    else:
      # Rutas para desarrollo local
      project_root = Path(__file__).parent.parent
      default_repo_images = str(project_root / "repo-images")
      default_logos_dir = str(project_root / "resources" / "images")

    self.repo_images_dir = os.getenv("REPO_IMAGES_DIR", default_repo_images)
    self.logos_dir = os.getenv("LOGOS_DIR", default_logos_dir)

    # Configuración de medios
    self.media_domain = os.getenv("MEDIA_DOMAIN", "https://static.walerike.com")
    self.default_logo = os.getenv("DEFAULT_LOGO",
                                  f"{self.media_domain}/default.png")

    # Configuración de bucket (se carga desde Supabase)
    self.bucket_name = None
    self._load_bucket_name()

    # Validar configuración crítica
    self._validate()

  def _validate(self):
    """Valida que las configuraciones críticas estén presentes"""
    errors = []

    if not self.iptv_user or not self.iptv_pass:
      errors.append("Faltan credenciales IPTV (IPTV_USER, IPTV_PASS)")

    if not self.supabase_url or not self.supabase_key:
      errors.append("Faltan credenciales Supabase (SUPABASE_URL, SUPABASE_KEY)")

    if errors:
      print("\n⚠️  Advertencias de configuración:")
      for error in errors:
        print(f"   - {error}")
      print()

  def get_supabase_client(self) -> Optional[Client]:
    """Retorna un cliente de Supabase con storage URL configurada correctamente"""
    if not self.supabase_url or not self.supabase_key:
      print("❌ Error: No se encontraron credenciales Supabase")
      return None

    try:
      # Construir storage URL explícitamente para evitar el error del trailing slash
      base_url = self.supabase_url.rstrip('/')
      storage_url = f"{base_url}/storage/v1/"

      # Importar ClientOptions si está disponible
      try:
        from supabase import ClientOptions

        options = ClientOptions(
          storage={"url": storage_url}
        )

        client = create_client(self.supabase_url, self.supabase_key, options)
        print(f"✓ Cliente Supabase con storage URL explícita: {storage_url}")

      except (ImportError, TypeError):
        # Si ClientOptions no está disponible o falla, usar cliente normal
        client = create_client(self.supabase_url, self.supabase_key)
        print("✓ Cliente Supabase inicializado (sin opciones)")

      return client

    except Exception as e:
      print(f"❌ Error al crear cliente Supabase: {e}")
      import traceback
      traceback.print_exc()
      return None

  def is_valid(self) -> bool:
    """Verifica si la configuración es válida"""
    return all([
      self.iptv_user,
      self.iptv_pass,
      self.supabase_url,
      self.supabase_key
    ])

  def __repr__(self):
    """Representación de la configuración (sin mostrar contraseñas)"""
    is_docker = os.path.exists("/.dockerenv") or os.getenv("DOCKER_CONTAINER") == "true"
    mode = "Docker" if is_docker else "Local"

    return (
      f"Config(\n"
      f"  Modo: {mode}\n"
      f"  IPTV: {'✓' if self.iptv_user else '✗'}\n"
      f"  Supabase: {'✓' if self.supabase_url else '✗'}\n"
      f"  Logos Dir: {self.logos_dir}\n"
      f"  Bucket: {self.bucket_name}\n"
      f")"
    )


# Instancia global de configuración (singleton pattern)
_config_instance = None


def get_config() -> Config:
  """
  Obtiene la instancia global de configuración (singleton)

  Returns:
      Config: Instancia de configuración
  """
  global _config_instance
  if _config_instance is None:
    _config_instance = Config()
  return _config_instance


# Funciones de conveniencia para acceso rápido
def get_iptv_config() -> tuple[Optional[str], Optional[str], Optional[str]]:
  """Retorna (user, pass, url) de IPTV"""
  config = get_config()
  return config.iptv_user, config.iptv_pass, config.iptv_url


def get_directories() -> tuple[str, str]:
  """Retorna (repo_images_dir, logos_dir)"""
  config = get_config()
  return config.repo_images_dir, config.logos_dir


def get_media_config() -> tuple[str, str]:
  """Retorna (media_domain, default_logo)"""
  config = get_config()
  return config.media_domain, config.default_logo