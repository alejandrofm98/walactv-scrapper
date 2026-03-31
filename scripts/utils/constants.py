"""
Constantes globales de configuración para IPTV Sync
"""

# ===== PostgreSQL =====
PG_ENV_HOST = "PG_HOST"
PG_ENV_PORT = "PG_PORT"
PG_ENV_USER = "PG_USER"
PG_ENV_PASSWORD = "PG_PASSWORD"
PG_ENV_DATABASE = "PG_DATABASE"
PG_POOL_MIN_SIZE = 5
PG_POOL_MAX_SIZE = 20

# ===== Config table =====
CONFIG_TABLE = "config"

# ===== IPTV (keys en tabla config) =====
IPTV_USERNAME_KEY = "IPTV_USERNAME"
IPTV_PASSWORD_KEY = "IPTV_PASSWORD"
IPTV_BASE_URL_KEY = "IPTV_BASE_URL"

# ===== Defaults =====
DEFAULT_SESSION_TIMEOUT_MINUTES = 30
DEFAULT_CLEANUP_INTERVAL_MINUTES = 5

# ===== IPTV playlist =====
IPTV_PLAYLIST_TYPE = "m3u_plus"
IPTV_OUTPUT_FORMAT = "ts"

# ===== Docker =====
DOCKER_ENV_FLAG = "DOCKER_CONTAINER"
DOCKER_ENV_VALUE = "true"
DOCKER_ENV_PATH = "/.dockerenv"


DB_DEFAULT_BATCH_SIZE = 5000
DB_DEFAULT_MAX_WORKERS = 1
DB_DEFAULT_MAX_RETRIES = 3

# ===== Tablas de Base de Datos =====
CHANNELS_TABLE = "channels"
MOVIES_TABLE = "movies"
SERIES_TABLE = "series"
SYNC_METADATA_TABLE = "sync_metadata"
REPLAYS_TABLE = "replays"

# ===== Configuración M3U =====
M3U_FILENAME_LATEST = "playlist.m3u"
M3U_FILENAME_PREFIX = "playlist_"
M3U_DIR_DOCKER = "/app/data/m3u"
M3U_DIR_LOCAL_DEFAULT = "data/m3u"
M3U_BACKUP_COPIES_DEFAULT = 1

# ===== Variables de entorno M3U =====
M3U_DIR_ENV = "M3U_DIR"
M3U_ACCESS_TOKEN_ENV = "M3U_ACCESS_TOKEN"
M3U_BACKUP_COPIES_ENV = "M3U_BACKUP_COPIES"
PUBLIC_DOMAIN_ENV = "PUBLIC_DOMAIN"
PUBLIC_DOMAIN_DEFAULT_LOCAL = "http://localhost:8000"
PUBLIC_DOMAIN_DEFAULT_DOCKER = "https://tudominio.com"

# ===== Logos y recursos =====
DEFAULT_LOGO_URL = "https://via.placeholder.com/150"

# ===== Tipos de contenido IPTV =====
CONTENT_TYPE_CHANNEL = "channel"
CONTENT_TYPE_MOVIE = "movie"
CONTENT_TYPE_SERIE = "serie"

# ===== Patrones regex =====
SERIES_PATTERN = r"[Ss](\d{1,2})\s*[Ee](\d{1,2})"
COUNTRY_CODE_PATTERN = r"^[|\s]*([A-Z]{2})[|\s]"

# ===== URLs y paths =====
URL_SERIES_PATH = "/series/"
URL_MOVIE_PATH = "/movie/"

# ===== Configuración de inserción masiva =====
DELETE_BATCH_LIMIT = 5000
MAX_DELETE_ATTEMPTS = 100

# ===== Timeouts y tiempos =====
PLAYLIST_DOWNLOAD_TIMEOUT = 300  # 5 minutos
DELETE_BATCH_SLEEP = 0.1  # segundos

# ===== Metadata sync =====
SYNC_METADATA_ID = "iptv_sync"

# ===== M3U Parsing =====
M3U_EXTINF_PREFIX = "#EXTINF:"
M3U_GROUP_TITLE_ATTR = 'group-title="'
M3U_TVG_LOGO_ATTR = 'tvg-logo="'
M3U_TVG_ID_ATTR = 'tvg-id="'

# ===== HTTP Headers =====
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
