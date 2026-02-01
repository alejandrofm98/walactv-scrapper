import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Query, Depends, Header, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, StreamingResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt

from services import UserService, DeviceService, PlaylistService, StreamProxyService
from utils.config import get_settings
from utils.models import UserCreate, UserUpdate, ValidateCredentials, AuthResult, SystemStats
from utils.constants import JWT_ALGORITHM, JWT_ACCESS_TOKEN_EXPIRE_MINUTES
from utils.models import Token

# Configuración
settings = get_settings()

# Configuración JWT
SECRET_KEY = settings.jwt_secret
ALGORITHM = JWT_ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES = JWT_ACCESS_TOKEN_EXPIRE_MINUTES

# Clientes Supabase (inicializados en startup)
supabase_client = None
user_service: UserService = None
device_service: DeviceService = None
playlist_service: PlaylistService = None
stream_service: StreamProxyService = None

# ============================================
# Funciones de Utilidad Auth
# ============================================

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# ============================================
# Limpieza y Ciclo de Vida
# ============================================

async def cleanup_sessions_task():
    """Tarea periódica para limpiar sesiones inactivas"""
    while True:
        try:
            await asyncio.sleep(settings.cleanup_interval_minutes * 60)
            if device_service:
                cleaned = device_service.cleanup_inactive_sessions()
                if cleaned > 0:
                    print(f"🧹 Limpiadas {cleaned} sesiones inactivas")
        except Exception as e:
            print(f"❌ Error en limpieza de sesiones: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestión del ciclo de vida de la aplicación"""
    global supabase_client, user_service, device_service, playlist_service, stream_service

    print("🚀 Iniciando IPTV API...")

    if not settings.is_valid():
        print("❌ Error: Configuración incompleta")
    else:
        supabase_client = settings.get_supabase_client()
        user_service = UserService(supabase_client)
        device_service = DeviceService(supabase_client)
        playlist_service = PlaylistService(supabase_client)
        stream_service = StreamProxyService(supabase_client)

        stream_service.preload_cache()
        asyncio.create_task(cleanup_sessions_task())
        print("✅ IPTV API iniciada correctamente")

    yield

    print("🛑 Cerrando IPTV API...")

# Crear aplicación
app = FastAPI(
    title="IPTV API",
    description="API para gestión de usuarios IPTV con control de dispositivos y JWT",
    version="2.0.0",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Ajustar en producción
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================
# Dependencias de Seguridad
# ============================================

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/auth/login")

async def get_current_user(token: str = Depends(oauth2_scheme)):
    """Verifica el token JWT y retorna el usuario"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No se pudo validar las credenciales",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: str = payload.get("sub")
        role: str = payload.get("role")

        if user_id is None or role is None:
            raise credentials_exception

        # Opcional: Verificar que el usuario sigue existiendo y activo en BD
        # user_db = user_service.get_user(user_id)
        # if not user_db or not user_db['is_active']:
        #    raise credentials_exception

        return {"id": user_id, "role": role}

    except JWTError:
        raise credentials_exception

async def require_admin(current_user: dict = Depends(get_current_user)):
    """Verifica que el usuario tenga rol de admin"""
    if current_user.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Se requieren permisos de administrador"
        )
    return current_user

# Helpers para servicios
def get_user_service_dep() -> UserService:
    if not user_service: raise HTTPException(500, "Servicio no disponible")
    return user_service

def get_device_service_dep() -> DeviceService:
    if not device_service: raise HTTPException(500, "Servicio no disponible")
    return device_service

def get_playlist_service_dep() -> PlaylistService:
    if not playlist_service: raise HTTPException(500, "Servicio no disponible")
    return playlist_service

def get_stream_service_dep() -> StreamProxyService:
    if not stream_service: raise HTTPException(500, "Servicio no disponible")
    return stream_service


# ============================================
# Health Check
# ============================================

@app.get("/")
async def root():
    return {"service": "IPTV API v2", "status": "running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.post("/api/auth/login", response_model=Token, tags=["Auth"])
async def login(form_data: OAuth2PasswordRequestForm = Depends(), svc: UserService = Depends(get_user_service_dep)):
    """Endpoint de Login. Retorna JWT token."""
    user = svc.get_user_by_username(form_data.username)

    if not user:
        raise HTTPException(status_code=400, detail="Usuario o contraseña incorrectos")

    if not svc._verify_password(form_data.password, user['password_hash']):
        raise HTTPException(status_code=400, detail="Usuario o contraseña incorrectos")

    if not user['is_active']:
        raise HTTPException(status_code=403, detail="Usuario inactivo")

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user['id'], "role": user.get('role', 'user')},
        expires_delta=access_token_expires
    )

    return {"access_token": access_token, "token_type": "bearer", "role": user.get('role', 'user')}


# ============================================
# API: Usuarios (Protegido con JWT Admin)
# ============================================

@app.post("/api/users", response_model=dict, tags=["Users"])
async def create_user(
    user_data: UserCreate,
    _: dict = Depends(require_admin),
    svc: UserService = Depends(get_user_service_dep)
):
    """Crear nuevo usuario (Solo Admin)"""
    try:
        return svc.create_user(user_data)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        raise HTTPException(500, f"Error al crear usuario: {e}")

@app.get("/api/users", response_model=list, tags=["Users"])
async def list_users(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    _: dict = Depends(require_admin),
    svc: UserService = Depends(get_user_service_dep)
):
    """Listar usuarios (Solo Admin)"""
    return svc.list_users(skip, limit)

@app.get("/api/users/{user_id}", response_model=dict, tags=["Users"])
async def get_user(
    user_id: str,
    _: dict = Depends(require_admin),
    svc: UserService = Depends(get_user_service_dep)
):
    """Obtener usuario por ID (Solo Admin)"""
    user = svc.get_user(user_id)
    if not user:
        raise HTTPException(404, "Usuario no encontrado")
    return user

@app.put("/api/users/{user_id}", response_model=dict, tags=["Users"])
async def update_user(
    user_id: str,
    user_data: UserUpdate,
    _: dict = Depends(require_admin),
    svc: UserService = Depends(get_user_service_dep)
):
    """Actualizar usuario (Solo Admin)"""
    user = svc.update_user(user_id, user_data)
    if not user:
        raise HTTPException(404, "Usuario no encontrado")
    return user

@app.delete("/api/users/{user_id}", tags=["Users"])
async def delete_user(
    user_id: str,
    _: dict = Depends(require_admin),
    svc: UserService = Depends(get_user_service_dep)
):
    """Eliminar usuario (Solo Admin)"""
    success = svc.delete_user(user_id)
    if not success:
        raise HTTPException(404, "Usuario no encontrado")
    return {"message": "Usuario eliminado"}

# Mantenemos validate_credentials para compatibilidad externa si la usas scripts internos,
# pero para el Frontend deberían usar /api/auth/login
@app.post("/api/users/validate", response_model=AuthResult, tags=["Users"], deprecated=True)
async def validate_credentials_legacy(
    creds: ValidateCredentials,
    _: dict = Depends(require_admin),
    svc: UserService = Depends(get_user_service_dep)
):
    """Validar credenciales (Uso interno/Legacy)"""
    return svc.validate_credentials(creds.username, creds.password)


# ============================================
# API: Dispositivos (Protegido con JWT Admin)
# ============================================

@app.get("/api/users/{user_id}/devices", response_model=list, tags=["Devices"])
async def get_user_devices(
    user_id: str,
    _: dict = Depends(require_admin),
    svc: DeviceService = Depends(get_device_service_dep)
):
    return svc.get_user_devices(user_id)

@app.delete("/api/users/{user_id}/devices/{device_id}", tags=["Devices"])
async def disconnect_device(
    user_id: str,
    device_id: str,
    _: dict = Depends(require_admin),
    svc: DeviceService = Depends(get_device_service_dep)
):
    success = svc.disconnect_device(user_id, device_id)
    if not success:
        raise HTTPException(404, "Dispositivo no encontrado")
    return {"message": "Dispositivo desconectado"}

@app.delete("/api/users/{user_id}/devices", tags=["Devices"])
async def disconnect_all_devices(
    user_id: str,
    _: dict = Depends(require_admin),
    svc: DeviceService = Depends(get_device_service_dep)
):
    count = svc.disconnect_all_devices(user_id)
    return {"message": f"{count} dispositivos desconectados"}

@app.get("/api/sessions", response_model=list, tags=["Devices"])
async def get_all_sessions(
    limit: int = Query(100, ge=1, le=1000),
    _: dict = Depends(require_admin),
    svc: DeviceService = Depends(get_device_service_dep)
):
    return svc.get_all_sessions(limit)


# ============================================
# API: Stats (Protegido con JWT Admin)
# ============================================

@app.get("/api/stats", response_model=SystemStats, tags=["Stats"])
async def get_system_stats(
    _: dict = Depends(require_admin),
    user_svc: UserService = Depends(get_user_service_dep),
    device_svc: DeviceService = Depends(get_device_service_dep),
    playlist_svc: PlaylistService = Depends(get_playlist_service_dep)
):
    users = user_svc.list_users(limit=10000)
    sessions = device_svc.get_all_sessions(limit=10000)
    playlist_stats = playlist_svc.get_playlist_stats()

    active_users = sum(1 for u in users if u.get('is_active', False))

    return SystemStats(
        total_users=len(users),
        active_users=active_users,
        total_sessions=len(sessions),
        total_channels=playlist_stats['total_channels'],
        total_movies=playlist_stats['total_movies'],
        total_series=playlist_stats['total_series']
    )


# ============================================
# API: Contenido (Protegido con JWT Admin)
# ============================================

@app.get("/api/content/groups", tags=["Content"])
async def get_groups(
    _: dict = Depends(require_admin),
    playlist_svc: PlaylistService = Depends(get_playlist_service_dep)
):
    return {"groups": playlist_svc.get_available_groups()}

@app.get("/api/content/countries", tags=["Content"])
async def get_countries(
    _: dict = Depends(require_admin),
    playlist_svc: PlaylistService = Depends(get_playlist_service_dep)
):
    return {"countries": playlist_svc.get_available_countries()}

@app.post("/api/admin/reload-template", tags=["Admin"])
async def reload_template(
    _: dict = Depends(require_admin),
    playlist_svc: PlaylistService = Depends(get_playlist_service_dep)
):
    """
    Recarga el template M3U en memoria.
    Útil después de sincronizar el contenido con sync_iptv.py
    """
    playlist_svc.reload_template()
    if playlist_svc._template_cache is not None:
        return {
            "status": "success",
            "message": "Template recargado correctamente",
            "size": len(playlist_svc._template_cache)
        }
    else:
        raise HTTPException(
            status_code=500,
            detail="No se pudo recargar el template. Verifica que playlist_template.m3u exista."
        )


# ============================================
# Playlist M3U (Público o Protegido por User/Pass en URL)
# ============================================

@app.get("/playlist/{username}/{password}.m3u", tags=["Playlist"])
async def get_playlist(
    username: str,
    password: str,
    request: Request,
    type: Optional[str] = Query(None),
    group: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    user_svc: UserService = Depends(get_user_service_dep),
    device_svc: DeviceService = Depends(get_device_service_dep),
    playlist_svc: PlaylistService = Depends(get_playlist_service_dep)
):
    auth = user_svc.validate_credentials(username, password)

    if not auth.valid:
        raise HTTPException(401, auth.message)

    if not auth.can_connect:
        raise HTTPException(403, auth.message)

    user_agent = request.headers.get('User-Agent', 'Unknown')
    ip_address = request.client.host if request.client else 'Unknown'

    success, message, _ = device_svc.register_or_update_session(
        user_id=auth.user_id,
        user_agent=user_agent,
        ip_address=ip_address,
        max_connections=auth.max_devices
    )

    if not success:
        raise HTTPException(429, message)

    include_channels = type is None or type == 'channels'
    include_movies = type is None or type == 'movies'
    include_series = type is None or type == 'series'

    m3u_content = playlist_svc.generate_m3u(
        username=username,
        password=password,
        include_channels=include_channels,
        include_movies=include_movies,
        include_series=include_series,
        group_filter=group,
        country_filter=country
    )

    return PlainTextResponse(
        content=m3u_content,
        media_type="application/vnd.apple.mpegurl; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{username}_playlist.m3u"',
            "Cache-Control": "no-cache, no-store, must-revalidate"
        }
    )


# ============================================
# Stream Proxy
# ============================================

@app.get("/live/{username}/{password}/{stream_id}", tags=["Stream"])
@app.get("/movie/{username}/{password}/{stream_id}", tags=["Stream"])
@app.get("/series/{username}/{password}/{stream_id}", tags=["Stream"])
async def proxy_stream(
    username: str,
    password: str,
    stream_id: str,
    request: Request,
    user_svc: UserService = Depends(get_user_service_dep),
    device_svc: DeviceService = Depends(get_device_service_dep),
    stream_svc: StreamProxyService = Depends(get_stream_service_dep)
):
    auth = user_svc.validate_credentials(username, password)

    if not auth.valid:
        raise HTTPException(401, auth.message)

    if not auth.can_connect:
        raise HTTPException(403, auth.message)

    user_agent = request.headers.get('User-Agent', 'Unknown')
    ip_address = request.client.host if request.client else 'Unknown'

    success, message, _ = device_svc.register_or_update_session(
        user_id=auth.user_id,
        user_agent=user_agent,
        ip_address=ip_address,
        max_connections=auth.max_devices
    )

    if not success:
        raise HTTPException(429, message)

    path = request.url.path
    if path.startswith('/live/'): content_type = 'live'
    elif path.startswith('/movie/'): content_type = 'movie'
    elif path.startswith('/series/'): content_type = 'series'
    else: content_type = 'live'

    clean_stream_id = stream_id.split('.')[0]
    original_url = stream_svc.get_original_url(clean_stream_id, content_type)

    if not original_url:
        raise HTTPException(404, "Stream no encontrado")

    try:
        status_code, headers, body = await stream_svc.get_stream_response(original_url)

        return StreamingResponse(
            body,
            status_code=status_code,
            headers=headers,
            media_type=headers.get('content-type', 'video/mp2t')
        )
    except Exception as e:
        raise HTTPException(502, f"Error al obtener stream: {e}")


# ============================================
# Stream Validation for Nginx (sin proxy)
# ============================================

@app.get("/api/auth/validate-stream/{content_type}/{username}/{password}/{provider_id}", tags=["Stream"])
async def validate_stream(
    content_type: str,
    username: str,
    password: str,
    provider_id: str,
    request: Request,
    user_svc: UserService = Depends(get_user_service_dep),
    device_svc: DeviceService = Depends(get_device_service_dep),
    stream_svc: StreamProxyService = Depends(get_stream_service_dep)
):
    """
    Valida credenciales y devuelve URL original para nginx.
    Usado por nginx auth_request para validar antes de proxy directo.
    Busca por provider_id (ej: "176861") en lugar de hash MD5.
    """
    auth = user_svc.validate_credentials(username, password)

    if not auth.valid:
        raise HTTPException(401, auth.message)

    if not auth.can_connect:
        raise HTTPException(403, auth.message)

    user_agent = request.headers.get('User-Agent', 'Unknown')
    ip_address = request.client.host if request.client else 'Unknown'

    success, message, _ = device_svc.register_or_update_session(
        user_id=auth.user_id,
        user_agent=user_agent,
        ip_address=ip_address,
        max_connections=auth.max_devices
    )

    if not success:
        raise HTTPException(429, message)

    # Quitar extensión si existe (.mkv, .mp4, .ts)
    clean_provider_id = provider_id.split('.')[0]
    original_url = stream_svc.get_original_url(clean_provider_id, content_type)

    if not original_url:
        raise HTTPException(404, "Stream no encontrado")

    # Devolver URL original en header para que nginx haga proxy
    return PlainTextResponse(
        content="OK",
        headers={
            "X-Original-Url": original_url,
            "X-Provider-Id": clean_provider_id
        }
    )


# ============================================
# Main
# ============================================

if __name__ == "__main__":
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=3001,
        reload=True
    )