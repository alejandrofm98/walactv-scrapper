# IPTV API v2.0 - Sistema de Gestión de Usuarios con Control de Dispositivos

API para gestión de usuarios IPTV con autenticación JWT, control de conexiones simultáneas y generación dinámica de playlists M3U.

## Características

- **Autenticación JWT**: Sistema completo de autenticación con tokens JWT y OAuth2
- **Roles de usuario**: Soporte para roles `admin` y `user`
- **Gestión de usuarios**: Crear, editar, eliminar usuarios con límite de conexiones
- **Control de dispositivos**: Tracking de dispositivos conectados por usuario
- **Límite de conexiones**: Máximo de dispositivos simultáneos por cuenta
- **Playlists dinámicas**: Generación de M3U con URLs proxificadas y filtros
- **Proxy de streams**: Autenticación en cada solicitud de stream
- **Detección de dispositivos**: Identificación automática del tipo de dispositivo
- **Limpieza automática**: Sesiones inactivas se eliminan automáticamente
- **Estadísticas del sistema**: Endpoint para monitorear uso del sistema
- **Sincronización automática**: Scripts para sincronizar contenido desde fuente IPTV

## Arquitectura

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Cliente   │────▶│    Nginx    │────▶│  FastAPI    │
│  (App IPTV) │     │   (Proxy)   │     │    API      │
└─────────────┘     └─────────────┘     └──────┬──────┘
                                                │
                                         ┌──────▼──────┐
                                         │  Supabase   │
                                         │ (PostgreSQL)│
                                         └─────────────┘
```

## Estructura del Proyecto

```
iptv-api/
├── scripts/
│   ├── api.py              # Aplicación FastAPI principal (v2.0)
│   └── sync_iptv.py        # Script de sincronización de contenido
├── services/
│   ├── __init__.py         # Exporta todos los servicios
│   ├── user_service.py     # Gestión de usuarios
│   ├── device_service.py   # Control de dispositivos
│   ├── playlist_service.py # Generación de M3U
│   ├── stream_service.py   # Proxy de streams
│   └── bulk_insert.py      # Inserción masiva de datos
├── utils/
│   ├── config.py           # Configuración centralizada
│   ├── models.py           # Modelos Pydantic
│   └── constants.py        # Constantes del sistema
├── database/
│   └── schema.sql          # Script SQL para Supabase
├── docker/
│   ├── Dockerfile          # Imagen Docker de la API
│   ├── docker-compose.yml  # Orquestación de servicios
│   └── .env                # Variables de entorno Docker
├── nginx/
│   └── nginx.conf          # Configuración Nginx
├── postman/
│   ├── postman.json        # Colección de Postman
│   └── environment.json    # Variables de entorno Postman
├── requirements.txt        # Dependencias Python
└── .env.example            # Ejemplo de variables de entorno
```

## Instalación

### 1. Configurar Supabase

Ejecuta el script SQL en tu proyecto de Supabase:

```bash
# Abre el SQL Editor en Supabase y ejecuta:
database/schema.sql
```

### 2. Configurar variables de entorno

```bash
cp .env.example .env
# Editar .env con tus valores de Supabase y configuración
```

Variables importantes:
- `SUPABASE_URL` y `SUPABASE_KEY`: Credenciales de Supabase
- `API_SECRET_KEY`: Clave secreta para JWT
- `IPTV_SOURCE_URL`: URL de la playlist fuente para sincronización
- `PUBLIC_DOMAIN`: Dominio público para generar URLs

### 3. Iniciar con Docker

```bash
cd docker
docker-compose up -d
```

### 4. Verificar funcionamiento

```bash
curl http://localhost/health
```

## Autenticación

La API utiliza JWT (JSON Web Tokens) para autenticación. Los endpoints administrativos requieren un token válido.

### Login

```bash
curl -X POST http://localhost/api/auth/login \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=admin&password=admin123"
```

Respuesta:
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIs...",
  "token_type": "bearer",
  "role": "admin"
}
```

### Uso del Token

Incluir el token en el header `Authorization`:

```bash
curl http://localhost/api/users \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIs..."
```

## Endpoints API

### Autenticación

| Método | Endpoint | Descripción | Auth |
|--------|----------|-------------|------|
| POST | `/api/auth/login` | Login con JWT | Público |

### Usuarios (Requiere Admin)

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| POST | `/api/users` | Crear usuario |
| GET | `/api/users` | Listar usuarios |
| GET | `/api/users/{id}` | Obtener usuario |
| PUT | `/api/users/{id}` | Actualizar usuario |
| DELETE | `/api/users/{id}` | Eliminar usuario |
| POST | `/api/users/validate` | Validar credenciales (Legacy) |

**Nota**: Los usuarios pueden tener rol `admin` o `user`.

### Dispositivos (Requiere Admin)

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| GET | `/api/users/{id}/devices` | Dispositivos del usuario |
| DELETE | `/api/users/{id}/devices/{device_id}` | Desconectar dispositivo |
| DELETE | `/api/users/{id}/devices` | Desconectar todos |
| GET | `/api/sessions` | Todas las sesiones activas |

### Estadísticas (Requiere Admin)

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| GET | `/api/stats` | Estadísticas del sistema |

Respuesta de `/api/stats`:
```json
{
  "total_users": 100,
  "active_users": 85,
  "total_sessions": 45,
  "total_channels": 2500,
  "total_movies": 500,
  "total_series": 300
}
```

### Contenido (Requiere Admin)

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| GET | `/api/content/groups` | Lista de grupos disponibles |
| GET | `/api/content/countries` | Lista de países disponibles |

### Playlist y Streams (Público - Autenticado por URL)

| Endpoint | Descripción |
|----------|-------------|
| `/playlist/{user}/{pass}.m3u` | Playlist M3U personalizada |
| `/live/{user}/{pass}/{stream_id}` | Stream de canal en vivo |
| `/movie/{user}/{pass}/{stream_id}` | Stream de película |
| `/series/{user}/{pass}/{stream_id}` | Stream de serie |

**Parámetros de playlist**:
- `type`: Filtrar por tipo (`channels`, `movies`, `series`)
- `group`: Filtrar por grupo
- `country`: Filtrar por país

Ejemplo: `/playlist/usuario/pass.m3u?type=channels&country=ES`

## Uso

### Crear usuario (como Admin)

```bash
# Primero obtener token
curl -X POST http://localhost/api/auth/login \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=admin&password=admin123"

# Crear usuario con el token
curl -X POST http://localhost/api/users \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer TOKEN_AQUI" \
  -d '{
    "username": "usuario1",
    "password": "password123",
    "max_connections": 2,
    "role": "user"
  }'
```

### Obtener playlist

```
# En tu app IPTV, usar la URL:
http://tu-dominio.com/playlist/usuario1/password123.m3u

# Con filtros:
http://tu-dominio.com/playlist/usuario1/password123.m3u?type=channels&country=ES
```

### Ver dispositivos conectados

```bash
curl http://localhost/api/users/{user_id}/devices \
  -H "Authorization: Bearer TOKEN_AQUI"
```

### Ver estadísticas del sistema

```bash
curl http://localhost/api/stats \
  -H "Authorization: Bearer TOKEN_AQUI"
```

## Sincronización de Contenido

Para sincronizar el contenido desde la fuente IPTV:

```bash
# Desde el contenedor Docker
docker exec -it iptv-api python scripts/sync_iptv.py

# O manualmente con Python
python scripts/sync_iptv.py
```

Este script descarga la playlist fuente y actualiza la base de datos con canales, películas y series.

## Detección de Dispositivos

El sistema detecta automáticamente el tipo de dispositivo basándose en el User-Agent:

| Tipo | Ejemplos |
|------|----------|
| `tv` | TiviMate, Perfect Player, Kodi, Smart TVs |
| `mobile` | IPTV Smarters, GSE Smart IPTV, Android/iPhone |
| `desktop` | VLC, Chrome, Firefox |
| `iptv_app` | Apps IPTV específicas |
| `unknown` | Dispositivo no identificado |

## Respuestas de Error

| Código | Significado |
|--------|-------------|
| 401 | Credenciales inválidas o token expirado |
| 403 | Cuenta desactivada, expirada o sin permisos |
| 429 | Límite de dispositivos alcanzado |
| 404 | Recurso no encontrado |
| 400 | Error en la solicitud |
| 500 | Error interno del servidor |
| 502 | Error al obtener stream |

## Configuración Avanzada

### Límites de conexiones

Cada usuario tiene un `max_connections` que define cuántos dispositivos pueden usar la cuenta simultáneamente. Cuando se alcanza el límite, nuevos dispositivos reciben error 429.

### Sesiones inactivas

Las sesiones se marcan como activas con cada solicitud de stream. Las sesiones que no tienen actividad durante `SESSION_TIMEOUT_MINUTES` (default: 30) se eliminan automáticamente.

### Forzar desconexión

Los administradores pueden:
1. Desconectar un dispositivo específico
2. Desconectar todos los dispositivos de un usuario
3. Ejecutar limpieza manual de sesiones

## Documentación API

- **Swagger UI**: `http://tu-dominio.com/docs`
- **ReDoc**: `http://tu-dominio.com/redoc`

## Colección Postman

En el directorio `postman/` encontrarás:
- `postman.json`: Colección completa de endpoints
- `environment.json`: Variables de entorno

Importa ambos archivos en Postman para probar todos los endpoints.

## Desarrollo

### Estructura de servicios

Los servicios principales están en `services/`:

- **UserService**: Gestión de usuarios, autenticación, validación de credenciales
- **DeviceService**: Control de sesiones y dispositivos conectados
- **PlaylistService**: Generación de playlists M3U, filtros, estadísticas
- **StreamProxyService**: Proxy de streams, cache de URLs

### Modelos de datos

Los modelos Pydantic están en `utils/models.py`:
- `UserCreate`, `UserUpdate`, `UserResponse`: Gestión de usuarios
- `Token`: Respuesta de autenticación JWT
- `DeviceResponse`, `SessionInfo`: Información de dispositivos
- `SystemStats`: Estadísticas del sistema

### Configuración

La configuración centralizada está en `utils/config.py` usando Pydantic Settings.

## Licencia

MIT
