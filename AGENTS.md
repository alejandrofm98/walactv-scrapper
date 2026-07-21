# AGENTS.md - WALACTV-SCRAPPER

> Guia para agentes de codigo que trabajen en walactv-scrapper y en su
> ecosistema de proyectos hermanos (iptv-api, WalacTV, walactvWeb).
> Secciones 0-3 son contexto obligatorio antes de tocar nada.
> Secciones 4-11 son referencia operativa.

## 0. Ecosistema y posicion del proyecto

walactv-scrapper es el motor de ingestion de datos del ecosistema WalacTV.
Produce los catalogos de contenido que iptv-api consume.

```
   +-------------+        +-------------+
   |  walactvWeb |        |   WalacTV   |
   |  Angular 20 |        |  Android TV |
   +------+------+        +-----+-------+
          |                    |
          v                    v
   +--------------------------------------+
   |          iptv-api (FastAPI)          |
   |     Puerto 3010 · Postgres           |
   +---+----------+-----------+-----------+
       |          |           |
       | lee JSON | escribe   | escribe scraper_failures
       v          v           v
   +--------+ +----------+ +-----------+
   |walactv-| | iptv-data| | Postgres  |
   |scrapper| | volumen  | | tabla     |
   | (este) | | compartido| | scraper_  |
   |        | |  (JSONs) | | failures  |
   +--------+ +----------+ +-----------+
```

### Tabla de proyectos hermanos

| Proyecto           | Rol                  | Repo                                                 | Relacion con walactv-scrapper                        |
| ------------------ | -------------------- | ---------------------------------------------------- | ---------------------------------------------------- |
| iptv-api           | Backend central      | `github.com/alejandrofm98/iptv-api`                  | Lee los JSONs que produce este scrapper              |
| iptv-db            | ORM compartido       | `github.com/alejandrofm98/iptv-db`                   | Este scrapper importa modelos de ahi                 |
| WalacTV (Android)  | Cliente TV           | `github.com/alejandrofm98/WalacTV`                   | Consume endpoints de iptv-api                        |
| walactvWeb         | Cliente web          | `github.com/alejandrofm98/walactvWeb`                | Consume endpoints de iptv-api                        |

## 1. Contexto rapido

- **Stack**: Python 3.12, SQLAlchemy 2.0 + psycopg3 (via iptv-db), BeautifulSoup4, requests, Pillow, python-dotenv.
- **Entry points**: `scripts/main.py` (scraper principal), `scripts/sync_iptv.py` (sync IPTV),
  `scripts/sync_replays.py` (replays), `scripts/scrape_tmdb_metadata.py` (TMDB),
  `scripts/generate_content_json.py` (cache JSON), `scripts/poblar_mapeo_canales.py` (mapeo).
- **BD**: PostgreSQL via SQLAlchemy 2.0 (paquete `iptv-db`). El `DatabasePG` mantiene un pool asyncpg legacy para backward compat.
- **Docker**: 5 Dockerfiles + docker-compose.yaml. Despliegue via Ansible + Ofelia (cron).
- **Horarios**: main.py (08:00 diario), sync_iptv.py (cada 6h), sync_replays.py (12:00 diario), tmdb (03:00 diario).

## 2. Arquitectura

### 2.1 Modulos principales (`scripts/`)

| Archivo | Proposito |
|---|---|
| `main.py` | Entry point: init DatabasePG (engine iptv-db), corre scrapper, guarda calendario |
| `scrapper.py` | Scraper de futbolenlatv.es: parsea HTML, mapea canales, genera imagenes |
| `sync_iptv.py` | Sync de playlists M3U del proveedor IPTV a tablas normalizadas |
| `sync_replays.py` | Scraper de replays UFC/wrestling |
| `scrape_tmdb_metadata.py` | Enriquecimiento de metadata via TMDB API |
| `generate_content_json.py` | Generacion de cache JSON gzipped |
| `poblar_mapeo_canales.py` | Poblacion de mapeo de canales |
| `actualiza_epg.py` | Actualizador EPG (credenciales via env vars) |
| `proxificaUrl.py` | HLS proxy (DESHABILITADO en docker-compose) |
| `config.py` | Settings + singleton via @lru_cache (usa iptv_db.engine.build_url) |
| `database.py` | DatabasePG con iptv-db engine + pool asyncpg legacy (backward compat). 6 clases migradas: ConfigManager, ChannelMappingManager, CalendarioAcestreamManager, DataManagerSupabase, etc. |

### 2.2 Servicios (`scripts/services/`)

| Archivo | Proposito |
|---|---|
| `bulk_insert.py` | Insercion masiva async via asyncpg.copy_records_to_table |
| `event_images.py` | Generacion de imagenes de eventos deportivos via PIL |
| `football_logos.py` | Resolucion de logos de equipos de futbol |
| `tennis_flags.py` | Resolucion de banderas para tenis |

### 2.3 Utilidades (`scripts/utils/`)

| Archivo | Proposito |
|---|---|
| `constants.py` | Constantes globales: env vars, nombres de tablas, regex |
| `series_keys.py` | Normalizacion de nombres de series para deduplicacion |

### 2.4 Convenciones de codigo

- Clases: `PascalCase`. Funciones/variables: `snake_case`. Constantes: `UPPER_CASE`.
- Docstrings en espanol, breves.
- Imports: stdlib -> terceros -> locales.
- Tipo hints: parcial (mejor en archivos nuevos).
- Errores: try/except con `print()` para scripts cron, `logging` para scripts nuevos.

## 3. Patrones obligatorios

1. Config via `scripts/config.py` → `Settings` singleton con `@lru_cache`.
2. BD via `scripts/database.py` → `DatabasePG` (asyncpg pool singleton).
3. Para bulk insert, usar `scripts/services/bulk_insert.py` con batch size configurable.
4. Scripts deben tener `if __name__ == "__main__":` guard.
5. Variables sensibles via env vars, NUNCA hardcoded.
6. Para imagenes, usar `scripts/services/event_images.py` (PIL).
7. Fechas en formato ISO 8601.
8. Naming de tablas: singular snake_case (channels, movies_catalog, series_episodes).

## 4. Contratos publicos (cross-project)

### 4.1 Datos que produce este scrapper

- **JSONs de catalogo**: Escritos en `data/json/` (volumen `iptv-data` compartido). iptv-api los lee de `../walactv-scrapper/data/json/`.
- **Tabla `scraper_failures`**: Este scrapper escribe filas de fallos de TMDB. iptv-api las lee para alertas.
- **Volumen compartido**: `iptv-data` (Docker volume / NFS). Mismo path desde ambos contenedores.
- **Red compartida**: `dokploy-network`.

### 4.2 Datos que consume de iptv-api

- `IPTV_API_URL=http://localhost:3010` (via env var).
- `API_SECRET_KEY` (mismo valor que iptv-api).
- Tabla `channels` (iptv-api escribe, este scrapper lee para mapeo).

### 4.3 Variables de entorno comunes

| Variable | Descripcion | Usado por |
|---|---|---|
| `PG_HOST` | Host de PostgreSQL | Ambos |
| `PG_PORT` | Puerto de PostgreSQL | Ambos |
| `PG_USER` | Usuario de PostgreSQL | Ambos |
| `PG_PASSWORD` | Contrasena de PostgreSQL | Ambos |
| `PG_DATABASE` | Nombre de la BD | Ambos |
| `IPTV_API_URL` | URL de iptv-api | Este scrapper |
| `API_SECRET_KEY` | Clave compartida con iptv-api | Ambos |
| `TMDB_API_KEY` | API key de TMDB | Este scrapper |
| `TMDB_READ_TOKEN` | Read token de TMDB | Este scrapper |
| `JWT_SECRET` | Secret para JWT | Ambos |

### 4.4 Dependencia de iptv-db

Este scrapper importa modelos ORM de `iptv-db`:
- `from iptv_db.models import Channel, MovieCatalog, ...`
- `from iptv_db.engine import get_session_factory, get_sync_session_factory`

iptv-db se instala via pip desde el requirements file:
```
iptv-db @ git+https://github.com/alejandrofm98/iptv-db.git@<commit_hash>
```

Cuando se actualiza iptv-db (nuevos modelos, columnas), hay que actualizar el hash
en todos los requirements files (ver seccion 5).

## 5. Configuracion y secretos

`scripts/config.py` carga `.env` via python-dotenv. Las variables de BD vienen de env vars o del docker/.env.

**Reglas:**
- Nunca commitear `.env` ni credenciales.
- `docker/.env` tiene PG real (ya en `.gitignore`, no se commitea — FALSO POSITIVO resuelto en F0).
- `actualiza_epg.py` usa `EPG_USER` y `EPG_PASS` via env vars.
- `docker/.env-example` es la referencia commiteable.

## 6. Lint, formato, tipos y calidad

Toda la config vive en `pyproject.toml`.

### 6.1 Comandos

```bash
ruff format scripts/
ruff check scripts/ --fix
mypy scripts/ --config-file pyproject.toml
vulture scripts/ --min-confidence 80
pytest tests/
```

### 6.2 Reglas activas (resumen)

- Line length: 100.
- Target Python: 3.12.
- Selectors ruff: E, F, W, I, UP, B, SIM, RUF.
- vulture: umbral 80%.

### 6.3 Pre-commit y CI

Configurados en F4a. Instalar local: `pre-commit install`. CI: ver `.github/workflows/ci.yml`.

### 6.4 Como correr TODO

```bash
ruff format scripts/ && ruff check scripts/ --fix && mypy scripts/ --config-file pyproject.toml && vulture scripts/ --min-confidence 80 && pytest tests/
```

## 7. Testing

- Framework: pytest.
- Tests en `tests/`: conftest.py con fixtures para asyncpg mock, settings mock, datos de ejemplo.
- Patrones:
  - Mockear asyncpg/DB — nada de red en tests unitarios.
  - Tests de normalizacion de sync_iptv (idiomas, paises, calidad).
  - Tests de parsing de HTML del scrapper.
  - Tests de singleton de DatabasePG.

## 8. Despliegue (Dokploy)

Este proyecto usa **Dokploy** para despliegue continuo. Cada `git push` a `master`
despliega automaticamente el nuevo codigo en produccion.

- **NO es necesario** hacer deploy manual, SSH al servidor, docker compose, etc.
- **Solo hacer `git push`** — Dokploy detecta el push y reconstruye + reinicia el contenedor.
- Los crons de Ofelia se configuran en `docker-compose.yaml` y se actualizan automaticamente con cada deploy.
- Tener en cuenta: cualquier push a `master` va a produccion inmediatamente.
  Si necesitas probar sin afectar produccion, trabajar en una rama y hacer PR.

## 9. Criterios para cambios

1. No romper el pipeline de Ofelia (cron). Cada cambio debe preserving el `if __name__ == "__main__"` guard.
2. Si modificas tablas o esquemas, actualizar `iptv-db/` (Alembic migration). `database/schema.sql.legacy` es solo referencia historica.
3. Si agregas un script nuevo, registrarlo en docker-compose.yaml y en este archivo.
4. Evitar cambios de estilo en archivos no tocados.

## 10. Roadmap (no en esta iteracion)

### Completado
- ~~Activar pre-commit~~ (F4a)
- ~~CI en GitHub Actions~~ (F4a)
- ~~Migrar DataManagerSupabase a un nombre que refleje que usa PostgreSQL~~ (decidido mantener)
- F3: scrapper migrado de asyncpg/psycopg2 a iptv-db
- F1.5: schema.sql renombrado a .legacy (Alembic unificado en iptv-db)

### Pendiente
1. Migrar los 4 modulos de `services/` legacy a `app/services/`... [este era iptv-api]
2. ~~Eliminar dead code: Firebase shim (database.py:568-799)~~ (hecho en F3, ReplayManager eliminado)
3. Migrar `print()` con emojis a `logging` framework.
4. Agregar mas tests (target: 80% coverage en sync_iptv y scrapper).
5. Mover `actualiza_epg.py` a Ofelia schedule.

### Cancelado
- F4b: tests con testcontainers

## 11. Checklist antes de cerrar una tarea

1. Codigo formateado: `ruff format` sin diffs.
2. Lint limpio: `ruff check` sin warnings.
3. Tipos: `mypy` sin errores en modulos tocados.
4. Tests: `pytest tests/` pasa.
5. Sin credenciales hardcodeadas (verificar con `grep -rn "password\|secret\|key\|token" scripts/ --include="*.py" | grep -v "env\|config\|ENV"`).
6. Sin emojis en codigo ni comentarios nuevos.
7. Si tocaste tablas o esquemas: generar migration en iptv-db (Alembic) y actualizar hash en requirements.
8. Verificar deploy post-push (ver seccion 12).

## 12. Verificacion de deploy post-push

Dokploy detecta pushes y redesplega automaticamente. Despues de cada push a la branch
principal, verificar que el deploy fue exitoso.

### Acceso al server de Dokploy

El server de Dokploy es accesible via SSH usando un alias configurado en `~/.ssh/config`
(alias: `pro`). El orquestador puede acceder cuando se lo pidas explicitamente.

**No commitees** informacion sensible sobre el server (IPs, paths de claves, etc.) en este
AGENTS.md. Si necesitas ver que comandos usar, consulta la config SSH local o pregunta al
orquestador.

### Checklist post-push

Despues de hacer push a la branch principal (`master` en este repo):

- [ ] Esperar 1-2 minutos para que Dokploy detecte el push
- [ ] Verificar que el deploy fue exitoso (ver "Comandos utiles" abajo)
- [ ] Si hay errores, pedirle al orquestador que investigue via SSH y proponga un fix
- [ ] Si el fix es trivial, aplicarlo, commitear, pushear
- [ ] Verificar que el siguiente deploy (triggered por el push del fix) ahora funcione

### Comandos utiles (via SSH como `pro`)

```bash
# Ver el ultimo log de deploy de una app
ssh pro "ls -t /etc/dokploy/logs/<app-name>/ | head -1 | xargs -I {} cat /etc/dokploy/logs/<app-name>/{}"

# Ver el estado actual de los containers
ssh pro "docker ps -a --format 'table {{.Names}}\t{{.Status}}' | grep walactv"

# Ver logs en vivo de un container
ssh pro "docker logs --tail 50 <container-name>"

# Ver logs de un cron especifico (Ofelia)
ssh pro "journalctl -u ofelia-scheduler --since '10 minutes ago'"
```

Reemplazar `<app-name>` y `<container-name>` segun corresponda:
- **App name en Dokploy**: `walactv-scrapper-gbhx9q`
- **Containers**: `walactv-sync-iptv`, `walactv-sync-replays`, `walactv-futboltv`, `walactv-imdb-*`, `walactv-sync-tmdb-metadata`

### Errores comunes en deploys

| Error | Causa | Fix |
|-------|-------|-----|
| `ModuleNotFoundError: No module named 'iptv_db'` | Falta `iptv-db` en requirements | Agregar `iptv-db @ git+https://...` al requirements file del cron que fallo |
| `fatal: remote error: upload-pack: not our ref <hash>` | Hash de iptv-db incorrecto | Verificar hash via `git ls-remote origin main` en iptv-db y corregir |
| `Cannot find command 'git'` en Docker | Falta `git` en el Dockerfile | Agregar `git` al `apk add --no-cache` (Alpine) |
| Container `Up` pero crons fallan | Variable de entorno faltante o BD no accesible | Verificar `docker logs` del container y env vars en Dokploy |

### Auto-correccion via orquestador

Si queres que el orquestador verifique el deploy por vos:
1. Decile "verifica el deploy de walactv-scrapper"
2. El orquestador se conecta via SSH y lee los logs
3. Si hay errores, los analiza y propone un fix
4. Vos decis si aplicar el fix o no
5. El orquestador commitearia y pushearia solo si vos lo autorizas

NO dar acceso automatico al orquestador sin autorizacion explicita. El orquestador solo
actua cuando se le pide.
