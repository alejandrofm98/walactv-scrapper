# Integración IntroDB

## Estado

El scrapper consulta `GET /segments` de IntroDB para episodios que tienen un
IMDb de episodio (`series_episodes.imdb_id`). Las lecturas son puntuales y se
cachean durante 30 días; no se descarga la base completa.

## Migración requerida en `iptv-db`

Antes de ejecutar `sync_introdb_segments.py`, `iptv-db` debe incluir:

El hash de `iptv-db` usado en `docker/config/requirements-tmdb.txt` debe
actualizarse al commit que publique esta migración. No se debe cambiar al hash
actual hasta que la migración esté subida al repositorio remoto.

```sql
ALTER TABLE series_episodes ADD COLUMN imdb_id VARCHAR(20);

CREATE INDEX ix_series_episodes_imdb_id
    ON series_episodes(imdb_id);

CREATE TABLE video_segment_sync (
    episode_id UUID NOT NULL REFERENCES series_episodes(id) ON DELETE CASCADE,
    source VARCHAR(30) NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    not_found BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (episode_id, source)
);

CREATE TABLE video_segments (
    id UUID PRIMARY KEY,
    episode_id UUID NOT NULL REFERENCES series_episodes(id) ON DELETE CASCADE,
    segment_type VARCHAR(10) NOT NULL
        CHECK (segment_type IN ('intro', 'recap', 'outro')),
    start_ms BIGINT NOT NULL CHECK (start_ms >= 0),
    end_ms BIGINT NOT NULL CHECK (end_ms > start_ms),
    confidence NUMERIC(4, 3),
    submission_count INTEGER,
    source VARCHAR(30) NOT NULL,
    source_updated_at TIMESTAMPTZ,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ,
    UNIQUE (episode_id, segment_type, source)
);
```

`video_segment_sync` es necesario para recordar también los episodios que no
tienen datos en IntroDB. Sin esta tabla se volverían a consultar en cada
ejecución.

## Clientes incluidos

La funcionalidad VOD se integra en `WalacTV` (Android TV) y
`WalacTV-Desktop` (Tauri/libmpv). Ambos consumen `imdb_id` y
`skip_segments` desde `iptv-api` y muestran el salto de intro, recap y outro.

`walactvWeb` queda explícitamente fuera de esta funcionalidad: ese proyecto
solo gestiona canales en directo y eventos.

## Contrato de `iptv-api`

Los episodios devuelven:

```json
{
  "imdb_id": "tt0944947",
  "skip_segments": {
    "intro": {
      "start_ms": 437000,
      "end_ms": 531000,
      "confidence": 1.0,
      "submission_count": 2
    },
    "recap": null,
    "outro": null
  }
}
```

Los clientes toleran `skip_segments: null`. No deben consultar IntroDB
directamente cuando el backend ya proporciona el campo.

## Ejecución local

```bash
python iptv_scrapper/sync_introdb_segments.py --limit 20 --dry-run
python iptv_scrapper/sync_introdb_segments.py --limit 500
```

En Docker el job de Ofelia está programado a las 07:00 UTC y permanece
desactivado por defecto. Activarlo después de aplicar la migración de
`iptv-db` con `INTRODB_SYNC_ENABLED=true`.

El resultado debe exponerse después desde `iptv-api` como `skip_segments` en
la respuesta de cada episodio. Los clientes no deberían consultar IntroDB
directamente.
