-- ==========================================
-- Schema SQL Simplificado para PostgreSQL
-- WALACTV Scrapper - Versión simplificada
-- ==========================================

-- ==========================================
-- 0. Tabla de configuración
-- ==========================================
CREATE TABLE IF NOT EXISTS config (
    key VARCHAR(100) PRIMARY KEY,
    value TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ==========================================
-- 0b. Tabla de metadata de sincronización
-- ==========================================
CREATE TABLE IF NOT EXISTS sync_metadata (
    id VARCHAR(50) PRIMARY KEY,
    ultima_actualizacion TIMESTAMP WITH TIME ZONE,
    total_canales INT DEFAULT 0,
    total_movies INT DEFAULT 0,
    total_series INT DEFAULT 0,
    m3u_template_path TEXT,
    m3u_template_filename TEXT,
    m3u_size_mb NUMERIC(10,2),
    channels_con_logo INT DEFAULT 0,
    channels_sin_logo INT DEFAULT 0,
    movies_con_logo INT DEFAULT 0,
    movies_sin_logo INT DEFAULT 0,
    series_con_logo INT DEFAULT 0,
    series_sin_logo INT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ==========================================
-- 1. Tabla de canales IPTV (sincronizada desde M3U)
-- ==========================================
CREATE TABLE IF NOT EXISTS channels (
    id VARCHAR(50) PRIMARY KEY,
    numero INT,
    provider_id VARCHAR(50),
    nombre VARCHAR(255) NOT NULL,
    nombre_normalizado VARCHAR(255),
    logo TEXT,
    url TEXT NOT NULL,
    stream_url TEXT,
    grupo VARCHAR(255),
    grupo_normalizado VARCHAR(255),
    country VARCHAR(10),
    tvg_id VARCHAR(100),
    estado_stream TEXT,
    ultimo_chequeo TIMESTAMP WITH TIME ZONE,
    tiempo_respuesta_ms INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_channels_grupo ON channels(grupo);
CREATE INDEX IF NOT EXISTS idx_channels_grupo_normalizado ON channels(grupo_normalizado);
CREATE INDEX IF NOT EXISTS idx_channels_nombre_normalizado ON channels(nombre_normalizado);
CREATE INDEX IF NOT EXISTS idx_channels_country ON channels(country);

-- ==========================================
-- 2. NUEVA: Tabla de mapeos simplificada
-- Unifica: nombre_futboltv + display_name + channel_ids
-- ==========================================
CREATE TABLE IF NOT EXISTS channel_mappings (
    id BIGSERIAL PRIMARY KEY,
    source_name TEXT NOT NULL UNIQUE,      -- Nombre en futbolenlatv (ej: "DAZN 1 HD")
    display_name TEXT NOT NULL,             -- Nombre en la web (ej: "DAZN 1")
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mappings_source ON channel_mappings(source_name);
CREATE INDEX IF NOT EXISTS idx_mappings_display ON channel_mappings(display_name);

-- ==========================================
-- 3. NUEVA: Tabla de variantes/calidades
-- Relaciona un mapeo con múltiples channel_ids (FHD, HD, etc.)
-- ==========================================
CREATE TABLE IF NOT EXISTS channel_variants (
    id BIGSERIAL PRIMARY KEY,
    mapping_id BIGINT NOT NULL REFERENCES channel_mappings(id) ON DELETE CASCADE,
    channel_id VARCHAR(50) REFERENCES channels(id) ON DELETE CASCADE,
    quality TEXT DEFAULT 'HD',              -- FHD, HD, SD, 4K
    priority INTEGER DEFAULT 0,             -- 0 = mejor calidad
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_variants_mapping ON channel_variants(mapping_id);
CREATE INDEX IF NOT EXISTS idx_variants_channel ON channel_variants(channel_id);
CREATE INDEX IF NOT EXISTS idx_variants_priority ON channel_variants(priority);

-- ==========================================
-- 4. Tabla de calendario
-- ==========================================
CREATE TABLE IF NOT EXISTS calendario (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    fecha DATE NOT NULL,
    hora TEXT NOT NULL,
    competicion TEXT,
    subtitulo_competicion TEXT,
    categoria TEXT,
    equipos TEXT NOT NULL,
    imagen_evento TEXT,
    canales TEXT[],                         -- Array de source_names
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(fecha, hora, equipos)
);

CREATE INDEX IF NOT EXISTS idx_calendario_fecha ON calendario(fecha);
CREATE INDEX IF NOT EXISTS idx_calendario_categoria ON calendario(categoria);
-- ==========================================
-- 5. Tablas de películas y series (sin cambios)
-- ==========================================
CREATE TABLE IF NOT EXISTS movies (
    id VARCHAR(50) PRIMARY KEY,
    numero INT,
    provider_id VARCHAR(50),
    nombre TEXT NOT NULL,
    nombre_normalizado TEXT,
    nombre_dedup_key TEXT,
    logo TEXT,
    url TEXT NOT NULL,
    stream_url TEXT,
    grupo TEXT,
    grupo_normalizado TEXT,
    country VARCHAR(10),
    tvg_id VARCHAR(100),
    year INT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS series (
    id VARCHAR(50) PRIMARY KEY,
    numero INT,
    provider_id VARCHAR(50),
    nombre TEXT NOT NULL,
    nombre_normalizado TEXT,
    nombre_dedup_key TEXT,
    serie_name VARCHAR(255),
    series_key TEXT,
    logo TEXT,
    url TEXT NOT NULL,
    stream_url TEXT,
    grupo TEXT,
    grupo_normalizado TEXT,
    country VARCHAR(10),
    tvg_id VARCHAR(100),
    temporada VARCHAR(10),
    episodio VARCHAR(10),
    year INT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

ALTER TABLE sync_metadata ADD COLUMN IF NOT EXISTS channels_generated_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE sync_metadata ADD COLUMN IF NOT EXISTS channels_json_size_mb NUMERIC(10,2);
ALTER TABLE sync_metadata ADD COLUMN IF NOT EXISTS movies_generated_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE sync_metadata ADD COLUMN IF NOT EXISTS movies_json_size_mb NUMERIC(10,2);
ALTER TABLE sync_metadata ADD COLUMN IF NOT EXISTS series_generated_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE sync_metadata ADD COLUMN IF NOT EXISTS series_json_size_mb NUMERIC(10,2);

ALTER TABLE channels ADD COLUMN IF NOT EXISTS nombre_normalizado VARCHAR(255);
ALTER TABLE channels ADD COLUMN IF NOT EXISTS grupo_normalizado VARCHAR(255);
ALTER TABLE channels ADD COLUMN IF NOT EXISTS stream_url TEXT;
ALTER TABLE channels ADD COLUMN IF NOT EXISTS estado_stream TEXT;
ALTER TABLE channels ADD COLUMN IF NOT EXISTS ultimo_chequeo TIMESTAMP WITH TIME ZONE;
ALTER TABLE channels ADD COLUMN IF NOT EXISTS tiempo_respuesta_ms INTEGER;
ALTER TABLE movies ADD COLUMN IF NOT EXISTS nombre_normalizado TEXT;
ALTER TABLE movies ADD COLUMN IF NOT EXISTS grupo_normalizado TEXT;
ALTER TABLE movies ADD COLUMN IF NOT EXISTS stream_url TEXT;
ALTER TABLE movies ADD COLUMN IF NOT EXISTS nombre_dedup_key TEXT;
ALTER TABLE series ADD COLUMN IF NOT EXISTS nombre_normalizado TEXT;
ALTER TABLE series ADD COLUMN IF NOT EXISTS grupo_normalizado TEXT;
ALTER TABLE series ADD COLUMN IF NOT EXISTS stream_url TEXT;
ALTER TABLE series ADD COLUMN IF NOT EXISTS nombre_dedup_key TEXT;
ALTER TABLE series ADD COLUMN IF NOT EXISTS series_key TEXT;

CREATE INDEX IF NOT EXISTS idx_movies_grupo_normalizado ON movies(grupo_normalizado);
CREATE INDEX IF NOT EXISTS idx_movies_nombre_normalizado ON movies(nombre_normalizado);
CREATE INDEX IF NOT EXISTS idx_movies_nombre_dedup_key ON movies(nombre_dedup_key);
CREATE INDEX IF NOT EXISTS idx_series_grupo_normalizado ON series(grupo_normalizado);
CREATE INDEX IF NOT EXISTS idx_series_nombre_normalizado ON series(nombre_normalizado);
CREATE INDEX IF NOT EXISTS idx_series_nombre_dedup_key ON series(nombre_dedup_key);
CREATE INDEX IF NOT EXISTS idx_series_series_key ON series(series_key);

-- ==========================================
-- 5b. Tablas de catálogo normalizadas (series y películas)
-- series_catalog: 1 fila por serie (sin idioma)
-- series_episodes: 1 fila por episodio lógico
-- series_streams: variantes idioma/calidad de cada episodio
-- movies_catalog: 1 fila por película (sin idioma)
-- movie_streams: variantes idioma/calidad de cada película
-- ==========================================

CREATE TABLE IF NOT EXISTS series_catalog (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title TEXT NOT NULL,
    series_key TEXT NOT NULL,
    canonical_key VARCHAR,
    provider_id VARCHAR(50),
    tmdb_id VARCHAR(20),
    nombre_dedup_key TEXT,
    year INT,
    countries VARCHAR(10)[] DEFAULT '{}',
    group_normalizado TEXT,
    logo TEXT,
    not_found BOOLEAN DEFAULT FALSE,
    retry_count INT DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT fk_series_catalog_tmdb FOREIGN KEY (tmdb_id)
        REFERENCES series_metadata(tmdb_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS series_episodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    catalog_id UUID NOT NULL REFERENCES series_catalog(id) ON DELETE CASCADE,
    season_number INT NOT NULL,
    episode_number INT NOT NULL,
    title TEXT,
    overview TEXT,
    air_date DATE,
    still_path VARCHAR(255),
    numero INT,
    UNIQUE(catalog_id, season_number, episode_number)
);

CREATE TABLE IF NOT EXISTS series_streams (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    episode_id UUID NOT NULL REFERENCES series_episodes(id) ON DELETE CASCADE,
    country VARCHAR(10) NOT NULL,
    quality VARCHAR(10),
    provider_id VARCHAR(50),
    stream_url TEXT NOT NULL,
    url TEXT,
    label TEXT,
    numero INT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS movies_catalog (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title TEXT NOT NULL,
    provider_id VARCHAR(50),
    tmdb_id VARCHAR(20),
    nombre_dedup_key TEXT,
    canonical_key VARCHAR,
    year INT,
    countries VARCHAR(10)[] DEFAULT '{}',
    group_normalizado TEXT,
    logo TEXT,
    not_found BOOLEAN DEFAULT FALSE,
    retry_count INT DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT fk_movies_catalog_tmdb FOREIGN KEY (tmdb_id)
        REFERENCES movies_metadata(tmdb_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS movie_streams (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    movie_id UUID NOT NULL REFERENCES movies_catalog(id) ON DELETE CASCADE,
    country VARCHAR(10) NOT NULL,
    quality VARCHAR(10),
    provider_id VARCHAR(50),
    stream_url TEXT NOT NULL,
    url TEXT,
    label TEXT,
    numero INT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_series_catalog_series_key ON series_catalog(series_key);
CREATE INDEX IF NOT EXISTS idx_series_catalog_tmdb ON series_catalog(tmdb_id);
CREATE INDEX IF NOT EXISTS idx_series_catalog_group ON series_catalog(group_normalizado);
CREATE INDEX IF NOT EXISTS idx_series_catalog_countries ON series_catalog USING gin(countries);
CREATE INDEX IF NOT EXISTS idx_series_catalog_year ON series_catalog(year);
CREATE INDEX IF NOT EXISTS idx_series_episodes_catalog ON series_episodes(catalog_id);
CREATE INDEX IF NOT EXISTS idx_series_streams_episode ON series_streams(episode_id);
CREATE INDEX IF NOT EXISTS idx_series_streams_country ON series_streams(country);
CREATE INDEX IF NOT EXISTS idx_movies_catalog_tmdb ON movies_catalog(tmdb_id);
CREATE INDEX IF NOT EXISTS idx_movies_catalog_dedup ON movies_catalog(nombre_dedup_key);
CREATE INDEX IF NOT EXISTS idx_movies_catalog_group ON movies_catalog(group_normalizado);
CREATE INDEX IF NOT EXISTS idx_movies_catalog_countries ON movies_catalog USING gin(countries);
CREATE INDEX IF NOT EXISTS idx_movies_catalog_year ON movies_catalog(year);
CREATE INDEX IF NOT EXISTS idx_movie_streams_movie ON movie_streams(movie_id);
CREATE INDEX IF NOT EXISTS idx_movie_streams_country ON movie_streams(country);
CREATE INDEX IF NOT EXISTS idx_movies_catalog_canonical ON movies_catalog(canonical_key);
CREATE INDEX IF NOT EXISTS idx_series_catalog_canonical ON series_catalog(canonical_key);

-- ==========================================
-- 5c. Migración: scrape state pasa de metadata → catalog
-- ==========================================

ALTER TABLE movies_catalog ADD COLUMN IF NOT EXISTS not_found BOOLEAN DEFAULT FALSE;
ALTER TABLE movies_catalog ADD COLUMN IF NOT EXISTS retry_count INT DEFAULT 0;
ALTER TABLE movies_catalog ADD COLUMN IF NOT EXISTS last_error TEXT;

ALTER TABLE series_catalog ADD COLUMN IF NOT EXISTS not_found BOOLEAN DEFAULT FALSE;
ALTER TABLE series_catalog ADD COLUMN IF NOT EXISTS retry_count INT DEFAULT 0;
ALTER TABLE series_catalog ADD COLUMN IF NOT EXISTS last_error TEXT;

UPDATE movies_catalog mc
SET not_found = mm.not_found,
    retry_count = COALESCE(mm.retry_count, 0),
    last_error = mm.last_error
FROM movies_metadata mm
WHERE mm.provider_id = mc.provider_id
  AND (mm.not_found = TRUE OR mm.retry_count > 0 OR mm.last_error IS NOT NULL);

UPDATE series_catalog sc
SET not_found = sm.not_found,
    retry_count = COALESCE(sm.retry_count, 0),
    last_error = sm.last_error
FROM series_metadata sm
WHERE sm.series_key = sc.series_key
  AND (sm.not_found = TRUE OR sm.retry_count > 0 OR sm.last_error IS NOT NULL);

ALTER TABLE movies_metadata DROP COLUMN IF EXISTS provider_id;
ALTER TABLE movies_metadata DROP COLUMN IF EXISTS not_found;
ALTER TABLE movies_metadata DROP COLUMN IF EXISTS retry_count;
ALTER TABLE movies_metadata DROP COLUMN IF EXISTS last_error;

ALTER TABLE series_metadata DROP COLUMN IF EXISTS series_key;
ALTER TABLE series_metadata DROP COLUMN IF EXISTS not_found;
ALTER TABLE series_metadata DROP COLUMN IF EXISTS retry_count;
ALTER TABLE series_metadata DROP COLUMN IF EXISTS last_error;

DROP INDEX IF EXISTS idx_movies_metadata_provider;
DROP INDEX IF EXISTS idx_movies_metadata_provider_nf;
DROP INDEX IF EXISTS idx_movies_metadata_not_found;
DROP INDEX IF EXISTS idx_series_metadata_serieskey;
DROP INDEX IF EXISTS idx_series_metadata_serieskey_nf;
DROP INDEX IF EXISTS idx_series_metadata_not_found;

CREATE INDEX IF NOT EXISTS idx_movies_catalog_scrape ON movies_catalog(not_found, retry_count, year DESC);
CREATE INDEX IF NOT EXISTS idx_series_catalog_scrape ON series_catalog(not_found, retry_count, year DESC);

-- ==========================================
-- 6. Tabla de replays externos
-- ==========================================
CREATE TABLE IF NOT EXISTS replays (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    slug TEXT NOT NULL UNIQUE,
    source_site TEXT NOT NULL DEFAULT 'watch-wrestling.eu',
    title TEXT NOT NULL,
    event_name TEXT,
    event_type TEXT,
    event_date DATE,
    post_url TEXT NOT NULL,
    featured_image_url TEXT,
    description TEXT,
    video_sources JSONB NOT NULL DEFAULT '[]'::jsonb,
    match_card JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_replays_source_site ON replays(source_site);
CREATE INDEX IF NOT EXISTS idx_replays_event_date ON replays(event_date DESC);
CREATE INDEX IF NOT EXISTS idx_replays_event_type ON replays(event_type);

-- ==========================================
-- RLS deshabilitado (PostgreSQL propio, no Supabase)
-- ==========================================
-- Las políticas RLS de Supabase no aplican aquí
-- Si necesitas seguridad, configura pg_hba.conf y GRANTs

-- ==========================================
-- Funciones utilitarias simplificadas
-- ==========================================

-- Trigger para updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_mappings_updated_at BEFORE UPDATE ON channel_mappings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_calendario_updated_at BEFORE UPDATE ON calendario
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_replays_updated_at BEFORE UPDATE ON replays
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ==========================================
-- Función principal: Obtener evento con channels resueltos
-- ==========================================
CREATE OR REPLACE FUNCTION get_evento_con_channels(p_calendario_id UUID)
RETURNS TABLE (
    id UUID,
    fecha DATE,
    hora TEXT,
    competicion TEXT,
    subtitulo_competicion TEXT,
    categoria TEXT,
    equipos TEXT,
    imagen_evento TEXT,
    canales_original TEXT[],
    canales_resueltos JSONB
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.id,
        c.fecha,
        c.hora,
        c.competicion,
        c.subtitulo_competicion,
        c.categoria,
        c.equipos,
        c.imagen_evento,
        c.canales as canales_original,
        COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'channel_id', cv.channel_id,
                    'display_name', cm.display_name,
                    'quality', cv.quality,
                    'priority', cv.priority,
                    'source_name', cm.source_name
                ) ORDER BY cm.source_name, cv.priority
                ) FILTER (WHERE cv.channel_id IS NOT NULL AND (ch.estado_stream IS NULL OR ch.estado_stream = 'ok')),
                    '[]'::jsonb
                ) as canales_resueltos
            FROM calendario c
            LEFT JOIN unnest(c.canales) AS canal_nombre ON true
            LEFT JOIN channel_mappings cm ON cm.source_name = canal_nombre
            LEFT JOIN channel_variants cv ON cv.mapping_id = cm.id
            LEFT JOIN channels ch ON cv.channel_id = ch.id
            WHERE c.id = p_calendario_id
    GROUP BY c.id, c.fecha, c.hora, c.competicion, c.subtitulo_competicion,
        c.categoria, c.equipos, c.imagen_evento, c.canales;
END;
$$ LANGUAGE plpgsql;

-- ==========================================
-- Función: Obtener eventos por fecha con channels
-- ==========================================
CREATE OR REPLACE FUNCTION get_eventos_fecha_con_channels(p_fecha DATE)
RETURNS TABLE (
    id UUID,
    fecha DATE,
    hora TEXT,
    competicion TEXT,
    subtitulo_competicion TEXT,
    categoria TEXT,
    equipos TEXT,
    imagen_evento TEXT,
    canales_original TEXT[],
    canales_resueltos JSONB
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.id,
        c.fecha,
        c.hora,
        c.competicion,
        c.subtitulo_competicion,
        c.categoria,
        c.equipos,
        c.imagen_evento,
        c.canales as canales_original,
        COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'channel_id', cv.channel_id,
                    'display_name', cm.display_name,
                    'quality', cv.quality,
                    'priority', cv.priority,
                    'source_name', cm.source_name
                ) ORDER BY cm.source_name, cv.priority
                ) FILTER (WHERE cv.channel_id IS NOT NULL AND (ch.estado_stream IS NULL OR ch.estado_stream = 'ok')),
                    '[]'::jsonb
                ) as canales_resueltos
            FROM calendario c
            LEFT JOIN unnest(c.canales) AS canal_nombre ON true
            LEFT JOIN channel_mappings cm ON cm.source_name = canal_nombre
            LEFT JOIN channel_variants cv ON cv.mapping_id = cm.id
            LEFT JOIN channels ch ON cv.channel_id = ch.id
            WHERE c.fecha = p_fecha
    GROUP BY c.id, c.fecha, c.hora, c.competicion, c.subtitulo_competicion,
        c.categoria, c.equipos, c.imagen_evento, c.canales
    ORDER BY c.hora;
END;
$$ LANGUAGE plpgsql;

-- ==========================================
-- Función: Obtener channel_ids desde array de nombres
-- ==========================================
CREATE OR REPLACE FUNCTION get_channels_from_names(nombres_canales TEXT[])
RETURNS TABLE (
    source_name TEXT,
    display_name TEXT,
    channel_id VARCHAR(50),
    quality TEXT,
    priority INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        cm.source_name,
        cm.display_name,
        cv.channel_id,
        cv.quality,
        cv.priority
    FROM channel_mappings cm
    JOIN channel_variants cv ON cv.mapping_id = cm.id
    LEFT JOIN channels ch ON cv.channel_id = ch.id
    WHERE cm.source_name = ANY(nombres_canales)
      AND (ch.estado_stream IS NULL OR ch.estado_stream = 'ok')
    ORDER BY cm.source_name, cv.priority;
END;
$$ LANGUAGE plpgsql;

-- ==========================================
-- Función: Insertar mapeo completo (conveniencia)
-- ==========================================
CREATE OR REPLACE FUNCTION insert_channel_mapping(
    p_source_name TEXT,
    p_display_name TEXT,
    p_channel_ids TEXT[],  -- Array de channel_ids
    p_qualities TEXT[] DEFAULT NULL  -- Array de calidades (opcional)
)
RETURNS BIGINT AS $$
DECLARE
    v_mapping_id BIGINT;
    v_quality TEXT;
    v_channel_id TEXT;
    i INTEGER;
BEGIN
    -- Insertar o actualizar mapeo
    INSERT INTO channel_mappings (source_name, display_name)
    VALUES (p_source_name, p_display_name)
    ON CONFLICT (source_name) DO UPDATE
    SET display_name = EXCLUDED.display_name
    RETURNING id INTO v_mapping_id;
    
    -- Eliminar variantes antiguas
    DELETE FROM channel_variants WHERE mapping_id = v_mapping_id;
    
    -- Insertar nuevas variantes
    FOR i IN 1..array_length(p_channel_ids, 1) LOOP
        v_channel_id := p_channel_ids[i];
        v_quality := COALESCE(p_qualities[i], 'HD');
        
        INSERT INTO channel_variants (mapping_id, channel_id, quality, priority)
        VALUES (v_mapping_id, v_channel_id, v_quality, i-1);
    END LOOP;
    
    RETURN v_mapping_id;
END;
$$ LANGUAGE plpgsql;

-- ==========================================
-- Tabla: Metadatos de TMDB para películas
-- ==========================================
CREATE TABLE IF NOT EXISTS movies_metadata (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tmdb_id VARCHAR(20) UNIQUE,
    overview_es TEXT,
    overview_en TEXT,
    vote_average FLOAT,
    vote_count INT,
    title VARCHAR(255),
    original_title VARCHAR(255),
    release_date DATE,
    year INT,
    runtime_minutes INT,
    genres TEXT[],
    poster_path VARCHAR(255),
    backdrop_path VARCHAR(255),
    tagline VARCHAR(500),
    popularity FLOAT,
    status VARCHAR(50),
    tmdb_data JSONB,
    scraped_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_movies_metadata_tmdb ON movies_metadata(tmdb_id);
CREATE INDEX IF NOT EXISTS idx_movies_metadata_year ON movies_metadata(year);

-- ==========================================
-- Tabla: Metadatos TMDB a nivel serie
-- ==========================================
CREATE TABLE IF NOT EXISTS series_metadata (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tmdb_id VARCHAR(20) UNIQUE,
    overview_es TEXT,
    overview_en TEXT,
    vote_average FLOAT,
    vote_count INT,
    title VARCHAR(255),
    original_title VARCHAR(255),
    release_date DATE,
    year INT,
    genres TEXT[],
    poster_path VARCHAR(255),
    backdrop_path VARCHAR(255),
    tagline VARCHAR(500),
    popularity FLOAT,
    status VARCHAR(50),
    tmdb_data JSONB,
    scraped_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_series_metadata_tmdb ON series_metadata(tmdb_id);
CREATE INDEX IF NOT EXISTS idx_series_metadata_year ON series_metadata(year);

-- Trigger para actualizar updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_movies_metadata_updated_at ON movies_metadata;
CREATE TRIGGER update_movies_metadata_updated_at
    BEFORE UPDATE ON movies_metadata
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_series_metadata_updated_at ON series_metadata;
CREATE TRIGGER update_series_metadata_updated_at
    BEFORE UPDATE ON series_metadata
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ==========================================
-- 12. Fallos del scraper TMDB (persistente, no lo borra el sync)
-- ==========================================
CREATE TABLE IF NOT EXISTS scraper_failures (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider_id VARCHAR(50),
    series_key VARCHAR(255),
    title TEXT NOT NULL,
    year INT,
    error_message TEXT,
    failed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    retry_count INT DEFAULT 1,
    last_retry_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_failures_provider ON scraper_failures(provider_id) WHERE provider_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_failures_series ON scraper_failures(series_key) WHERE series_key IS NOT NULL;

-- ==========================================
-- 13. Columnas adicionales para series_episodes (TMDB episode metadata)
-- ==========================================
ALTER TABLE series_episodes ADD COLUMN IF NOT EXISTS title_en TEXT;
ALTER TABLE series_episodes ADD COLUMN IF NOT EXISTS overview_en TEXT;
ALTER TABLE series_episodes ADD COLUMN IF NOT EXISTS runtime INTEGER;
ALTER TABLE series_episodes ADD COLUMN IF NOT EXISTS vote_average NUMERIC(3,1);
ALTER TABLE series_episodes ADD COLUMN IF NOT EXISTS vote_count INTEGER;
ALTER TABLE series_episodes ADD COLUMN IF NOT EXISTS episode_type VARCHAR(50);
ALTER TABLE series_episodes ADD COLUMN IF NOT EXISTS tmdb_checked BOOLEAN DEFAULT FALSE;
