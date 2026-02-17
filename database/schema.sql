-- ==========================================
-- Schema SQL Normalizado para Supabase
-- Estructura relacional para WALACTV Scrapper
-- NOTA: Usa tabla 'channels' existente
-- IDs numéricos autoincrementales (BIGSERIAL)
-- ==========================================

-- ==========================================
-- 1. Tabla de canales (sincronizada desde IPTV)
-- ==========================================
CREATE TABLE IF NOT EXISTS channels (
    id VARCHAR(50) PRIMARY KEY,
    numero INT,
    provider_id VARCHAR(50),  -- ID del proveedor (ej: "176861" de la URL)
    nombre VARCHAR(255) NOT NULL,
    logo TEXT,
    url TEXT NOT NULL,
    grupo VARCHAR(255),
    country VARCHAR(10),
    tvg_id VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_channels_grupo ON channels(grupo);
CREATE INDEX IF NOT EXISTS idx_channels_country ON channels(country);
CREATE INDEX IF NOT EXISTS idx_channels_provider_id ON channels(provider_id);

-- ==========================================
-- 2. Tabla de películas (sincronizada desde IPTV)
-- ==========================================
CREATE TABLE IF NOT EXISTS movies (
    id VARCHAR(50) PRIMARY KEY,
    numero INT,
    provider_id VARCHAR(50),  -- ID del proveedor (ej: "2001330" de la URL)
    nombre TEXT NOT NULL,
    logo TEXT,
    url TEXT NOT NULL,
    grupo TEXT,
    country VARCHAR(10),
    tvg_id VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_movies_grupo ON movies(grupo);
CREATE INDEX IF NOT EXISTS idx_movies_country ON movies(country);
CREATE INDEX IF NOT EXISTS idx_movies_provider_id ON movies(provider_id);

-- ==========================================
-- 3. Tabla de series (sincronizada desde IPTV)
-- ==========================================
CREATE TABLE IF NOT EXISTS series (
    id VARCHAR(50) PRIMARY KEY,
    numero INT,
    provider_id VARCHAR(50),  -- ID del proveedor (ej: "1306345" de la URL)
    nombre TEXT NOT NULL,
    serie_name VARCHAR(255),  -- Nombre de la serie (ej: "Breaking Bad")
    logo TEXT,
    url TEXT NOT NULL,
    grupo TEXT,
    country VARCHAR(10),
    tvg_id VARCHAR(100),
    temporada VARCHAR(10),
    episodio VARCHAR(10),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_series_grupo ON series(grupo);
CREATE INDEX IF NOT EXISTS idx_series_country ON series(country);
CREATE INDEX IF NOT EXISTS idx_series_provider_id ON series(provider_id);
CREATE INDEX IF NOT EXISTS idx_series_serie_name ON series(serie_name);

-- ==========================================
-- 4. Tabla de metadata de sincronización
-- ==========================================
CREATE TABLE IF NOT EXISTS sync_metadata (
    id VARCHAR(50) PRIMARY KEY,
    ultima_actualizacion TIMESTAMP WITH TIME ZONE,
    total_canales INT DEFAULT 0,
    total_movies INT DEFAULT 0,
    total_series INT DEFAULT 0,
    m3u_url TEXT,
    m3u_size BIGINT,
    m3u_size_mb DECIMAL(10,2),
    channels_con_logo INT DEFAULT 0,
    channels_sin_logo INT DEFAULT 0,
    movies_con_logo INT DEFAULT 0,
    movies_sin_logo INT DEFAULT 0,
    series_con_logo INT DEFAULT 0,
    series_sin_logo INT DEFAULT 0
);

-- ==========================================
-- 5. Función para truncar tablas (optimización)
-- ==========================================
CREATE OR REPLACE FUNCTION truncate_table(table_name TEXT)
RETURNS VOID AS $$
BEGIN
    EXECUTE 'TRUNCATE TABLE ' || quote_ident(table_name) || ' RESTART IDENTITY CASCADE';
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

DROP TABLE IF EXISTS mapeo_futbolenlatv_canales CASCADE;
DROP TABLE IF EXISTS canales_calidades CASCADE;
DROP TABLE IF EXISTS mapeo_futbolenlatv CASCADE;
DROP TABLE IF EXISTS canales_walactv CASCADE;
DROP TABLE IF EXISTS calendario CASCADE;

-- ==========================================
-- Tabla: canales_walactv
-- Canal padre/comercial que referencia a channels
-- Ejemplo: "DAZN 1" referencia a channel_id "dazn1_hd"
-- ==========================================
CREATE TABLE IF NOT EXISTS canales_walactv (
    id BIGSERIAL PRIMARY KEY,
    nombre TEXT NOT NULL UNIQUE,  -- Ej: "DAZN 1"
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_canales_walactv_nombre ON canales_walactv(nombre);

-- ==========================================
-- Tabla: canales_calidades
-- Variaciones de un canal con diferentes calidades
-- Ej: "ES| DAZN 1 FHD", "ES| DAZN 1 HD"
-- Relación N:1 con canales_walactv
-- ==========================================
CREATE TABLE IF NOT EXISTS canales_calidades (
    id BIGSERIAL PRIMARY KEY,
    canal_walactv_id BIGINT NOT NULL REFERENCES canales_walactv(id) ON DELETE CASCADE,
    channel_id VARCHAR(50) REFERENCES channels(id) ON DELETE SET NULL,
    nombre_iptv TEXT NOT NULL,              -- Nombre completo del canal IPTV
    calidad TEXT,                           -- FHD, HD, SD, 4K, RAW, etc.
    orden INTEGER DEFAULT 0,                -- Orden de preferencia (0 = mejor)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_calidades_canal_id ON canales_calidades(canal_walactv_id);
CREATE INDEX IF NOT EXISTS idx_calidades_calidad ON canales_calidades(calidad);
CREATE INDEX IF NOT EXISTS idx_calidades_channel_id ON canales_calidades(channel_id);
CREATE INDEX IF NOT EXISTS idx_calidades_nombre_iptv ON canales_calidades(nombre_iptv);


-- ==========================================
-- Tabla: mapeo_futbolenlatv
-- Nombres que aparecen en futbolenlatv
-- Ejemplo: "DAZN 1 HD"
-- Origen: mapeoCanalesFutbolEnLaTv de Firebase
-- ==========================================
CREATE TABLE IF NOT EXISTS mapeo_futbolenlatv (
    id BIGSERIAL PRIMARY KEY,
    nombre_futboltv TEXT NOT NULL UNIQUE,   -- Ej: "DAZN 1 HD" (como aparece en futbolenlatv)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mapeo_futboltv_nombre ON mapeo_futbolenlatv(nombre_futboltv);

-- ==========================================
-- Tabla: mapeo_futbolenlatv_canales
-- Relación N:M entre mapeo_futbolenlatv y canales_walactv
-- Un nombre de futbolenlatv puede referenciar varios canales walactv
-- Ejemplo: "DAZN 1 HD" -> ["DAZN 1", "DAZN 1 Bar"]
-- ==========================================
CREATE TABLE IF NOT EXISTS mapeo_futbolenlatv_canales (
    id BIGSERIAL PRIMARY KEY,
    mapeo_futbolenlatv_id BIGINT NOT NULL REFERENCES mapeo_futbolenlatv(id) ON DELETE CASCADE,
    canal_walactv_id BIGINT NOT NULL REFERENCES canales_walactv(id) ON DELETE CASCADE,
    orden INTEGER DEFAULT 0,                -- Orden de preferencia entre los canales
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(mapeo_futbolenlatv_id, canal_walactv_id)
);

CREATE INDEX IF NOT EXISTS idx_mapeo_futboltv_canales_mapeo ON mapeo_futbolenlatv_canales(mapeo_futbolenlatv_id);
CREATE INDEX IF NOT EXISTS idx_mapeo_futboltv_canales_canal ON mapeo_futbolenlatv_canales(canal_walactv_id);

-- ==========================================
-- Tabla: calendario
-- Partidos con enlaces acestream (scraping de futbolenlatv)
-- ==========================================
CREATE TABLE IF NOT EXISTS calendario (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    fecha DATE NOT NULL,
    hora TEXT NOT NULL,
    competicion TEXT,
    categoria TEXT,
    equipos TEXT NOT NULL,  -- "Real Madrid vs Barcelona"
    canales TEXT[],         -- Array de nombres de canales
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(fecha, hora, equipos)
);

CREATE INDEX IF NOT EXISTS idx_calendario_fecha ON calendario(fecha);
CREATE INDEX IF NOT EXISTS idx_calendario_categoria ON calendario(categoria);
CREATE INDEX IF NOT EXISTS idx_calendario_equipos ON calendario USING gin(to_tsvector('spanish', equipos));

-- ==========================================
-- Habilitar RLS (Row Level Security)
-- ==========================================

ALTER TABLE canales_walactv ENABLE ROW LEVEL SECURITY;
ALTER TABLE canales_calidades ENABLE ROW LEVEL SECURITY;
ALTER TABLE mapeo_futbolenlatv ENABLE ROW LEVEL SECURITY;
ALTER TABLE mapeo_futbolenlatv_canales ENABLE ROW LEVEL SECURITY;
ALTER TABLE calendario ENABLE ROW LEVEL SECURITY;

-- Políticas de lectura pública
CREATE POLICY "Allow public read walactv" ON canales_walactv FOR SELECT USING (true);
CREATE POLICY "Allow public read calidades" ON canales_calidades FOR SELECT USING (true);
CREATE POLICY "Allow public read mapeo_futbolenlatv" ON mapeo_futbolenlatv FOR SELECT USING (true);
CREATE POLICY "Allow public read mapeo_futbolenlatv_canales" ON mapeo_futbolenlatv_canales FOR SELECT USING (true);
CREATE POLICY "Allow public read calendario" ON calendario FOR SELECT USING (true);

-- Políticas de escritura para usuarios autenticados
CREATE POLICY "Allow authenticated write walactv" ON canales_walactv FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow authenticated write calidades" ON canales_calidades FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow authenticated write mapeo_futbolenlatv" ON mapeo_futbolenlatv FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow authenticated write mapeo_futbolenlatv_canales" ON mapeo_futbolenlatv_canales FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow authenticated write calendario" ON calendario FOR ALL USING (true) WITH CHECK (true);

-- ==========================================
-- Triggers para actualizar updated_at
-- ==========================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_walactv_updated_at BEFORE UPDATE ON canales_walactv
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_calidades_updated_at BEFORE UPDATE ON canales_calidades
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_mapeo_futbolenlatv_updated_at BEFORE UPDATE ON mapeo_futbolenlatv
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_mapeo_futbolenlatv_canales_updated_at BEFORE UPDATE ON mapeo_futbolenlatv_canales
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_calendario_updated_at BEFORE UPDATE ON calendario
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ==========================================
-- Funciones utilitarias
-- ==========================================

-- Eliminar funciones existentes para poder recrearlas con nuevas firmas
DROP FUNCTION IF EXISTS get_calidades_canal(TEXT);
DROP FUNCTION IF EXISTS resolver_canal_futboltv(TEXT);
DROP FUNCTION IF EXISTS get_canales_por_futboltv(TEXT);
DROP FUNCTION IF EXISTS get_eventos_con_canales(DATE);

-- Obtener todas las calidades de un canal walactv
CREATE OR REPLACE FUNCTION get_calidades_canal(nombre_comercial TEXT)
RETURNS TABLE (
    nombre_iptv TEXT,
    calidad TEXT,
    orden INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        cc.nombre_iptv,
        cc.calidad,
        cc.orden
    FROM canales_calidades cc
    JOIN canales_walactv cw ON cc.canal_walactv_id = cw.id
    WHERE cw.nombre = nombre_comercial
    ORDER BY cc.orden, cc.nombre_iptv;
END;
$$ LANGUAGE plpgsql;

-- Resolver canal desde futbolenlatv: obtener todas las calidades de todos los canales asociados
CREATE OR REPLACE FUNCTION resolver_canal_futboltv(nombre_futboltv TEXT)
RETURNS TABLE (
    nombre_comercial TEXT,
    nombre_iptv TEXT,
    calidad TEXT,
    canal_orden INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        cw.nombre as nombre_comercial,
        cc.nombre_iptv,
        cc.calidad,
        mfc.orden as canal_orden
    FROM mapeo_futbolenlatv mf
    JOIN mapeo_futbolenlatv_canales mfc ON mf.id = mfc.mapeo_futbolenlatv_id
    JOIN canales_walactv cw ON mfc.canal_walactv_id = cw.id
    LEFT JOIN canales_calidades cc ON cw.id = cc.canal_walactv_id
    WHERE mf.nombre_futboltv = nombre_futboltv
    ORDER BY mfc.orden, cc.orden;
END;
$$ LANGUAGE plpgsql;

-- Obtener todos los canales walactv asociados a un nombre de futbolenlatv
CREATE OR REPLACE FUNCTION get_canales_por_futboltv(nombre_futboltv TEXT)
RETURNS TABLE (
    canal_id BIGINT,
    nombre TEXT,
    orden INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        cw.id,
        cw.nombre,
        mfc.orden
    FROM mapeo_futbolenlatv mf
    JOIN mapeo_futbolenlatv_canales mfc ON mf.id = mfc.mapeo_futbolenlatv_id
    JOIN canales_walactv cw ON mfc.canal_walactv_id = cw.id
    WHERE mf.nombre_futboltv = nombre_futboltv
    ORDER BY mfc.orden;
END;
$$ LANGUAGE plpgsql;
