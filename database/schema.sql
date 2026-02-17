-- ==========================================
-- Schema SQL Simplificado para Supabase
-- WALACTV Scrapper - Versión simplificada
-- ==========================================

-- ==========================================
-- 1. Tabla de canales IPTV (sincronizada desde M3U)
-- ==========================================
CREATE TABLE IF NOT EXISTS channels (
    id VARCHAR(50) PRIMARY KEY,
    numero INT,
    provider_id VARCHAR(50),
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
-- 4. Tabla de calendario (sin cambios)
-- ==========================================
CREATE TABLE IF NOT EXISTS calendario (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    fecha DATE NOT NULL,
    hora TEXT NOT NULL,
    competicion TEXT,
    categoria TEXT,
    equipos TEXT NOT NULL,
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
    logo TEXT,
    url TEXT NOT NULL,
    grupo TEXT,
    country VARCHAR(10),
    tvg_id VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS series (
    id VARCHAR(50) PRIMARY KEY,
    numero INT,
    provider_id VARCHAR(50),
    nombre TEXT NOT NULL,
    serie_name VARCHAR(255),
    logo TEXT,
    url TEXT NOT NULL,
    grupo TEXT,
    country VARCHAR(10),
    tvg_id VARCHAR(100),
    temporada VARCHAR(10),
    episodio VARCHAR(10),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ==========================================
-- RLS (Row Level Security)
-- ==========================================
ALTER TABLE channel_mappings ENABLE ROW LEVEL SECURITY;
ALTER TABLE channel_variants ENABLE ROW LEVEL SECURITY;
ALTER TABLE calendario ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow public read mappings" ON channel_mappings FOR SELECT USING (true);
CREATE POLICY "Allow public read variants" ON channel_variants FOR SELECT USING (true);
CREATE POLICY "Allow public read calendario" ON calendario FOR SELECT USING (true);

CREATE POLICY "Allow authenticated write mappings" ON channel_mappings FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow authenticated write variants" ON channel_variants FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow authenticated write calendario" ON calendario FOR ALL USING (true) WITH CHECK (true);

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

-- ==========================================
-- Función principal: Obtener evento con channels resueltos
-- ==========================================
CREATE OR REPLACE FUNCTION get_evento_con_channels(p_calendario_id UUID)
RETURNS TABLE (
    id UUID,
    fecha DATE,
    hora TEXT,
    competicion TEXT,
    categoria TEXT,
    equipos TEXT,
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
        c.categoria,
        c.equipos,
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
            ) FILTER (WHERE cv.channel_id IS NOT NULL),
            '[]'::jsonb
        ) as canales_resueltos
    FROM calendario c
    LEFT JOIN unnest(c.canales) AS canal_nombre ON true
    LEFT JOIN channel_mappings cm ON cm.source_name = canal_nombre
    LEFT JOIN channel_variants cv ON cv.mapping_id = cm.id
    WHERE c.id = p_calendario_id
    GROUP BY c.id, c.fecha, c.hora, c.competicion, c.categoria, c.equipos, c.canales;
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
    categoria TEXT,
    equipos TEXT,
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
        c.categoria,
        c.equipos,
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
            ) FILTER (WHERE cv.channel_id IS NOT NULL),
            '[]'::jsonb
        ) as canales_resueltos
    FROM calendario c
    LEFT JOIN unnest(c.canales) AS canal_nombre ON true
    LEFT JOIN channel_mappings cm ON cm.source_name = canal_nombre
    LEFT JOIN channel_variants cv ON cv.mapping_id = cm.id
    WHERE c.fecha = p_fecha
    GROUP BY c.id, c.fecha, c.hora, c.competicion, c.categoria, c.equipos, c.canales
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
    WHERE cm.source_name = ANY(nombres_canales)
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
