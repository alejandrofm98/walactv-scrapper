-- ============================================
-- IPTV API - Script SQL para Supabase
-- ============================================
-- Ejecutar este script en el SQL Editor de Supabase

-- ============================================
-- 1. Tabla de usuarios IPTV
-- ============================================
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(50) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    max_connections INT DEFAULT 2 CHECK (max_connections >= 1 AND max_connections <= 10),
    is_active BOOLEAN DEFAULT true,
    role VARCHAR(20) DEFAULT 'user',
    expires_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Índices para usuarios
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_is_active ON users(is_active);
CREATE INDEX IF NOT EXISTS idx_users_expires_at ON users(expires_at);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role)

-- Trigger para actualizar updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_users_updated_at ON users;
CREATE TRIGGER update_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- 2. Tabla de sesiones/dispositivos activos
-- ============================================
CREATE TABLE IF NOT EXISTS active_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_id VARCHAR(64) NOT NULL,
    device_name VARCHAR(100),
    device_type VARCHAR(20) DEFAULT 'unknown',
    ip_address VARCHAR(45),
    user_agent TEXT,
    last_activity TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(user_id, device_id)
);

-- Índices para sesiones
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON active_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_device_id ON active_sessions(device_id);
CREATE INDEX IF NOT EXISTS idx_sessions_last_activity ON active_sessions(last_activity);

-- ============================================
-- 3. Tabla de canales (si no existe)
-- ============================================
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
-- ============================================
-- 4. Tabla de películas (si no existe)
-- ============================================
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


-- ============================================
-- 5. Tabla de series (si no existe)
-- ============================================
CREATE TABLE IF NOT EXISTS series (
    id VARCHAR(50) PRIMARY KEY,
    numero INT,
    provider_id VARCHAR(50),  -- ID del proveedor (ej: "1306345" de la URL)
    nombre TEXT NOT NULL,
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

-- ============================================
-- 6. Tabla de metadata de sincronización
-- ============================================
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

-- ============================================
-- 7. Función para truncar tablas (optimización)
-- ============================================
CREATE OR REPLACE FUNCTION truncate_table(table_name TEXT)
RETURNS VOID AS $$
BEGIN
    EXECUTE 'TRUNCATE TABLE ' || quote_ident(table_name) || ' RESTART IDENTITY CASCADE';
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- ============================================
-- 8. Función para limpiar sesiones inactivas
-- ============================================
CREATE OR REPLACE FUNCTION cleanup_inactive_sessions(timeout_minutes INT DEFAULT 30)
RETURNS INT AS $$
DECLARE
    deleted_count INT;
BEGIN
    DELETE FROM active_sessions
    WHERE last_activity < NOW() - (timeout_minutes || ' minutes')::INTERVAL;

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- 9. Vista de usuarios con dispositivos activos
-- ============================================
CREATE OR REPLACE VIEW users_with_devices AS
SELECT
    u.id,
    u.username,
    u.max_connections,
    u.is_active,
    u.expires_at,
    u.created_at,
    COUNT(s.id) as active_devices,
    ARRAY_AGG(
        jsonb_build_object(
            'device_id', s.device_id,
            'device_name', s.device_name,
            'device_type', s.device_type,
            'ip_address', s.ip_address,
            'last_activity', s.last_activity
        )
    ) FILTER (WHERE s.id IS NOT NULL) as devices
FROM users u
LEFT JOIN active_sessions s ON u.id = s.user_id
GROUP BY u.id, u.username, u.max_connections, u.is_active, u.expires_at, u.created_at;

-- ============================================
-- 10. Tabla de configuración
-- ============================================

CREATE TABLE config (
  key TEXT PRIMARY KEY,
  value TEXT,
  description TEXT,
  updated_at TIMESTAMPTZ DEFAULT now()
);