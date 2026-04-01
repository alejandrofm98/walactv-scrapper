-- ==========================================
-- Migration: Añadir columnas para tracking de JSON cache
-- Fecha: 2024-01
-- ==========================================

-- Añadir columnas a sync_metadata si no existen
ALTER TABLE sync_metadata 
ADD COLUMN IF NOT EXISTS channels_generated_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE sync_metadata 
ADD COLUMN IF NOT EXISTS channels_json_size_mb NUMERIC(10,2);

ALTER TABLE sync_metadata 
ADD COLUMN IF NOT EXISTS movies_generated_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE sync_metadata 
ADD COLUMN IF NOT EXISTS movies_json_size_mb NUMERIC(10,2);

ALTER TABLE sync_metadata 
ADD COLUMN IF NOT EXISTS series_generated_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE sync_metadata 
ADD COLUMN IF NOT EXISTS series_json_size_mb NUMERIC(10,2);

-- Añadir indice para lookup rapido de generated_at
CREATE INDEX IF NOT EXISTS idx_sync_metadata_channels_generated 
ON sync_metadata(channels_generated_at);

CREATE INDEX IF NOT EXISTS idx_sync_metadata_movies_generated 
ON sync_metadata(movies_generated_at);

CREATE INDEX IF NOT EXISTS idx_sync_metadata_series_generated 
ON sync_metadata(series_generated_at);

-- Verificar que las columnas se añadieron
DO $$
BEGIN
    RAISE NOTICE 'Migration completed: Added JSON tracking columns to sync_metadata';
END $$;
