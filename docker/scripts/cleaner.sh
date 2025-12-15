#!/bin/sh

# Script de limpieza automática para AceStream
# Limpia archivos antiguos y mantiene el tamaño de caché bajo control

set -e

# Verificar que el directorio existe
if [ ! -d "/cache" ]; then
    echo "Error: /cache no existe. Esperando..."
    sleep 10
fi

CACHE_DIR="/cache"
INTERVAL_HOURS=${CLEANUP_INTERVAL:-6}
MAX_AGE_DAYS=${MAX_AGE_DAYS:-2}
MAX_CACHE_SIZE_MB=${MAX_CACHE_SIZE_MB:-5120}

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

cleanup_old_files() {
    log "🗑️  Limpiando archivos más antiguos de ${MAX_AGE_DAYS} días..."

    if [ -d "$CACHE_DIR" ]; then
        DELETED=$(find "$CACHE_DIR" -type f -mtime +${MAX_AGE_DAYS} 2>/dev/null | wc -l)

        if [ "$DELETED" -gt 0 ]; then
            find "$CACHE_DIR" -type f -mtime +${MAX_AGE_DAYS} -delete 2>/dev/null || true
            log "✅ Eliminados $DELETED archivos antiguos"
        else
            log "ℹ️  No hay archivos antiguos para eliminar"
        fi
    else
        log "⚠️  Directorio de caché no encontrado: $CACHE_DIR"
    fi
}

cleanup_by_size() {
    log "📊 Verificando tamaño de caché (límite: ${MAX_CACHE_SIZE_MB}MB)..."

    if [ -d "$CACHE_DIR" ]; then
        # Obtener tamaño actual en MB
        CURRENT_SIZE=$(du -sm "$CACHE_DIR" 2>/dev/null | cut -f1)

        log "📦 Tamaño actual: ${CURRENT_SIZE}MB"

        if [ "$CURRENT_SIZE" -gt "$MAX_CACHE_SIZE_MB" ]; then
            log "⚠️  Caché excede el límite. Limpiando archivos más antiguos..."

            # Eliminar archivos más antiguos hasta estar por debajo del límite
            find "$CACHE_DIR" -type f -printf '%T+ %p\n' 2>/dev/null | \
                sort | \
                head -n 50 | \
                cut -d' ' -f2- | \
                xargs rm -f 2>/dev/null || true

            NEW_SIZE=$(du -sm "$CACHE_DIR" 2>/dev/null | cut -f1)
            FREED=$((CURRENT_SIZE - NEW_SIZE))
            log "✅ Liberados ${FREED}MB de espacio"
        else
            log "✅ Tamaño de caché dentro del límite"
        fi
    fi
}

cleanup_temp_files() {
    log "🧹 Limpiando archivos temporales..."

    # Limpiar archivos .part (descargas incompletas)
    find "$CACHE_DIR" -type f -name "*.part" -mtime +1 -delete 2>/dev/null || true

    # Limpiar archivos .lock antiguos
    find "$CACHE_DIR" -type f -name "*.lock" -mtime +1 -delete 2>/dev/null || true

    # Limpiar directorios vacíos
    find "$CACHE_DIR" -type d -empty -delete 2>/dev/null || true

    log "✅ Archivos temporales limpiados"
}

show_stats() {
    if [ -d "$CACHE_DIR" ]; then
        SIZE=$(du -sh "$CACHE_DIR" 2>/dev/null | cut -f1)
        FILES=$(find "$CACHE_DIR" -type f 2>/dev/null | wc -l)
        log "📈 Estadísticas: $SIZE de espacio usado, $FILES archivos"
    fi
}

# Bucle principal
log "🚀 Iniciando AceStream Cleaner"
log "⚙️  Configuración:"
log "   - Intervalo de limpieza: cada ${INTERVAL_HOURS} horas"
log "   - Edad máxima de archivos: ${MAX_AGE_DAYS} días"
log "   - Tamaño máximo de caché: ${MAX_CACHE_SIZE_MB}MB"

while true; do
    log "======================================"
    log "🔄 Iniciando ciclo de limpieza"

    cleanup_old_files
    cleanup_by_size
    cleanup_temp_files
    show_stats

    log "✅ Ciclo completado. Próxima limpieza en ${INTERVAL_HOURS} horas"
    log "======================================"

    sleep $((INTERVAL_HOURS * 3600))
done