#!/bin/sh
set -e

echo "Iniciando AceStream Engine (ARM64)..."

# Esperar un momento para que el sistema esté listo
sleep 2

# Variables de entorno con valores por defecto
CACHE_SIZE="${CACHE_SIZE:-1024}"
DISK_CACHE_SIZE="${DISK_CACHE_SIZE:-1536}"

export CACHE_SIZE
export DISK_CACHE_SIZE

# Detectar directorio de AceStream
if [ -d "/acestream" ]; then
    ACESTREAM_DIR="/acestream"
elif [ -d "/opt/acestream" ]; then
    ACESTREAM_DIR="/opt/acestream"
else
    echo "❌ Error: No se encuentra el directorio de AceStream"
    exit 1
fi

echo "AceStream dir: $ACESTREAM_DIR"

cd "$ACESTREAM_DIR"

# Detectar Python de AceStream
if [ -x "$ACESTREAM_DIR/python/bin/python" ]; then
    PYTHON="$ACESTREAM_DIR/python/bin/python"
elif [ -x "$ACESTREAM_DIR/python/bin/python3" ]; then
    PYTHON="$ACESTREAM_DIR/python/bin/python3"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
    PYTHON="$(command -v python)"
else
    echo "❌ Error: No se encontró ningún intérprete de Python"
    exit 1
fi

echo "Python usado: $PYTHON"

# PATH limpio para evitar conflictos
export PATH="/acestream/python/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf