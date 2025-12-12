#!/bin/sh
set -e

echo "Iniciando Acestream Engine para ARM64..."

# Esperar un momento
sleep 2

# Variables de entorno
export CACHE_SIZE=${CACHE_SIZE:-1024}
export DISK_CACHE_SIZE=${DISK_CACHE_SIZE:-1536}

# Buscar Acestream
if [ -d "/acestream" ]; then
    ACESTREAM_DIR="/acestream"
elif [ -d "/opt/acestream" ]; then
    ACESTREAM_DIR="/opt/acestream"
else
    echo "Error: No se encuentra Acestream"
    exit 1
fi

cd "$ACESTREAM_DIR"

# Buscar el Python de Acestream
if [ -f "/acestream/python/bin/python" ]; then
    PYTHON="/acestream/python/bin/python"
elif [ -f "/acestream/python/bin/python3" ]; then
    PYTHON="/acestream/python/bin/python3"
elif [ -f "$ACESTREAM_DIR/python/bin/python" ]; then
    PYTHON="$ACESTREAM_DIR/python/bin/python"
else
    echo "Advertencia: No se encuentra Python de Acestream, usando python del PATH"
    PYTHON="python"
fi

echo "Acestream dir: $ACESTREAM_DIR"
echo "Python: $PYTHON"

# Limpiar el PATH para evitar conflictos, dejando solo lo básico
export PATH="/acestream/python/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

# Iniciar Acestream
exec "$PYTHON" main.py \
    --bind-all \
    --client-console \
    --live-cache-type memory \
    --live-mem-cache-size ${CACHE_SIZE}000000 \
    --vod-cache-size ${DISK_CACHE_SIZE} \
    --disable-sentry \
    --log-stdout \
    --disable-upnp