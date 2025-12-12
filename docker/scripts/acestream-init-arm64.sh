#!/bin/sh
set -e

echo "Iniciando Acestream Engine para ARM64..."

# Esperar un momento para que el sistema esté listo
sleep 2

# Configurar variables de entorno si no existen
export CACHE_SIZE=${CACHE_SIZE:-1024}
export DISK_CACHE_SIZE=${DISK_CACHE_SIZE:-1536}

# Buscar el directorio de Acestream y su Python
ACESTREAM_DIR=""
ACESTREAM_PYTHON=""

if [ -d "/acestream" ]; then
    ACESTREAM_DIR="/acestream"
    ACESTREAM_PYTHON="/acestream/python/bin/python"
elif [ -d "/opt/acestream" ]; then
    ACESTREAM_DIR="/opt/acestream"
    ACESTREAM_PYTHON="/opt/acestream/python/bin/python"
else
    echo "Error: No se encuentra el directorio de Acestream"
    exit 1
fi

# Verificar que existe el Python de Acestream
if [ ! -f "$ACESTREAM_PYTHON" ]; then
    # Intentar con python3
    ACESTREAM_PYTHON="${ACESTREAM_DIR}/python/bin/python3"
    if [ ! -f "$ACESTREAM_PYTHON" ]; then
        echo "Error: No se encuentra el Python de Acestream"
        exit 1
    fi
fi

cd "$ACESTREAM_DIR"

echo "Usando Python: $ACESTREAM_PYTHON"
echo "Directorio: $ACESTREAM_DIR"

# Iniciar Acestream con su propio Python
exec "$ACESTREAM_PYTHON" main.py \
    --bind-all \
    --client-console \
    --live-cache-type memory \
    --live-mem-cache-size ${CACHE_SIZE}000000 \
    --vod-cache-size ${DISK_CACHE_SIZE} \
    --disable-sentry \
    --log-stdout \
    --disable-upnp