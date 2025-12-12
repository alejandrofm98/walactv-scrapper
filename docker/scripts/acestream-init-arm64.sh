#!/bin/sh
set -e

echo "Iniciando Acestream Engine para ARM64..."

# Esperar un momento para que el sistema esté listo
sleep 2

# Configurar variables de entorno si no existen
export CACHE_SIZE=${CACHE_SIZE:-1024}
export DISK_CACHE_SIZE=${DISK_CACHE_SIZE:-1536}

# Buscar el directorio de Acestream
if [ -d "/opt/acestream" ]; then
    cd /opt/acestream
elif [ -d "/acestream" ]; then
    cd /acestream
else
    echo "Error: No se encuentra el directorio de Acestream"
    exit 1
fi

# Iniciar Acestream con los parámetros específicos para ARM64
exec python3 main.py \
    --bind-all \
    --client-console \
    --live-cache-type memory \
    --live-mem-cache-size ${CACHE_SIZE}000000 \
    --vod-cache-size ${DISK_CACHE_SIZE} \
    --disable-sentry \
    --log-stdout \
    --disable-upnp