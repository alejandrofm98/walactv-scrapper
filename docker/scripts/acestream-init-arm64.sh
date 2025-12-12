#!/bin/bash
set -e

echo "Iniciando Acestream Engine para ARM64..."

# Esperar un momento para que el sistema esté listo
sleep 2

# Configurar variables de entorno si no existen
export CACHE_SIZE=${CACHE_SIZE:-1024}
export DISK_CACHE_SIZE=${DISK_CACHE_SIZE:-1536}

# Iniciar Acestream con los parámetros específicos para ARM64
cd /opt/acestream || cd /acestream

exec python main.py \
    --bind-all \
    --client-console \
    --live-cache-type memory \
    --live-mem-cache-size ${CACHE_SIZE}000000 \
    --vod-cache-size ${DISK_CACHE_SIZE} \
    --disable-sentry \
    --log-stdout \
    --disable-upnp