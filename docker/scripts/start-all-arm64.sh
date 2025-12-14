#!/bin/sh
set -e

echo "🚀 Iniciando AceStream Engine (ARM64)..."

cd /acestream

# Variables de entorno
CACHE_SIZE="${CACHE_SIZE:-1024}"
CACHE_BYTES=$((CACHE_SIZE * 1000000))

# La imagen base YA tiene configuradas estas variables:
# PYTHONHOME=/acestream/python
# PYTHONPATH=/acestream/python/lib/stdlib:/acestream/python/lib/modules:...
# NO las modificamos

echo "📡 Iniciando AceStream en background..."

# Iniciar Acestream usando el comando original de la imagen
python main.py \
    --bind-all \
    --client-console \
    --live-cache-type memory \
    --live-mem-cache-size ${CACHE_BYTES} \
    --disable-sentry \
    --log-stdout \
    --disable-upnp > /proxy/logs/acestream.log 2>&1 &

ACESTREAM_PID=$!
echo "✅ AceStream iniciado (PID: $ACESTREAM_PID)"

# Esperar a que Acestream esté listo
sleep 5

if ! kill -0 $ACESTREAM_PID 2>/dev/null; then
    echo "❌ AceStream se detuvo"
    cat /proxy/logs/acestream.log
    exit 1
fi

echo "✅ AceStream corriendo"

# Ahora iniciar supervisord que gestiona el proxy y el login
echo "🚀 Iniciando Supervisord..."
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf