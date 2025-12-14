#!/bin/sh
set -e

echo "========================================="
echo "🚀 Iniciando AceStream + Proxy (ARM64)"
echo "========================================="

echo ""
echo "📡 Iniciando AceStream Engine..."

cd /acestream

CACHE_SIZE="${CACHE_SIZE:-1024}"
CACHE_BYTES=$((CACHE_SIZE * 1000000))

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

echo ""
echo "⏳ Esperando a que AceStream esté listo..."

MAX_WAIT=60
COUNTER=0

while ! nc -z localhost 6878 2>/dev/null; do
    sleep 2
    COUNTER=$((COUNTER + 2))

    if [ "$COUNTER" -ge "$MAX_WAIT" ]; then
        echo "❌ Timeout"
        tail -50 /proxy/logs/acestream.log
        exit 1
    fi

    if ! kill -0 $ACESTREAM_PID 2>/dev/null; then
        echo "❌ AceStream se detuvo"
        cat /proxy/logs/acestream.log
        exit 1
    fi

    echo "   Esperando... (${COUNTER}/${MAX_WAIT}s)"
done

echo "✅ AceStream escuchando en puerto 6878"

if [ -n "$ACESTREAM_EMAIL" ] && [ -n "$ACESTREAM_PASSWORD" ]; then
    echo ""
    echo "🔐 Realizando login..."
    /usr/local/bin/acestream-init-arm64.sh &
    echo "✅ Proceso de login iniciado en background"
fi

echo ""
echo "🔌 Iniciando Proxy en puerto 3000..."

cd /proxy

# CRÍTICO: Limpiar TODAS las variables de Python de Acestream
unset PYTHONHOME
unset PYTHONPATH
unset ANDROID_ROOT
unset ANDROID_DATA
unset LD_LIBRARY_PATH

# PATH con el Python del proxy primero
export PATH="/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin"

echo "✅ Variables limpiadas"
echo "   Usando Python del proxy en /usr/local"

# Iniciar gunicorn
exec /usr/local/bin/gunicorn \
    --workers 4 \
    --bind 0.0.0.0:3000 \
    --access-logfile - \
    --error-logfile - \
    --log-level info \
    acestream_proxy:app