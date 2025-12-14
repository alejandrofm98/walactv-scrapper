#!/bin/sh
set -e

echo "🚀 Iniciando AceStream Engine (ARM64)..."
sleep 2

# Variables de entorno
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

echo "📁 AceStream dir: $ACESTREAM_DIR"
cd "$ACESTREAM_DIR"

# Limpiar archivos .pyc corruptos
echo "🧹 Limpiando archivos bytecode corruptos..."
find "$ACESTREAM_DIR/python" -type f -name "*.pyc" -delete 2>/dev/null || true
find "$ACESTREAM_DIR/python" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

# Detectar Python de AceStream
if [ -x "$ACESTREAM_DIR/python/bin/python3.12" ]; then
    PYTHON="$ACESTREAM_DIR/python/bin/python3.12"
elif [ -x "$ACESTREAM_DIR/python/bin/python3" ]; then
    PYTHON="$ACESTREAM_DIR/python/bin/python3"
elif [ -x "$ACESTREAM_DIR/python/bin/python" ]; then
    PYTHON="$ACESTREAM_DIR/python/bin/python"
else
    echo "❌ Error: No se encontró Python en AceStream"
    ls -la "$ACESTREAM_DIR/python/bin/" 2>/dev/null
    exit 1
fi

echo "🐍 Python usado: $PYTHON"

# CRÍTICO: Configurar PYTHONHOME para que el Python de Acestream encuentre sus librerías
export PYTHONHOME="$ACESTREAM_DIR/python"
export PYTHONPATH="$ACESTREAM_DIR/python/lib/stdlib:$ACESTREAM_DIR/python/lib/modules:$ACESTREAM_DIR/data:$ACESTREAM_DIR/modules.zip:$ACESTREAM_DIR/eggs-unpacked:$ACESTREAM_DIR/lib"
export PATH="$ACESTREAM_DIR/python/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

# Verificar que el Python funciona
echo "🔍 Verificando Python..."
if ! "$PYTHON" -c "import sys; print('Python OK')" 2>/dev/null; then
    echo "❌ Error: El Python de AceStream no funciona"
    "$PYTHON" -c "import sys; print(sys.path)" 2>&1 || true
    exit 1
fi

echo "✅ Python validado correctamente"

# Iniciar AceStream en background
echo "🎬 Iniciando AceStream Engine en background..."
"$PYTHON" main.py \
    --bind-all \
    --client-console \
    --live-cache-type memory \
    --live-mem-cache-size ${CACHE_SIZE}000000 \
    --vod-cache-size ${DISK_CACHE_SIZE} \
    --disable-sentry \
    --log-stdout \
    --disable-upnp &

ACESTREAM_PID=$!
echo "✅ AceStream iniciado con PID: $ACESTREAM_PID"

# Esperar a que Acestream esté listo
sleep 10

# Verificar que el proceso sigue corriendo
if ! kill -0 $ACESTREAM_PID 2>/dev/null; then
    echo "❌ Error: AceStream se detuvo inmediatamente"
    exit 1
fi

echo "✅ AceStream corriendo correctamente"

# IMPORTANTE: Limpiar las variables de Python antes de iniciar supervisor
# para que el proxy use su propio Python
unset PYTHONHOME
unset PYTHONPATH

# Ahora iniciar el proxy con supervisord
echo "🚀 Iniciando Proxy con Supervisor..."
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf