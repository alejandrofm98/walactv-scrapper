#!/bin/sh
set -e

echo "🚀 Iniciando AceStream Engine (ARM64)..."

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

echo "📁 AceStream dir: $ACESTREAM_DIR"
cd "$ACESTREAM_DIR"

# CRÍTICO: Limpiar archivos .pyc corruptos antes de iniciar
echo "🧹 Limpiando archivos bytecode corruptos..."
find "$ACESTREAM_DIR/python" -type f -name "*.pyc" -delete 2>/dev/null || true
find "$ACESTREAM_DIR/python" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

# Detectar Python de AceStream - ESPECÍFICAMENTE buscar python3.12
if [ -x "$ACESTREAM_DIR/python/bin/python3.12" ]; then
    PYTHON="$ACESTREAM_DIR/python/bin/python3.12"
elif [ -x "$ACESTREAM_DIR/python/bin/python3" ]; then
    PYTHON="$ACESTREAM_DIR/python/bin/python3"
elif [ -x "$ACESTREAM_DIR/python/bin/python" ]; then
    PYTHON="$ACESTREAM_DIR/python/bin/python"
else
    echo "❌ Error: No se encontró ningún intérprete de Python en AceStream"
    echo "Contenido de $ACESTREAM_DIR/python/bin/:"
    ls -la "$ACESTREAM_DIR/python/bin/" 2>/dev/null || echo "Directorio no existe"
    exit 1
fi

echo "🐍 Python usado: $PYTHON"

# Verificar que el Python de Acestream funciona
if ! "$PYTHON" --version 2>/dev/null; then
    echo "❌ Error: El Python de AceStream no responde"
    exit 1
fi

# CRÍTICO: Limpiar variables de entorno para evitar conflictos
unset PYTHONHOME
unset PYTHONPATH

# PATH limpio - primero el Python de Acestream
export PATH="$ACESTREAM_DIR/python/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

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

# Dar tiempo a que Acestream arranque
sleep 5

# Verificar que el proceso sigue corriendo
if ! kill -0 $ACESTREAM_PID 2>/dev/null; then
    echo "❌ Error: AceStream se detuvo inmediatamente"
    exit 1
fi

echo "✅ AceStream corriendo correctamente"

# Ahora iniciar el proxy con supervisord
echo "🚀 Iniciando Proxy con Supervisor..."
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf