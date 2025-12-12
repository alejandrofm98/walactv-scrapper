#!/bin/bash
set -e

echo "🚀 Iniciando Acestream Engine..."

# Iniciar Acestream en background usando el comando original de la imagen
/acestream/start-engine --client-console --bind-all &
ACESTREAM_PID=$!

echo "✅ Acestream iniciado con PID: $ACESTREAM_PID"
echo "⏳ Esperando a que Acestream esté listo..."

# Esperar a que el puerto 6878 esté disponible
COUNTER=0
MAX_WAIT=60
while ! nc -z localhost 6878; do
  sleep 2
  COUNTER=$((COUNTER + 2))
  if [ $COUNTER -ge $MAX_WAIT ]; then
    echo "❌ Timeout esperando a que Acestream arranque"
    exit 1
  fi
  echo "Esperando... ($COUNTER/$MAX_WAIT segundos)"
done

echo "✅ Acestream está listo en el puerto 6878"

echo "🚀 Iniciando supervisord (proxy + init script)..."
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf