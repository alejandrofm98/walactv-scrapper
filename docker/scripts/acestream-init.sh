#!/bin/sh
# Compatible con sh (Alpine) y bash
set -e

ACESTREAM_HOST="${ACESTREAM_HOST:-acestream-engine}"
ACESTREAM_PORT="${ACESTREAM_PORT:-6878}"

echo "🔐 AceStream Login Script"
echo "========================================="
echo "Host: $ACESTREAM_HOST:$ACESTREAM_PORT"

# Esperar a que AceStream esté disponible
echo ""
echo "⏳ Esperando a que AceStream esté listo..."

MAX_WAIT=60
COUNTER=0

while ! nc -z "$ACESTREAM_HOST" "$ACESTREAM_PORT" 2>/dev/null; do
    sleep 2
    COUNTER=$((COUNTER + 2))

    if [ "$COUNTER" -ge "$MAX_WAIT" ]; then
        echo "❌ Timeout esperando a AceStream"
        exit 1
    fi

    echo "   Esperando... (${COUNTER}/${MAX_WAIT}s)"
done

echo "✅ AceStream disponible"

# Verificar credenciales
if [ -z "$ACESTREAM_EMAIL" ] || [ -z "$ACESTREAM_PASSWORD" ]; then
    echo "ℹ️  Sin credenciales, saltando login"
    exit 0
fi

echo ""
echo "📧 Email: $ACESTREAM_EMAIL"

# Esperar un poco más para que la API esté completamente lista
sleep 5

echo "🔑 Obteniendo token..."

RETRIES=0
TOKEN=""

while [ "$RETRIES" -lt 30 ]; do
    RESPONSE=$(curl -s "http://$ACESTREAM_HOST:$ACESTREAM_PORT/server/api?api_version=3&method=get_api_access_token" || echo "")
    TOKEN=$(echo "$RESPONSE" | jq -r '.result.token' 2>/dev/null || echo "")

    if [ -n "$TOKEN" ] && [ "$TOKEN" != "null" ]; then
        echo "✅ Token obtenido"
        break
    fi

    RETRIES=$((RETRIES + 1))
    echo "   Intento $RETRIES/30..."
    sleep 3
done

if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
    echo "❌ No se pudo obtener token"
    exit 1
fi

echo ""
echo "🔓 Realizando login..."

LOGIN_RESPONSE=$(curl -s "http://$ACESTREAM_HOST:$ACESTREAM_PORT/server/api?api_version=3&method=sign_in&token=$TOKEN&password=$ACESTREAM_PASSWORD&email=$ACESTREAM_EMAIL")

echo "$LOGIN_RESPONSE" | jq '.' 2>/dev/null || echo "$LOGIN_RESPONSE"

sleep 2

echo ""
echo "👤 Verificando usuario..."

USER_INFO=$(curl -s "http://$ACESTREAM_HOST:$ACESTREAM_PORT/server/api?api_version=3&method=get_user_info&token=$TOKEN")

echo "$USER_INFO" | jq '.' 2>/dev/null || echo "$USER_INFO"

echo ""
echo "✅ Login completado exitosamente"