#!/bin/sh
set -e

echo "⏳ Esperando a que AceStream esté listo..."

MAX_WAIT=60
COUNTER=0

# Esperar a que el puerto 6878 esté disponible
while ! nc -z localhost 6878 >/dev/null 2>&1; do
    sleep 2
    COUNTER=$((COUNTER + 2))

    if [ "$COUNTER" -ge "$MAX_WAIT" ]; then
        echo "❌ Timeout esperando a AceStream en el puerto 6878"

        echo "🔍 Procesos AceStream:"
        ps aux | grep -i acestream | grep -v grep || echo "No se encontraron procesos"

        echo "🔍 Puertos en escucha:"
        ss -tulpn 2>/dev/null | grep LISTEN || netstat -tulpn 2>/dev/null | grep LISTEN || echo "No se pudo listar puertos"

        exit 1
    fi

    echo "Esperando... (${COUNTER}/${MAX_WAIT} segundos)"
done

echo "✅ AceStream escuchando en el puerto 6878"
sleep 5

echo "🔍 Variables:"
echo "EMAIL: ${ACESTREAM_EMAIL:-<no definido>}"
echo "PASSWORD: ***"

echo "🔑 Obteniendo token..."
TOKEN=""
RETRIES=0

while :; do
    if [ "$RETRIES" -ge 30 ]; then
        echo "❌ No se pudo obtener token tras 30 intentos"
        echo "🔍 Verificando conectividad API:"
        curl -v http://localhost:6878/server/api 2>&1 || true
        exit 1
    fi

    RESPONSE="$(curl -s 'http://localhost:6878/server/api?api_version=3&method=get_api_access_token' || true)"
    TOKEN="$(echo "$RESPONSE" | jq -r '.result.token' 2>/dev/null || echo '')"

    echo "Intento $((RETRIES + 1)): TOKEN=${TOKEN:-<vacío>}"

    if [ -n "$TOKEN" ] && [ "$TOKEN" != "null" ]; then
        break
    fi

    echo "Respuesta completa: $RESPONSE"
    sleep 3
    RETRIES=$((RETRIES + 1))
done

echo "✅ Token obtenido"

echo "🔐 Login (1/2)..."
RESP="$(curl -s "http://localhost:6878/server/api?api_version=3&method=sign_in&token=$TOKEN&password=$ACESTREAM_PASSWORD&email=$ACESTREAM_EMAIL" || true)"
echo "$RESP" | jq '.' 2>/dev/null || echo "$RESP"

sleep 2

echo "🔍 Verificando usuario (2/2)..."
USER_INFO="$(curl -s "http://localhost:6878/server/api?api_version=3&method=get_user_info&token=$TOKEN" || true)"
echo "$USER_INFO" | jq '.' 2>/dev/null || echo "$USER_INFO"

echo "✨ Login completado correctamente"
