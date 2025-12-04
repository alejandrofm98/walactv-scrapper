#!/bin/sh
set -e

echo ">>> Iniciando AceStream Engine..."
/acestream/acestreamengine --client-console &

ENGINE_PID=$!

echo ">>> Esperando 30 segundos para permitir que AceStream inicie completamente..."
sleep 30

echo ">>> Obteniendo token del API..."
TOKEN=$(curl -s "http://127.0.0.1:6878/server/api?api_version=3&method=get_api_access_token" | jq -r ".result.token")

if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
    echo "ERROR: No se pudo obtener el token"
    exit 1
fi

echo "Token obtenido: $TOKEN"

echo ">>> Ejecutando sign_in..."
LOGIN=$(curl -s "http://127.0.0.1:6878/server/api?api_version=3&method=sign_in&token=${TOKEN}&password=${PASSWORD}&email=${EMAIL}")

echo "Respuesta sign_in:"
echo "$LOGIN"

echo ">>> AceStream iniciado y autenticado. Manteniendo proceso vivo..."
wait $ENGINE_PID
