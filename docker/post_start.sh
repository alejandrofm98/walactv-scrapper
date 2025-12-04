#!/bin/sh
set -e

echo ">>> Esperando 30 segundos a que AceStream termine de arrancar..."
sleep 30

echo ">>> Obteniendo token del API..."
TOKEN=$(curl -s "http://127.0.0.1:6878/server/api?api_version=3&method=get_api_access_token" | jq -r ".result.token")

if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
    echo "ERROR: No se pudo obtener el token"
    exit 1
fi

echo "Token obtenido: $TOKEN"

echo ">>> Ejecutando sign_in..."
curl -s "http://127.0.0.1:6878/server/api?api_version=3&method=sign_in&token=${TOKEN}&password=${PASSWORD}&email=${EMAIL}"

echo ">>> Peticiones realizadas correctamente."
