#!/bin/sh
set -e

apk add --no-cache curl jq

echo 'Esperando a que AceStream arranque...'
TOKEN=''
until [ -n "$TOKEN" ] && [ "$TOKEN" != "null" ]; do
  TOKEN=$(curl -s "http://acestream:6878/server/api?api_version=3&method=get_api_access_token" | jq -r '.result.token') || true
  echo "TOKEN=$TOKEN"
  [ -n "$TOKEN" ] && [ "$TOKEN" != "null" ] || sleep 2
done

echo "Token obtenido: $TOKEN"
echo "Email: $EMAIL"
echo "Ejecutando login..."

# Guardar la respuesta completa
RESPONSE=$(curl -s "http://acestream:6878/server/api?api_version=3&method=sign_in&token=$TOKEN&password=$PASSWORD&email=$EMAIL")
echo "Respuesta del login:"
echo "$RESPONSE"

# Verificar si fue exitoso
if echo "$RESPONSE" | jq -e '.result' > /dev/null 2>&1; then
  echo "✓ Login completado exitosamente"
else
  echo "✗ Error en el login"
fi

tail -f /dev/null