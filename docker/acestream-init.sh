#!/bin/sh
set -e

apk add --no-cache curl jq

echo 'Esperando a que AceStream arranque...'
TOKEN=''
until [ -n "$TOKEN" ] && [ "$TOKEN" != "null" ]; do
  TOKEN=$(curl -s "http://acestream:6878/server/api?api_version=3&method=get_api_access_token" | jq -r '.result.token') || true
  echo "DEBUG VARIABLES:"
  echo "EMAIL=$EMAIL"
  echo "PASSWORD=$PASSWORD"
  echo "TOKEN=$TOKEN"
  [ -n "$TOKEN" ] && [ "$TOKEN" != "null" ] || sleep 2
done

echo "Token obtenido, ejecutando login..."
curl -s "http://acestream:6878/server/api?api_version=3&method=sign_in&token=$TOKEN&password=$PASSWORD&email=$EMAIL"
echo "Login completado"

# Mantener el contenedor vivo si quieres ver logs
tail -f /dev/null
