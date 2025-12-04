#!/bin/sh
set -e

# instalar herramientas
apk add --no-cache curl jq

echo "Esperando 30 segundos para que AceStream arranque..."
sleep 30

# obtener token dinámicamente
TOKEN=""
while [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; do
  TOKEN=$(curl -s "http://acestream:6878/server/api?api_version=3&method=get_api_access_token" | jq -r ".result.token")
  echo "TOKEN='$TOKEN'"
  [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ] && sleep 2
done

# debug variables
echo "DEBUG: EMAIL='$EMAIL', PASSWORD='$PASSWORD', TOKEN='$TOKEN'"

# login
curl -s "http://acestream:6878/server/api?api_version=3&method=sign_in&token=$TOKEN&password=$PASSWORD&email=$EMAIL"

echo "Petición de login completada"
