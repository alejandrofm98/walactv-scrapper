#!/bin/bash

echo '⏳ Esperando a que Acestream esté listo...'

# Esperar a que el puerto 6878 esté disponible
MAX_WAIT=60
COUNTER=0
while ! nc -z localhost 6878; do
  sleep 2
  COUNTER=$((COUNTER + 2))
  if [ $COUNTER -ge $MAX_WAIT ]; then
    echo "❌ Timeout esperando a que Acestream arranque en el puerto 6878"

    # Debug: mostrar procesos y puertos
    echo "🔍 Procesos de Acestream:"
    ps aux | grep acestream || echo "No se encontraron procesos"

    echo "🔍 Puertos abiertos:"
    netstat -tulpn 2>/dev/null | grep LISTEN || ss -tulpn | grep LISTEN || echo "No se pudo listar puertos"

    exit 1
  fi
  echo "Esperando... ($COUNTER/$MAX_WAIT segundos)"
done

echo '✅ Acestream está escuchando en el puerto 6878'
sleep 5  # Dar un poco más de tiempo para que el API esté completamente lista

echo '🔍 Variables:'
echo "EMAIL: $ACESTREAM_EMAIL"
echo "PASSWORD: ***"

echo '🔑 Obteniendo token...'
TOKEN=''
RETRIES=0

until [ -n "$TOKEN" ] && [ "$TOKEN" != 'null' ] && [ "$TOKEN" != '' ]; do
  if [ $RETRIES -ge 30 ]; then
    echo '❌ No se pudo obtener token después de 30 intentos'
    echo '🔍 Verificando conectividad:'
    curl -v http://localhost:6878/server/api 2>&1 || echo "No se pudo conectar"
    exit 1
  fi

  RESPONSE=$(curl -s 'http://localhost:6878/server/api?api_version=3&method=get_api_access_token' 2>&1)
  TOKEN=$(echo "$RESPONSE" | jq -r '.result.token' 2>/dev/null) || TOKEN=''

  echo "Intento $((RETRIES+1)): TOKEN=$TOKEN"

  if [ -z "$TOKEN" ] || [ "$TOKEN" = 'null' ]; then
    echo "Respuesta completa: $RESPONSE"
    sleep 3
  fi

  RETRIES=$((RETRIES+1))
done

echo "✅ Token obtenido: $TOKEN"

echo '🔐 Login (1/2)...'
RESP=$(curl -s "http://localhost:6878/server/api?api_version=3&method=sign_in&token=$TOKEN&password=$ACESTREAM_PASSWORD&email=$ACESTREAM_EMAIL")
echo "Respuesta login:"
echo "$RESP" | jq '.' 2>/dev/null || echo "$RESP"

sleep 2

echo '🔍 Verificando (2/2)...'
USER_INFO=$(curl -s "http://localhost:6878/server/api?api_version=3&method=get_user_info&token=$TOKEN")
echo "$USER_INFO" | jq '.' 2>/dev/null || echo "$USER_INFO"

echo '✨ Login completado'