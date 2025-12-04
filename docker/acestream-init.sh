#!/bin/sh
set -e

apk add --no-cache curl jq

echo '⏳ Esperando 30 segundos adicionales para estabilización...'
sleep 30

echo '🔑 Obteniendo token de API...'
TOKEN=''
RETRIES=0
MAX_RETRIES=10

until [ -n "$TOKEN" ] && [ "$TOKEN" != "null" ]; do
  if [ $RETRIES -ge $MAX_RETRIES ]; then
    echo "❌ No se pudo obtener el token después de $MAX_RETRIES intentos"
    exit 1
  fi

  TOKEN=$(curl -s "http://acestream:6878/server/api?api_version=3&method=get_api_access_token" | jq -r '.result.token' 2>/dev/null) || true
  echo "Intento $((RETRIES+1)): TOKEN=$TOKEN"

  if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
    RETRIES=$((RETRIES+1))
    sleep 2
  fi
done

echo "✅ Token obtenido: $TOKEN"
echo "📧 Email: $EMAIL"

# Primera llamada: Sign In
echo ""
echo "🔐 Ejecutando login (1/2)..."
RESPONSE=$(curl -s "http://acestream:6878/server/api?api_version=3&method=sign_in&token=$TOKEN&password=$PASSWORD&email=$EMAIL")
echo "Respuesta completa:"
echo "$RESPONSE" | jq '.' 2>/dev/null || echo "$RESPONSE"

if echo "$RESPONSE" | jq -e '.result' > /dev/null 2>&1; then
  echo "✅ Login completado exitosamente"
else
  echo "⚠️  Advertencia en el login (continuando...)"
fi

# Pausa entre llamadas
sleep 2

# Segunda llamada: Verificar estado
echo ""
echo "🔍 Verificando estado de la cuenta (2/2)..."
STATUS_RESPONSE=$(curl -s "http://acestream:6878/server/api?api_version=3&method=get_user_info&token=$TOKEN")
echo "Respuesta de verificación:"
echo "$STATUS_RESPONSE" | jq '.' 2>/dev/null || echo "$STATUS_RESPONSE"

if echo "$STATUS_RESPONSE" | jq -e '.result' > /dev/null 2>&1; then
  echo "✅ Verificación completada"
  USER_STATUS=$(echo "$STATUS_RESPONSE" | jq -r '.result.is_premium // "N/A"')
  echo "Estado premium: $USER_STATUS"
else
  echo "⚠️  No se pudo verificar el estado"
fi

echo ""
echo "✨ Inicialización completada con éxito"
exit 0