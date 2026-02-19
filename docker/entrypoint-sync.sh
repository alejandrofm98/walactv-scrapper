#!/bin/sh
# Entrypoint script para walactv-sync-iptv
# Ejecuta sync inmediatamente al iniciar y luego permite que Ofelia controle el schedule

echo "🚀 Iniciando servicio de sincronización IPTV..."

# Función para recargar el template en la API
reload_api_template() {
    API_URL="${IPTV_API_URL:-http://iptv-api:3010}"
    TOKEN="${JWT_SECRET:-}"
    
    echo "🔄 Recargando template en la API..."
    
    if [ -z "$TOKEN" ]; then
        echo "⚠️  JWT_SECRET no configurado. Saltando recarga."
        echo "   El template estará disponible pero la API puede tardar en detectar cambios."
        return
    fi
    
    # Llamar al endpoint de recarga
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST "${API_URL}/api/admin/content/reload" \
        -H "Authorization: Bearer ${TOKEN}" \
        -H "Content-Type: application/json" \
        --max-time 30)
    
    if [ "$HTTP_CODE" = "200" ]; then
        echo "✅ Template recargado correctamente en la API"
    else
        echo "⚠️  No se pudo recargar el template (HTTP $HTTP_CODE)"
        echo "   El archivo fue actualizado pero la API puede estar usando caché"
    fi
}

# Función para ejecutar ambos scripts en secuencia
run_sync_sequence() {
    echo "⏳ Ejecutando sincronización IPTV..."
    python scripts/sync_iptv.py
    SYNC_STATUS=$?
    
    if [ $SYNC_STATUS -eq 0 ]; then
        echo "✅ Sincronización IPTV completada."
        
        echo "⏳ Ejecutando poblamiento de mapeo de canales..."
        python scripts/poblar_mapeo_canales.py
        MAP_STATUS=$?
        
        if [ $MAP_STATUS -eq 0 ]; then
            echo "✅ Poblamiento de mapeo completado."
            # Recargar template en la API
            reload_api_template
        else
            echo "⚠️  El poblamiento de mapeo falló."
        fi
    else
        echo "❌ La sincronización IPTV falló. No se ejecutará poblar_mapeo_canales."
    fi
}

# Ejecutar secuencia al iniciar el contenedor
run_sync_sequence

echo "😴 El servicio está en espera. Ofelia ejecutará el sync cada 2 horas."
echo "💡 Comandos manuales:"
echo "   - Sync IPTV: docker exec walactv-sync-iptv python scripts/sync_iptv.py"
echo "   - Poblar mapeo: docker exec walactv-sync-iptv python scripts/poblar_mapeo_canales.py"

# Mantener el contenedor vivo para que Ofelia pueda ejecutar comandos
while true; do
    sleep 3600
done
