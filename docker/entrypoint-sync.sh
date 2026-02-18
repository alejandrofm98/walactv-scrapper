#!/bin/sh
# Entrypoint script para walactv-sync-iptv
# Ejecuta sync inmediatamente al iniciar y luego permite que Ofelia controle el schedule

echo "🚀 Iniciando servicio de sincronización IPTV..."

# Función para ejecutar ambos scripts en secuencia
run_sync_sequence() {
    echo "⏳ Ejecutando sincronización IPTV..."
    python scripts/sync_iptv.py
    SYNC_STATUS=$?
    
    if [ $SYNC_STATUS -eq 0 ]; then
        echo "✅ Sincronización IPTV completada."
        
        echo "⏳ Ejecutando poblamiento de mapeo de canales..."
        python scripts/poblar_mapeo_canales.py
        echo "✅ Poblamiento de mapeo completado."
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
