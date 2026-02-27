#!/bin/sh
# Entrypoint script para walactv-sync-iptv
# Ejecuta sync al iniciar Y luego mantiene el contenedor para Ofelia

echo "🚀 Iniciando contenedor IPTV Sync..."
echo "⏰ Hora de inicio: $(date)"
echo ""

# Ejecutar sincronización al iniciar
echo "⏳ Ejecutando sincronización inicial..."
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

echo ""
echo "📋 Configuración:"
echo "   - Ofelia ejecutará: cada 6 horas (0 */6 * * *)"
echo ""
echo "💡 Comandos manuales:"
echo "   - Sync IPTV: docker exec walactv-sync-iptv python scripts/sync_iptv.py"
echo "   - Poblar mapeo: docker exec walactv-sync-iptv python scripts/poblar_mapeo_canales.py"
echo ""

# Mantener el contenedor vivo para que Ofelia pueda ejecutar comandos
while true; do
    sleep 3600
done
