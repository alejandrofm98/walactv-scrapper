#!/bin/sh
# Entrypoint script para walactv-sync-iptv
# Ejecuta sync inmediatamente al iniciar (solo si no hay datos) y luego permite que Ofelia controle el schedule

echo "🚀 Iniciando servicio de sincronización IPTV..."

# Verificar si ya existen datos en Supabase
# Si no hay canales, ejecutar sync inmediatamente
python -c "
import sys
sys.path.insert(0, '/app/scripts')
from config import get_settings

settings = get_settings()
client = settings.get_supabase_client()

try:
    result = client.table('channels').select('*', count='exact').limit(1).execute()
    count = result.count if result.count else 0
    
    if count == 0:
        print('📺 No hay canales en la base de datos. Ejecutando sincronización inicial...')
        sys.exit(1)  # Indica que necesitamos ejecutar sync
    else:
        print(f'✅ Ya existen {count} canales en la base de datos. Saltando sincronización inicial.')
        sys.exit(0)  # No necesitamos ejecutar sync
except Exception as e:
    print(f'⚠️  Error verificando datos: {e}. Ejecutando sincronización inicial...')
    sys.exit(1)
"

# Si el comando anterior falla (exit 1), ejecutar sync
if [ $? -eq 1 ]; then
    echo "⏳ Ejecutando sincronización inicial..."
    python scripts/sync_iptv.py
    echo "✅ Sincronización IPTV completada."
    
    echo "⏳ Ejecutando poblamiento de mapeo de canales..."
    python scripts/poblar_mapeo_canales.py
    echo "✅ Poblamiento de mapeo completado."
fi

echo "😴 El servicio está en espera. Ofelia ejecutará el sync cada 2 horas."
echo "💡 Comandos manuales:"
echo "   - Sync IPTV: docker exec walactv-sync-iptv python scripts/sync_iptv.py"
echo "   - Poblar mapeo: docker exec walactv-sync-iptv python scripts/poblar_mapeo_canales.py"

# Mantener el contenedor vivo
while true; do
    sleep 3600
done
