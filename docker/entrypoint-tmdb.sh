#!/bin/sh
# Entrypoint para el servicio de metadata TMDB
# Ejecuta scrape al iniciar y mantiene contenedor vivo para Ofela

echo "🎬 Iniciando servicio TMDB Metadata..."
echo "⏰ Hora: $(date)"
echo ""

# Verificar variables de entorno requeridas
if [ -z "$PG_HOST" ]; then
    echo "❌ Error: PG_HOST no definido"
    exit 1
fi

if [ -z "$PG_USER" ]; then
    echo "❌ Error: PG_USER no definido"
    exit 1
fi

if [ -z "$PG_PASSWORD" ]; then
    echo "❌ Error: PG_PASSWORD no definido"
    exit 1
fi

echo "✅ Configuración:"
echo "   - PG_HOST: $PG_HOST"
echo "   - PG_DATABASE: ${PG_DATABASE:-postgres}"
echo "   - PG_USER: $PG_USER"
echo ""

# Ejecutar scrape inicial al iniciar
echo "⏳ Ejecutando scrape inicial..."
python iptv_scrapper/scrape_tmdb_metadata.py --batch-size 50
SCRAPE_STATUS=$?

if [ $SCRAPE_STATUS -eq 0 ]; then
    echo "✅ Scrape inicial completado"
else
    echo "⚠️  Scrape inicial falló (código: $SCRAPE_STATUS), pero continuamos..."
fi

echo ""
echo "📋 Programación:"
echo "   - Ofelia ejecutará scrape 4 veces al día (3:30, 9:30, 15:30, 21:30)"
echo "   - Contenedor se mantiene activo para recibir comandos"
echo ""
echo "💡 Comandos manuales:"
echo "   - Scrape completo: docker exec walactv-sync-tmdb-metadata python iptv_scrapper/scrape_tmdb_metadata.py"
echo "   - Scrape dry-run: docker exec walactv-sync-tmdb-metadata python iptv_scrapper/scrape_tmdb_metadata.py --dry-run"
echo "   - Scrape limitado: docker exec walactv-sync-tmdb-metadata python iptv_scrapper/scrape_tmdb_metadata.py --max-items 10"
echo ""

# Mantener contenedor vivo para Ofelia
while true; do
    sleep 3600
done
