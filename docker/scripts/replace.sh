#!/bin/sh
# Reemplazar marcadores en el archivo de configuración con las variables de entorno

sed -i "s/__IPTV_USER__/${IPTV_USER}/g" /etc/nginx/nginx.conf
sed -i "s/__IPTV_PASS__/${IPTV_PASS}/g" /etc/nginx/nginx.conf

echo "--- VARIABLES REEMPLAZADAS CORRECTAMENTE ---"
# Iniciar Nginx
exec nginx -g 'daemon off;'