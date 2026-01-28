FROM nginx:alpine

# Copiar configuración de Nginx
COPY nginx.conf /etc/nginx/nginx.conf

# Crear directorios necesarios
RUN mkdir -p /app/data/m3u /app/data/logos

# Exponer puerto
EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]