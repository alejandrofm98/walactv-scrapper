# ============================================
# IPTV API - Dockerfile
# ============================================
FROM python:3.12-slim

# Variables de entorno
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DOCKER_CONTAINER=true

# Directorio de trabajo
WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements e instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
# Copiamos desde el contexto raíz del proyecto
COPY services/ /app/services/
COPY utils/ /app/utils/
COPY scripts/ /app/scripts/

# Crear directorios necesarios
RUN mkdir -p /app/data/m3u

# Puerto expuesto
EXPOSE 3001

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:3001/health || exit 1

# Comando por defecto
CMD ["uvicorn", "scripts.api:app", "--host", "0.0.0.0", "--port", "3001"]