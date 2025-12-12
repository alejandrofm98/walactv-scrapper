#!/bin/bash

# Script para verificar que el Dockerfile ARM64 es correcto

DOCKERFILE="docker/Dockerfile-acestream-arm64"

echo "🔍 Verificando Dockerfile ARM64..."
echo ""

if [ ! -f "$DOCKERFILE" ]; then
    echo "❌ Error: No se encuentra $DOCKERFILE"
    exit 1
fi

# Verificar que NO instala python3
if grep -q "apk add.*python3" "$DOCKERFILE"; then
    echo "❌ ERROR: El Dockerfile está instalando python3 del sistema"
    echo "   Esto causará conflictos con el Python de Acestream"
    echo ""
    echo "   Líneas problemáticas:"
    grep -n "apk add.*python3" "$DOCKERFILE"
    echo ""
    echo "   ⚠️  SOLUCIÓN: Usa el Dockerfile actualizado que NO instala python3"
    exit 1
else
    echo "✅ Bien: No instala python3 del sistema"
fi

# Verificar que copia desde proxy-builder
if grep -q "COPY --from=proxy-builder /usr/local /opt/proxy-python" "$DOCKERFILE"; then
    echo "✅ Bien: Copia Python desde el builder"
else
    echo "⚠️  Advertencia: No encuentra la copia desde proxy-builder"
fi

# Verificar que tiene el comentario correcto
if grep -q "SIN Python adicional" "$DOCKERFILE"; then
    echo "✅ Bien: Usa la versión correcta del Dockerfile"
else
    echo "⚠️  Advertencia: Parece ser una versión antigua"
fi

echo ""
echo "📋 Resumen del Dockerfile:"
echo "   Stages: $(grep -c "^FROM" "$DOCKERFILE")"
echo "   Líneas totales: $(wc -l < "$DOCKERFILE")"
echo ""

# Mostrar las primeras líneas relevantes
echo "🔎 Primeras líneas del Stage 2:"
grep -A 10 "Stage 2:" "$DOCKERFILE" | head -15

echo ""
echo "✅ Verificación completada"