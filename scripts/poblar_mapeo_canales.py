#!/usr/bin/env python3
"""
Script para poblar las tablas de mapeo desde los archivos JSON:
- mapeo_canales_futbol_en_tv.json: mapeo de futbolenlatv a canales walactv
- canales.json: lista de canales con sus variaciones de calidad

Nuevo esquema simplificado:
- channel_mappings: source_name (futbolenlatv) + display_name (web)
- channel_variants: channel_id de la tabla channels + quality + priority
"""

import json
import sys
from pathlib import Path

# Añadir el directorio scripts al path
scripts_dir = Path(__file__).parent
sys.path.insert(0, str(scripts_dir))

from database import SupabaseDB, ChannelMappingManager


def load_json_files():
    """Carga los archivos JSON necesarios"""
    resources_dir = Path(__file__).parent.parent / 'resources'
    
    mapeo_path = resources_dir / 'mapeo_canales_futbol_en_tv.json'
    canales_path = resources_dir / 'canales.json'
    
    if not mapeo_path.exists():
        print(f"❌ No se encontró {mapeo_path}")
        return None, None
    
    if not canales_path.exists():
        print(f"❌ No se encontró {canales_path}")
        return None, None
    
    with open(mapeo_path, 'r', encoding='utf-8') as f:
        mapeo_futbolenlatv = json.load(f)
    
    with open(canales_path, 'r', encoding='utf-8') as f:
        canales_data = json.load(f)
    
    return mapeo_futbolenlatv, canales_data


def extraer_calidad(nombre_iptv: str) -> str:
    """Extrae la calidad del nombre IPTV"""
    calidades = ['FHD', '4K', 'UHD', 'HD', 'SD', 'RAW', 'LOW', 'HEVC']
    nombre_upper = nombre_iptv.upper()
    for calidad in calidades:
        if calidad in nombre_upper:
            return calidad
    return 'HD'


def buscar_channel_id_por_nombre(supabase, nombre_iptv: str) -> str:
    """Busca en la tabla channels por nombre exacto y retorna el ID"""
    try:
        result = supabase.table('channels').select('id').eq('nombre', nombre_iptv).execute()
        if result.data and len(result.data) > 0:
            return result.data[0]['id']
        return None
    except Exception as e:
        print(f"   ⚠️  Error buscando channel '{nombre_iptv}': {e}")
        return None


def main():
    print("🚀 Iniciando poblamiento de tablas de mapeo (Esquema Simplificado)...")
    print()
    
    # Cargar archivos JSON
    mapeo_futbolenlatv, canales_data = load_json_files()
    if not mapeo_futbolenlatv or not canales_data:
        print("❌ Error cargando archivos JSON")
        return
    
    print(f"📄 Cargado mapeo_futbolenlatv: {len(mapeo_futbolenlatv)} entradas")
    print(f"📄 Cargado canales.json: {len(canales_data)} canales")
    print()
    
    # Inicializar conexión a Supabase
    try:
        supabase = SupabaseDB.initialize()
    except Exception as e:
        print(f"❌ Error conectando a Supabase: {e}")
        return
    
    # Estadísticas
    stats = {
        'mapeos_insertados': 0,
        'mapeos_actualizados': 0,
        'variantes_insertadas': 0,
        'variantes_omitidas': 0,
        'errores': 0
    }
    
    print("📊 Procesando mapeos...")
    print("-" * 80)
    
    # Procesar cada entrada del mapeo
    for source_name, display_name in mapeo_futbolenlatv.items():
        print(f"\n📝 Procesando: {source_name} -> {display_name}")
        
        # Buscar las variaciones en canales.json
        variantes = canales_data.get(display_name, [])
        
        if not variantes:
            print(f"   ⚠️  No se encontraron variantes en canales.json")
            stats['errores'] += 1
            continue
        
        print(f"   📺 Encontradas {len(variantes)} variaciones")
        
        # Preparar arrays de channel_ids y qualities
        channel_ids = []
        qualities = []
        
        for idx, var in enumerate(variantes):
            if isinstance(var, dict) and 'nombre' in var:
                nombre_iptv = var['nombre']
                
                # Buscar en tabla channels
                channel_id = buscar_channel_id_por_nombre(supabase, nombre_iptv)
                
                if channel_id:
                    channel_ids.append(channel_id)
                    qualities.append(extraer_calidad(nombre_iptv))
                    print(f"      ✅ {nombre_iptv} -> {channel_id}")
                    stats['variantes_insertadas'] += 1
                else:
                    print(f"      ⚠️  No se encontró channel para: {nombre_iptv}")
                    stats['variantes_omitidas'] += 1
        
        if channel_ids:
            # Insertar en el nuevo esquema simplificado
            try:
                mapping_id = ChannelMappingManager.upsert_mapping(
                    source_name=source_name,
                    display_name=display_name,
                    channel_ids=channel_ids,
                    qualities=qualities
                )
                
                if mapping_id:
                    print(f"   ✅ Mapeo guardado (ID: {mapping_id}) con {len(channel_ids)} variantes")
                    stats['mapeos_insertados'] += 1
                else:
                    print(f"   ❌ Error guardando mapeo")
                    stats['errores'] += 1
                    
            except Exception as e:
                print(f"   ❌ Error: {e}")
                stats['errores'] += 1
        else:
            print(f"   ⚠️  No se encontraron channel_ids, creando mapeo vacío")
            # Crear mapeo sin variantes (se pueden agregar después)
            try:
                mapping_id = ChannelMappingManager.upsert_mapping(
                    source_name=source_name,
                    display_name=display_name
                )
                if mapping_id:
                    stats['mapeos_insertados'] += 1
            except Exception as e:
                print(f"   ❌ Error: {e}")
                stats['errores'] += 1
    
    # Resumen final
    print("\n" + "=" * 80)
    print("📊 RESUMEN DE INSERCIONES")
    print("=" * 80)
    print(f"✅ Mapeos insertados/actualizados: {stats['mapeos_insertados']}")
    print(f"✅ Variantes insertadas:            {stats['variantes_insertadas']}")
    print(f"⚠️  Variantes omitidas:             {stats['variantes_omitidas']}")
    print(f"❌ Errores:                        {stats['errores']}")
    print("=" * 80)
    print("🎉 Proceso completado!")
    print()
    print("💡 Nota: Si hay muchas variantes omitidas, verifica que la tabla 'channels'")
    print("   esté poblada primero desde el M3U.")


if __name__ == "__main__":
    main()
