#!/usr/bin/env python3
"""
Script para poblar las tablas de mapeo desde los archivos JSON:
- mapeo_canales_futbol_en_tv.json: mapeo de futbolenlatv a canales walactv
- canales.json: lista de canales con sus variaciones de calidad

Proceso:
1. Inserta en mapeo_futbolenlatv los nombres de canales de futbolenlatv (clave izquierda)
2. Inserta en canales_walactv las referencias de canales (valor derecha)
3. Busca en tabla channels por nombre y guarda en canales_calidades
4. Crea la relación en mapeo_futbolenlatv_canales

IDs numéricos autoincrementales (BIGSERIAL)
"""

import json
import sys
from pathlib import Path

# Añadir el directorio scripts al path
scripts_dir = Path(__file__).parent
sys.path.insert(0, str(scripts_dir))

from database import SupabaseDB, MapeoCanalesManager


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
    """Busca en la tabla channels por nombre exacto y retorna el ID (VARCHAR)"""
    try:
        result = supabase.table('channels').select('id').eq('nombre', nombre_iptv).execute()
        if result.data and len(result.data) > 0:
            return result.data[0]['id']
        return None
    except Exception as e:
        print(f"   ⚠️  Error buscando channel '{nombre_iptv}': {e}")
        return None


def main():
    print("🚀 Iniciando poblamiento de tablas de mapeo (IDs numéricos)...")
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
        'canales_walactv_insertados': 0,
        'canales_walactv_existentes': 0,
        'calidades_insertadas': 0,
        'calidades_omitidas': 0,
        'relaciones_insertadas': 0
    }
    
    print("📊 Procesando mapeos...")
    print("-" * 80)
    
    # Procesar cada entrada del mapeo
    for nombre_futboltv, nombre_canal_walactv in mapeo_futbolenlatv.items():
        print(f"\n📝 Procesando: {nombre_futboltv} -> {nombre_canal_walactv}")
        
        # 1. Insertar en mapeo_futbolenlatv (retorna ID numérico)
        try:
            mapeo_id = MapeoCanalesManager.upsert_mapeo_futboltv(nombre_futboltv)
            if mapeo_id:
                print(f"   ✅ Mapeo insertado (ID: {mapeo_id})")
                stats['mapeos_insertados'] += 1
            else:
                print(f"   ❌ Error insertando mapeo")
                continue
        except Exception as e:
            print(f"   ❌ Error insertando mapeo: {e}")
            continue
        
        # 2. Insertar en canales_walactv (retorna ID numérico)
        canal_walactv_id = None
        try:
            canal_walactv_id = MapeoCanalesManager.upsert_canal_walactv(nombre_canal_walactv)
            
            if canal_walactv_id:
                print(f"   ✅ Canal walactv creado (ID: {canal_walactv_id})")
                stats['canales_walactv_insertados'] += 1
            else:
                print(f"   ❌ Error creando canal walactv")
                continue
        except Exception as e:
            print(f"   ❌ Error con canal walactv: {e}")
            continue
        
        # 3. Buscar en canales.json las variaciones de este canal
        if nombre_canal_walactv in canales_data:
            variaciones = canales_data[nombre_canal_walactv]
            print(f"   📺 Encontradas {len(variaciones)} variaciones en canales.json")
            
            # Procesar cada variación
            for idx, var in enumerate(variaciones):
                if isinstance(var, dict) and 'nombre' in var:
                    nombre_iptv = var['nombre']
                    
                    # Buscar en tabla channels
                    channel_id = buscar_channel_id_por_nombre(supabase, nombre_iptv)
                    
                    if channel_id:
                        # 4. Insertar en canales_calidades con IDs numéricos
                        calidad = extraer_calidad(nombre_iptv)
                        
                        try:
                            success = MapeoCanalesManager.upsert_calidad(
                                canal_walactv_id=canal_walactv_id,
                                nombre_iptv=nombre_iptv,
                                channel_id=channel_id,
                                calidad=calidad,
                                orden=idx
                            )
                            
                            if success:
                                print(f"      ✅ Calidad: {nombre_iptv} -> channel_id: {channel_id}")
                                stats['calidades_insertadas'] += 1
                            else:
                                print(f"      ⚠️  No se pudo insertar calidad: {nombre_iptv}")
                                stats['calidades_omitidas'] += 1
                                
                        except Exception as e:
                            print(f"      ❌ Error insertando calidad '{nombre_iptv}': {e}")
                            stats['calidades_omitidas'] += 1
                    else:
                        print(f"      ⚠️  No se encontró channel para: {nombre_iptv}")
                        stats['calidades_omitidas'] += 1
        else:
            print(f"   ⚠️  No se encontró '{nombre_canal_walactv}' en canales.json")
        
        # 5. Crear relación en mapeo_futbolenlatv_canales (IDs numéricos)
        if mapeo_id and canal_walactv_id:
            try:
                success = MapeoCanalesManager.asociar_canal_a_mapeo(
                    mapeo_futbolenlatv_id=mapeo_id,
                    canal_walactv_id=canal_walactv_id,
                    orden=0
                )
                
                if success:
                    print(f"   ✅ Relación creada: {mapeo_id} <-> {canal_walactv_id}")
                    stats['relaciones_insertadas'] += 1
                else:
                    print(f"   ℹ️  Relación ya existente")
                    
            except Exception as e:
                print(f"   ❌ Error creando relación: {e}")
    
    # Resumen final
    print("\n" + "=" * 80)
    print("📊 RESUMEN DE INSERCIONES")
    print("=" * 80)
    print(f"✅ Mapeos insertados:           {stats['mapeos_insertados']}")
    print(f"✅ Canales walactv nuevos:      {stats['canales_walactv_insertados']}")
    print(f"ℹ️  Canales walactv existentes: {stats['canales_walactv_existentes']}")
    print(f"✅ Calidades insertadas:        {stats['calidades_insertadas']}")
    print(f"⚠️  Calidades omitidas:          {stats['calidades_omitidas']}")
    print(f"✅ Relaciones insertadas:       {stats['relaciones_insertadas']}")
    print("=" * 80)
    print("🎉 Proceso completado!")


if __name__ == "__main__":
    main()
