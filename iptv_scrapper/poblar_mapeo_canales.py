#!/usr/bin/env python3
"""
Script para poblar las tablas de mapeo desde los archivos JSON:
- mapeo_canales_futbol_en_tv.json: mapeo de futbolenlatv a canales walactv
- canales.json: lista de canales con sus variaciones de calidad

Esquema simplificado:
- channel_mappings: source_name (futbolenlatv) + display_name (web)
- channel_variants: channel_id de la tabla channels + quality + priority
"""

import asyncio
import json
import re
import sys
from pathlib import Path

from sqlalchemy import text

scripts_dir = Path(__file__).parent
sys.path.insert(0, str(scripts_dir))

from database import ChannelMappingManager, DatabasePG


def load_json_files():
    """Carga los archivos JSON necesarios"""
    resources_dir = Path(__file__).parent.parent / "resources"

    mapeo_path = resources_dir / "mapeo_canales_futbol_en_tv.json"
    canales_path = resources_dir / "canales.json"

    if not mapeo_path.exists():
        print(f"❌ No se encontró {mapeo_path}")
        return None, None

    if not canales_path.exists():
        print(f"❌ No se encontró {canales_path}")
        return None, None

    with open(mapeo_path, encoding="utf-8") as f:
        mapeo_futbolenlatv = json.load(f)

    with open(canales_path, encoding="utf-8") as f:
        canales_data = json.load(f)

    return mapeo_futbolenlatv, canales_data


def extraer_calidad(nombre_iptv: str) -> str:
    """Extrae la calidad del nombre IPTV"""
    calidades = ["FHD", "4K", "UHD", "HD", "SD", "RAW", "LOW", "HEVC"]
    nombre_upper = nombre_iptv.upper()
    for calidad in calidades:
        if calidad in nombre_upper:
            return calidad
    return "HD"


async def buscar_channel_id_por_nombre(nombre_iptv: str) -> str | None:
    """Busca en la tabla channels por nombre exacto y retorna el ID. F3d2: iptv-db."""
    try:
        session_factory = DatabasePG.get_session_factory()
        async with session_factory() as session:
            result = await session.execute(
                text("SELECT id FROM channels WHERE nombre = :nombre"),
                {"nombre": nombre_iptv},
            )
            row = result.mappings().first()
            if row:
                return str(row["id"])
            return None
    except Exception as e:
        print(f"   ⚠️  Error buscando channel '{nombre_iptv}': {e}")
        return None


async def buscar_channels_por_patron(variante: dict) -> list:
    """Busca channels por patrón dinámico usando nombre/grupo. F3d2: iptv-db."""
    try:
        session_factory = DatabasePG.get_session_factory()
        async with session_factory() as session:
            grupo_contains = variante.get("grupo_contains")
            nombre_contains = variante.get("nombre_contains")

            if grupo_contains and nombre_contains:
                rows = await session.execute(
                    text(
                        "SELECT id, nombre, grupo FROM channels WHERE grupo ILIKE :grupo AND nombre ILIKE :nombre"
                    ),
                    {"grupo": f"%{grupo_contains}%", "nombre": f"%{nombre_contains}%"},
                )
            elif grupo_contains:
                rows = await session.execute(
                    text("SELECT id, nombre, grupo FROM channels WHERE grupo ILIKE :grupo"),
                    {"grupo": f"%{grupo_contains}%"},
                )
            elif nombre_contains:
                rows = await session.execute(
                    text("SELECT id, nombre, grupo FROM channels WHERE nombre ILIKE :nombre"),
                    {"nombre": f"%{nombre_contains}%"},
                )
            else:
                return []

            result_rows = rows.mappings().all()
            if not result_rows:
                return []

            nombre_regex = variante.get("nombre_regex")
            grupo_regex = variante.get("grupo_regex")

            channels = []
            for row in result_rows:
                nombre = row.get("nombre", "")
                grupo = row.get("grupo", "")

                if nombre_regex and not re.search(nombre_regex, nombre, re.IGNORECASE):
                    continue

                if grupo_regex and not re.search(grupo_regex, grupo, re.IGNORECASE):
                    continue

                channels.append(dict(row))

            return channels
    except Exception as e:
        print(f"   ⚠️  Error buscando channels por patrón: {e}")
        return []


async def procesar_mapping(source_name: str, display_name: str, variantes: list, stats: dict):
    """Procesa un mapping individual y guarda sus variantes."""
    print(f"\n📝 Procesando: {source_name} -> {display_name}")

    if not variantes:
        print("   ⚠️  No se encontraron variantes en canales.json")
        stats["errores"] += 1
        return

    print(f"   📺 Encontradas {len(variantes)} variaciones")

    channel_ids = []
    qualities = []

    for var in variantes:
        if isinstance(var, dict) and "nombre" in var:
            nombre_iptv = var["nombre"]
            channel_id = await buscar_channel_id_por_nombre(nombre_iptv)

            if channel_id:
                channel_ids.append(channel_id)
                qualities.append(extraer_calidad(nombre_iptv))
                print(f"      ✅ {nombre_iptv} -> {channel_id}")
                stats["variantes_insertadas"] += 1
            else:
                print(f"      ⚠️  No se encontró channel para: {nombre_iptv}")
                stats["variantes_omitidas"] += 1

        elif isinstance(var, dict) and ("nombre_regex" in var or "grupo_regex" in var):
            channels = await buscar_channels_por_patron(var)

            if channels:
                for channel in channels:
                    channel_id = channel["id"]
                    nombre_iptv = channel["nombre"]
                    if channel_id in channel_ids:
                        continue

                    channel_ids.append(channel_id)
                    qualities.append(extraer_calidad(nombre_iptv))
                    print(f"      ✅ {nombre_iptv} -> {channel_id} (patrón)")
                    stats["variantes_insertadas"] += 1
            else:
                print("      ⚠️  No se encontraron channels para patrón dinámico")
                stats["variantes_omitidas"] += 1

    if channel_ids:
        try:
            mapping_id = await ChannelMappingManager.upsert_mapping(
                source_name=source_name,
                display_name=display_name,
                channel_ids=channel_ids,
                qualities=qualities,
            )

            if mapping_id:
                print(f"   ✅ Mapeo guardado (ID: {mapping_id}) con {len(channel_ids)} variantes")
                stats["mapeos_insertados"] += 1
            else:
                print("   ❌ Error guardando mapeo")
                stats["errores"] += 1

        except Exception as e:
            print(f"   ❌ Error: {e}")
            stats["errores"] += 1
    else:
        print("   ⚠️  No se encontraron channel_ids, creando mapeo vacío")
        try:
            mapping_id = await ChannelMappingManager.upsert_mapping(
                source_name=source_name, display_name=display_name
            )
            if mapping_id:
                stats["mapeos_insertados"] += 1
        except Exception as e:
            print(f"   ❌ Error: {e}")
            stats["errores"] += 1


async def main():
    print("🚀 Iniciando poblamiento de tablas de mapeo (Esquema Simplificado)...")
    print()

    mapeo_futbolenlatv, canales_data = load_json_files()
    if not mapeo_futbolenlatv or not canales_data:
        print("❌ Error cargando archivos JSON")
        return

    print(f"📄 Cargado mapeo_futbolenlatv: {len(mapeo_futbolenlatv)} entradas")
    print(f"📄 Cargado canales.json: {len(canales_data)} canales")
    print()

    try:
        await DatabasePG.initialize()  # Inicializa engines iptv-db
    except Exception as e:
        print(f"❌ Error conectando a PostgreSQL: {e}")
        return

    stats = {
        "mapeos_insertados": 0,
        "mapeos_actualizados": 0,
        "variantes_insertadas": 0,
        "variantes_omitidas": 0,
        "errores": 0,
    }

    print("📊 Procesando mapeos...")
    print("-" * 80)

    display_names_procesados = set()

    for source_name, display_name in mapeo_futbolenlatv.items():
        variantes = canales_data.get(display_name, [])
        await procesar_mapping(source_name, display_name, variantes, stats)
        display_names_procesados.add(display_name)

    extras = [
        display_name
        for display_name in canales_data.keys()
        if display_name not in display_names_procesados
    ]

    if extras:
        print("\n📦 Procesando mappings internos adicionales...")
        print("-" * 80)

    for display_name in extras:
        variantes = canales_data.get(display_name, [])
        await procesar_mapping(display_name, display_name, variantes, stats)

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
    asyncio.run(main())
