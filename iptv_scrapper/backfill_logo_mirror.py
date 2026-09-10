"""Backfill: des-anida y espeja los logos legacy de canales en BD (Opción C).

Solo toca la tabla `channels` de los paises configurados en
LOGO_MIRROR_COUNTRIES (config PG; default ES,UK,US,WO = los que filtran las
apps Desktop/TV). Descarga cada logo una vez al volumen compartido y
reescribe la columna logo con la URL propia servida por nginx
({PUBLIC_DOMAIN}/images/logos/channels/...). Idempotente: los archivos ya
descargados no se re-descargan y las filas ya espejadas no se tocan.

 movies_catalog y series_catalog NO se tocan: la app usa poster TMDB y su
logo M3U sigue con el proxy simple legacy.

Uso (dentro del contenedor walactv-sync-iptv, con el volumen montado):
    python backfill_logo_mirror.py            # aplica
    python backfill_logo_mirror.py --dry-run  # solo reporta
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text

from config import get_settings
from database import DatabasePG
from services.logo_mirror import (
    buscar_override,
    decode_nested,
    parsear_paises_config,
    premirror,
    reset_run_state,
    resolver_logo,
)

TABLE = "channels"
WHERE_LEGACY = (
    "(logo LIKE '%/logo?url=%')"
    " OR (logo LIKE 'http://%' AND logo NOT LIKE '%/images/logos/%')"
)


def _resumir(url: str) -> str:
    return (url or "")[:90]


async def backfill(dry_run: bool) -> int:
    settings = get_settings()
    await settings._load_config()
    public_domain = settings.public_domain
    print(f"PUBLIC_DOMAIN: {public_domain} | dry_run: {dry_run}")

    await DatabasePG.initialize()
    session_factory = DatabasePG.get_session_factory()

    async with session_factory() as session:
        result = await session.execute(
            text("SELECT value FROM config WHERE key = 'LOGO_MIRROR_COUNTRIES'")
        )
        row = result.first()
    paises = parsear_paises_config(row[0] if row else None)
    print(f"Paises objetivo: {paises if paises else '(todos)'}")

    params: dict = {}
    # Parentesis obligatorios: WHERE_LEGACY tiene un OR y el filtro de pais
    # debe aplicar a ambas ramas
    where = f"({WHERE_LEGACY})"
    if paises:
        where += " AND country = ANY(:paises)"
        params["paises"] = list(paises)

    async with session_factory() as session:
        result = await session.execute(
            text(f"SELECT id, logo, tvg_id, provider_id FROM {TABLE} WHERE logo IS NOT NULL AND {where}"),
            params,
        )
        rows = result.all()
    if not rows:
        print(f"\n[{TABLE}] sin logos legacy para los paises objetivo")
        await DatabasePG.close()
        return 0

    print(f"\n[{TABLE}] {len(rows):,} canales con logo legacy")
    reset_run_state()

    urls_unicas: set[str] = set()
    for _, logo, _, _ in rows:
        crudo = decode_nested(logo)
        if crudo.startswith("http://"):
            urls_unicas.add(crudo)

    premirror(sorted(urls_unicas), "channel")

    cambios: list[tuple[str, str]] = []
    por_override = 0
    for row_id, logo, tvg_id, provider_id in rows:
        nuevo = resolver_logo(logo, "channel", public_domain)
        if "/placeholder/" in nuevo:
            override = buscar_override([tvg_id, provider_id], public_domain)
            if override:
                nuevo = override
                por_override += 1
        if nuevo != logo:
            cambios.append((row_id, nuevo))

    a_propia = [c for c in cambios if "/images/logos/" in c[1]]
    a_placeholder = [c for c in cambios if "/placeholder/" in c[1]]
    print(f"  -> a reescribir: {len(cambios):,} (URL propia: {len(a_propia):,} | placeholder: {len(a_placeholder):,} | override: {por_override:,})")
    for row_id, nuevo in a_propia[:3]:
        print(f"     ej: {_resumir(nuevo)}")

    if dry_run or not cambios:
        await DatabasePG.close()
        print("\nDone")
        return 0

    batch = 500
    for start in range(0, len(cambios), batch):
        chunk = cambios[start : start + batch]
        async with session_factory() as session:
            await session.execute(
                text(
                    f"UPDATE {TABLE} SET logo = data.logo FROM ("
                    "  SELECT unnest(CAST(:ids AS text[])) AS id,"
                    "  unnest(CAST(:logos AS text[])) AS logo"
                    f") AS data WHERE {TABLE}.id = data.id"
                ),
                {"ids": [c[0] for c in chunk], "logos": [c[1] for c in chunk]},
            )
            await session.commit()

    print(f"  ✅ {TABLE}: {len(cambios):,} logos reescritos")
    await DatabasePG.close()
    print("\nDone")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill de logos de canales al espejo propio")
    parser.add_argument("--dry-run", action="store_true", help="no escribe en BD")
    args = parser.parse_args()
    return asyncio.run(backfill(args.dry_run))


if __name__ == "__main__":
    sys.exit(main())
