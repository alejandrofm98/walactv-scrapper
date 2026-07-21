import asyncio
import re
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
from sqlalchemy import text

from database import ChannelMappingManager, DatabasePG, DataManagerSupabase
from services.event_images import borrar_imagenes_eventos_fechas
from services.football_logos import FootballLogosResolver
from services.tennis_flags import TennisFlagsResolver
from utils.constants import (
    HEALTH_CHECK_BYTES,
    HEALTH_CHECK_CONCURRENCY,
    HEALTH_CHECK_TIMEOUT,
)


def limpia_html(html_canal):
    html_canal = html_canal.replace("&gt", "")
    html_canal = html_canal.replace("&lt", "")
    html_canal = html_canal.replace(";", "")
    html_canal = html_canal.replace("strong", "")
    html_canal = re.sub(r"\(.+", "", html_canal)
    return html_canal


class ScrapperFutbolenlatv:
    publicidad = [
        "DAZN (Regístrate)",
        "Amazon Prime Video (Prueba gratis)",
        "GolStadium Premium (acceder)",
        "MAX",
        "DAZN App Gratis (Regístrate)",
        "DAZN (Ver en directo)",
    ]
    CANALES_UFC_PREFERIDOS = {
        "default": [
            "TNT SPORTS",
            "PARAMOUNT PRELIMS ES",
            "PARAMOUNT PRELIMS EN",
            "PARAMOUNT MAIN ES",
            "PARAMOUNT MAIN EN",
        ],
        "prelims": ["PARAMOUNT PRELIMS ES", "PARAMOUNT PRELIMS EN"],
        "main": ["PARAMOUNT MAIN ES", "PARAMOUNT MAIN EN"],
    }

    COLUMNAS_EVENTO_NORMAL = 5
    COLUMNAS_EVENTO_DIFERENTE = 4
    REQUEST_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    }

    @staticmethod
    def generate_document_name(fecha):
        return "calendario_" + fecha.replace("/", ".")

    @staticmethod
    def guarda_partidos(eventos, fecha):
        DataManagerSupabase.guardar_calendario(eventos, fecha)

    @staticmethod
    async def guarda_partidos_async(todos_eventos):
        """
        Guarda todos los eventos de todas las fechas.
        Cada fecha en su propia transacción. F3d2: migrado a iptv-db.
        """
        try:
            session_factory = DatabasePG.get_session_factory()

            # DDL: se ejecuta en sesión separada por seguridad
            async with session_factory() as ddl_session:
                await ddl_session.execute(
                    text("""
                        ALTER TABLE calendario
                        ADD COLUMN IF NOT EXISTS imagen_evento TEXT,
                        ADD COLUMN IF NOT EXISTS subtitulo_competicion TEXT,
                        DROP COLUMN IF EXISTS imagen_local,
                        DROP COLUMN IF EXISTS imagen_visitante
                    """)
                )
                await ddl_session.commit()

            for fecha_str, partidos in todos_eventos.items():
                for fecha in fecha_str if isinstance(fecha_str, list) else [fecha_str]:
                    try:
                        fecha_date = datetime.strptime(fecha, "%d/%m/%Y").date()
                    except ValueError:
                        continue

                    partidos_validos = [
                        partido for partido in partidos.values() if isinstance(partido, dict)
                    ]
                    if not partidos_validos:
                        continue

                    async with session_factory() as session:
                        await session.execute(
                            text("DELETE FROM calendario WHERE fecha = :fecha"),
                            {"fecha": fecha_date},
                        )

                        for partido in partidos_validos:
                            try:
                                await session.execute(
                                    text("""
                                        INSERT INTO calendario (
                                            fecha, hora, equipos, competicion, canales, categoria,
                                            imagen_evento, subtitulo_competicion
                                        )
                                        VALUES (:fecha, :hora, :equipos, :competicion, :canales, :categoria,
                                                :imagen_evento, :subtitulo_competicion)
                                        ON CONFLICT (fecha, hora, equipos) DO UPDATE SET
                                            hora = EXCLUDED.hora,
                                            competicion = EXCLUDED.competicion,
                                            canales = EXCLUDED.canales,
                                            categoria = EXCLUDED.categoria,
                                            imagen_evento = EXCLUDED.imagen_evento,
                                            subtitulo_competicion = EXCLUDED.subtitulo_competicion
                                    """),
                                    {
                                        "fecha": fecha_date,
                                        "hora": partido.get("hora", "00:00"),
                                        "equipos": partido.get("equipos", ""),
                                        "competicion": partido.get("competicion", ""),
                                        "canales": partido.get("canales", []),
                                        "categoria": partido.get("categoria", ""),
                                        "imagen_evento": partido.get("imagen_evento", ""),
                                        "subtitulo_competicion": partido.get(
                                            "subtitulo_competicion", ""
                                        ),
                                    },
                                )
                            except Exception as e:
                                print(
                                    f"❌ Error guardando partido '{partido.get('equipos', '')}': {e}"
                                )

                        await session.commit()

        except Exception as e:
            print(f"❌ Error general guardando calendario: {e}")
            raise

    @staticmethod
    def obtener_fechas():
        today = datetime.now().strftime("%d/%m/%Y")
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")
        return [today, tomorrow]

    def __init__(self, mapeos=None, football_logos_proxy=""):
        self.url = "https://www.futbolenlatv.es/deporte"
        try:
            response = requests.get(self.url, headers=self.REQUEST_HEADERS, timeout=30)
            response.raise_for_status()
            self.soup = BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            print(f"Error accediendo a {self.url}: {e}")
            self.soup = None

        self.canales = []
        self._mapeos_cache = mapeos if mapeos is not None else {}
        self._football_logos_resolver = FootballLogosResolver(proxy_url=football_logos_proxy)
        self._tennis_flags_resolver = TennisFlagsResolver()
        self._proxy_url = football_logos_proxy

    @staticmethod
    def _mejorar_url_futbolenlatv(url):
        if not url:
            return ""
        return url.replace("/img/32/", "/img/")

    def _extraer_info_imagen_equipo(self, td):
        if not td:
            return "", ""

        img = td.find("img")
        nombre = ""
        url = ""

        if img:
            nombre = (img.get("alt") or img.get("title") or "").strip()
            url = img.get("src") or img.get("alt-img") or ""

        if not nombre:
            nombre = td.text.strip()

        return nombre, self._mejorar_url_futbolenlatv(url)

    def _generar_imagen_evento_futbol(
        self,
        fecha,
        hora,
        nombre_local,
        nombre_visitante,
        fallback_local,
        fallback_visitante,
        contexto_competicion="",
    ):
        if not nombre_local or not nombre_visitante:
            return ""

        try:
            fecha_slug = datetime.strptime(fecha, "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            fecha_slug = re.sub(r"[^a-z0-9]+", "-", (fecha or "").lower()).strip("-")

        logo_local_descargado = ""
        logo_visitante_descargado = ""
        try:
            try:
                logo_local_descargado = self._football_logos_resolver.resolver_logo(
                    nombre_local,
                    contexto=contexto_competicion,
                )
            except Exception as e:
                print(f"⚠️ Error descargando logo '{nombre_local}', usando fallback: {e}")

            try:
                logo_visitante_descargado = self._football_logos_resolver.resolver_logo(
                    nombre_visitante,
                    contexto=contexto_competicion,
                )
            except Exception as e:
                print(f"⚠️ Error descargando logo '{nombre_visitante}', usando fallback: {e}")

            logo_local = logo_local_descargado or fallback_local
            logo_visitante = logo_visitante_descargado or fallback_visitante
            if not logo_local or not logo_visitante:
                return ""

            from services.event_images import generar_imagen_evento

            imagen_evento = generar_imagen_evento(
                nombre_local=nombre_local,
                nombre_visitante=nombre_visitante,
                logo_local=logo_local,
                logo_visitante=logo_visitante,
                fecha_slug=fecha_slug,
                hora=hora,
            )
            self._football_logos_resolver.eliminar_logo_temporal(logo_local_descargado)
            self._football_logos_resolver.eliminar_logo_temporal(logo_visitante_descargado)
            return imagen_evento
        except Exception as e:
            print(f"⚠️ Error generando imagen evento '{nombre_local} vs {nombre_visitante}': {e}")
            return ""

    def _generar_imagen_evento_tenis(
        self,
        fecha,
        hora,
        nombre_local,
        nombre_visitante,
        fallback_local,
        fallback_visitante,
        contexto_competicion="",
    ):
        if not nombre_local or not nombre_visitante:
            return ""

        bandera_local_resuelta = self._tennis_flags_resolver.resolver_bandera(
            fallback_local,
            nombre_jugador=nombre_local,
        )
        bandera_visitante_resuelta = self._tennis_flags_resolver.resolver_bandera(
            fallback_visitante,
            nombre_jugador=nombre_visitante,
        )
        if not bandera_local_resuelta:
            print(
                f"⚠️ Bandera local no resuelta para '{nombre_local}', usando fallback: {fallback_local}"
            )
        if not bandera_visitante_resuelta:
            print(
                f"⚠️ Bandera visitante no resuelta para '{nombre_visitante}', "
                f"usando fallback: {fallback_visitante}"
            )

        bandera_local = bandera_local_resuelta or fallback_local
        bandera_visitante = bandera_visitante_resuelta or fallback_visitante
        if not bandera_local or not bandera_visitante:
            return ""

        try:
            fecha_slug = datetime.strptime(fecha, "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            fecha_slug = re.sub(r"[^a-z0-9]+", "-", (fecha or "").lower()).strip("-")

        try:
            from services.event_images import generar_imagen_evento_tenis

            return generar_imagen_evento_tenis(
                nombre_local=nombre_local,
                nombre_visitante=nombre_visitante,
                bandera_local=bandera_local,
                bandera_visitante=bandera_visitante,
                fecha_slug=fecha_slug,
                hora=hora,
                competicion=contexto_competicion,
            )
        except Exception as e:
            print(f"⚠️ Error generando imagen tenis '{nombre_local} vs {nombre_visitante}': {e}")
            return ""

    def _generar_imagen_evento(
        self,
        categoria,
        fecha,
        hora,
        nombre_local,
        nombre_visitante,
        fallback_local,
        fallback_visitante,
        contexto_competicion="",
    ):
        from services.event_images import obtener_imagen_evento_default

        imagen_default = obtener_imagen_evento_default(categoria, contexto_competicion)

        if categoria == "Fútbol":
            imagen = self._generar_imagen_evento_futbol(
                fecha,
                hora,
                nombre_local,
                nombre_visitante,
                fallback_local,
                fallback_visitante,
                contexto_competicion,
            )
            if imagen:
                return imagen
            return imagen_default

        if categoria == "Tenis":
            imagen = self._generar_imagen_evento_tenis(
                fecha,
                hora,
                nombre_local,
                nombre_visitante,
                fallback_local,
                fallback_visitante,
                contexto_competicion,
            )
            return imagen or imagen_default

        if self.es_evento_ufc(
            competicion=contexto_competicion,
            categoria=categoria,
            equipos=f"{nombre_local} vs {nombre_visitante}",
        ):
            imagen = self._obtener_imagen_evento_ufc()
            if imagen:
                return imagen
            return imagen_default

        return imagen_default

    def _obtener_imagen_evento_ufc(self):
        if hasattr(self, "_ufc_imagen_cache"):
            return self._ufc_imagen_cache
        self._ufc_imagen_cache = ""

        try:
            proxies = None
            if self._proxy_url:
                proxies = {"http": self._proxy_url, "https": self._proxy_url}
            resp = requests.get(
                "https://www.ufcespanol.com/events",
                headers=self.REQUEST_HEADERS,
                proxies=proxies,
                timeout=30,
            )
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            picture = soup.find("picture")
            if picture:
                source = picture.find("source")
                if source:
                    srcset = source.get("srcset", "")
                    url = srcset.split(" ")[0]
                    self._ufc_imagen_cache = url
        except Exception as e:
            print(f"⚠️ Error obteniendo imagen UFC: {e}")

        return self._ufc_imagen_cache

    @staticmethod
    def _extraer_competicion(detalles_td):
        if not detalles_td:
            return "", ""

        label = detalles_td.find("label")
        if label:
            competicion = (label.get("title") or label.text or "").strip()
        else:
            competicion = detalles_td.text.strip()

        contenedor = detalles_td.find("span", {"class": "ajusteDoslineas"})
        subtitulo_tag = contenedor.find("span", title=True) if contenedor else None
        subtitulo = ""
        if subtitulo_tag:
            subtitulo = (subtitulo_tag.get("title") or subtitulo_tag.text or "").strip()

        return competicion, subtitulo

    def _get_mapeos(self):
        """Retorna los mapeos (ya cargados o vacíos si no se proporcionaron)"""
        return self._mapeos_cache if self._mapeos_cache is not None else {}

    def existe_fecha(self, fecha):
        if not self.soup:
            return False
        # Verificar si existe en la página parseada
        cabecera_tabla = self.soup.find("td", string=re.compile(fecha))
        return cabecera_tabla is not None

    def obtener_partidos(self, fecha):
        if not self.soup:
            return {}

        # Primero verificamos si hay tabla para esta fecha
        if not self.existe_fecha(fecha):
            print(f"No se encontró tabla para la fecha {fecha}")
            return {}

        try:
            carpetas_borradas = borrar_imagenes_eventos_fechas([fecha])
            if carpetas_borradas:
                print(
                    f"🧹 Carpetas de imágenes de eventos borradas para {fecha}: {carpetas_borradas}"
                )
        except Exception as e:
            print(f"⚠️ Error borrando imágenes de eventos para {fecha}: {e}")

        try:
            cabecera = self.soup.find("td", string=re.compile(fecha))
            if not cabecera:
                return {}

            tbody = cabecera.find_parent("tbody")
            if not tbody:
                return {}

            trs = tbody.find_all("tr")
            eventos = {}
            cont = 0

            for tr in trs:
                tds = tr.find_all("td")
                if not tds:
                    continue

                num_columnas = len(tds)
                self.canales = []

                # Verificar si es una fila de evento válido
                # En BS4, attrs['class'] devuelve una lista
                clases = tds[0].get("class", [])

                if "hora" in clases:
                    hora = tds[0].text.strip()

                    # Manejo seguro de 'detalles'
                    detalles_td = tr.find("td", {"class": "detalles"})
                    competicion, subtitulo_competicion = self._extraer_competicion(detalles_td)
                    texto_competicion = f"{competicion} {subtitulo_competicion}".strip()

                    # Extraer categoría desde la imagen
                    categoria = "Otros"
                    if detalles_td:
                        img_tag = detalles_td.find("img")
                        if img_tag and img_tag.get("alt"):
                            categoria = img_tag.get("alt").strip()

                    if len(tds) > 2:
                        equipos = tds[2].text.strip()
                        local_td = tr.find("td", {"class": "local"})
                        visitante_td = tr.find("td", {"class": "visitante"})
                        nombre_local, fallback_local = self._extraer_info_imagen_equipo(local_td)
                        nombre_visitante, fallback_visitante = self._extraer_info_imagen_equipo(
                            visitante_td
                        )

                        # Ajustar equipos si hay columna extra
                        if num_columnas == self.COLUMNAS_EVENTO_NORMAL and len(tds) > 3:
                            equipos = f"{equipos} vs {tds[3].text.strip()}"

                        # Verificar si tiene canales mapeados
                        if self.existe_mapeo(
                            tds[num_columnas - 1],
                            competicion=texto_competicion,
                            categoria=categoria,
                            equipos=equipos,
                        ):
                            imagen_evento = self._generar_imagen_evento(
                                categoria,
                                fecha,
                                hora,
                                nombre_local,
                                nombre_visitante,
                                fallback_local,
                                fallback_visitante,
                                texto_competicion,
                            )

                            cont += 1
                            eventos[cont] = {
                                "hora": hora,
                                "competicion": competicion,
                                "subtitulo_competicion": subtitulo_competicion,
                                "categoria": categoria,
                                "equipos": equipos,
                                "canales": self.canales,
                                "imagen_evento": imagen_evento,
                            }
            return eventos
        except Exception as e:
            print(f"Error obteniendo partidos para {fecha}: {e}")
            return {}

    def es_evento_ufc(self, competicion: str = "", categoria: str = "", equipos: str = "") -> bool:
        texto_evento = " ".join([competicion or "", categoria or "", equipos or ""]).upper()
        return "UFC" in texto_evento

    def obtener_fase_ufc(
        self, competicion: str = "", categoria: str = "", equipos: str = ""
    ) -> str:
        texto_evento = " ".join([competicion or "", categoria or "", equipos or ""]).upper()

        if "PRELIMS" in texto_evento:
            return "prelims"

        if "MAIN" in texto_evento:
            return "main"

        return "default"

    def obtener_canales_manuales_evento(
        self, competicion: str = "", categoria: str = "", equipos: str = ""
    ) -> list:
        if self.es_evento_ufc(competicion, categoria, equipos):
            fase_ufc = self.obtener_fase_ufc(competicion, categoria, equipos)
            canales_preferidos = list(self.CANALES_UFC_PREFERIDOS.get("default", []))
            canales_preferidos.extend(self.CANALES_UFC_PREFERIDOS.get(fase_ufc, []))

            canales_disponibles = [
                canal for canal in canales_preferidos if canal in self._get_mapeos()
            ]

            if canales_disponibles:
                return canales_disponibles

            return canales_preferidos

        return []

    def existe_mapeo(self, td, competicion: str = "", categoria: str = "", equipos: str = ""):
        if not td:
            canales_manuales = self.obtener_canales_manuales_evento(competicion, categoria, equipos)
            if canales_manuales:
                self.canales.extend(canales_manuales)
                return True
            return False

        lista_canales = td.find_all("li")
        resultado = False

        if not lista_canales:
            canales_manuales = self.obtener_canales_manuales_evento(competicion, categoria, equipos)
            if canales_manuales:
                self.canales.extend(canales_manuales)
                return True
            return False

        for canal in lista_canales:
            canal_title = canal.get("title", "").strip()

            # Limpieza básica inline
            canal_title = re.sub(r"\(.+", "", canal_title).strip()

            if not canal_title or canal_title in self.publicidad:
                continue

            # Nuevo sistema simplificado: buscar directamente en los mapeos
            # Los mapeos tienen como clave el source_name (nombre de futbolenlatv)
            canal_lower = canal_title.lower()
            encontrado = False

            for source_name in self._get_mapeos().keys():
                if source_name.lower() == canal_lower:
                    encontrado = True
                    if source_name not in self.canales:
                        self.canales.append(source_name)
                    break

            if encontrado:
                resultado = True

        canales_manuales = self.obtener_canales_manuales_evento(competicion, categoria, equipos)
        for canal_manual in canales_manuales:
            if canal_manual not in self.canales:
                self.canales.append(canal_manual)

        if canales_manuales:
            resultado = True

        return resultado

    def limpia_canales(self, lista_canales):
        pass


# === Health check para canales de eventos ===

_VIDEO_CONTENT_TYPES = [
    "video/mp2t",
    "video/mp4",
    "video/x-flv",
    "video/quicktime",
    "video/x-matroska",
    "video/webm",
    "video/3gpp",
    "video/ogg",
    "video/mpeg",
    "application/vnd.apple.mpegurl",
    "application/x-mpegurl",
]


def _check_single_channel(url: str, timeout: int, bytes_to_check: int) -> tuple:
    """
    Verifica un stream HTTP. Retorna (is_alive, response_time_ms, info).
    """
    try:
        start = time.time()
        response = requests.get(
            url,
            headers={"Range": f"bytes=0-{bytes_to_check - 1}"},
            timeout=timeout,
        )
        elapsed_ms = int((time.time() - start) * 1000)

        if not response.ok:
            return False, elapsed_ms, f"HTTP {response.status_code}"

        content_type = (response.headers.get("Content-Type", "") or "").lower()
        body = response.content

        if len(body) == 0:
            return False, elapsed_ms, "empty_body"

        if not any(vct in content_type for vct in _VIDEO_CONTENT_TYPES):
            return False, elapsed_ms, f"bad_ct:{content_type[:30]}"

        return True, elapsed_ms, "ok"
    except requests.Timeout:
        return False, timeout * 1000, "timeout"
    except Exception as e:
        return False, 0, str(e)[:50]


async def verificar_salud_canales_evento(
    source_names: set[str],
    provider_username: str,
    provider_password: str,
):
    """
    Verifica salud de streams para source_names obtenidos de eventos.
    Solo testea canales que aparecen en eventos, no todos los del IPTV.
    """
    variants_map = await ChannelMappingManager.get_variants_for_source_names(list(source_names))
    if not variants_map:
        return

    total = sum(len(v) for v in variants_map.values())
    if total == 0:
        print("ℹ️ Health check: sin variantes con stream_url")
        return

    print(f"\n🔍 Verificando salud de {total} streams...")

    sem = asyncio.Semaphore(HEALTH_CHECK_CONCURRENCY)
    stats = {"ok": 0, "error": 0}
    completed = 0
    total_checks = total

    async def _check_variant(sn: str, variant: dict):
        nonlocal completed
        async with sem:
            stream_url = variant["stream_url"]
            if not stream_url:
                return

            test_url = stream_url.replace("{{USERNAME}}", provider_username).replace(
                "{{PASSWORD}}", provider_password
            )
            if test_url == stream_url:
                return

            is_alive, ms, info = await asyncio.get_event_loop().run_in_executor(
                None, _check_single_channel, test_url, HEALTH_CHECK_TIMEOUT, HEALTH_CHECK_BYTES
            )

            estado = "ok" if is_alive else "error"
            await ChannelMappingManager.update_channel_health(variant["channel_id"], estado, ms)

            completed += 1
            icon = "✅" if is_alive else "❌"
            print(f"  {icon} [{variant['quality']}] {sn} ({ms}ms)  {'' if is_alive else info}")
            stats["ok" if is_alive else "error"] += 1

    tasks = [_check_variant(sn, v) for sn, vars_list in variants_map.items() for v in vars_list]
    await asyncio.gather(*tasks)

    print(
        f"  📊 Health check: ✅ {stats['ok']} ok, ❌ {stats['error']} error (de {total_checks} total)"
    )
