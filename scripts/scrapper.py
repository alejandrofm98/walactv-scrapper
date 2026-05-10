import re
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from database import DataManagerSupabase, DatabasePG
from services.football_logos import FootballLogosResolver

def limpia_html(html_canal):
    html_canal = html_canal.replace("&gt", "")
    html_canal = html_canal.replace("&lt", "")
    html_canal = html_canal.replace(";", "")
    html_canal = html_canal.replace("strong", "")
    html_canal = re.sub(r'\(.+', "", html_canal)
    return html_canal

class ScrapperFutbolenlatv:
    publicidad = ["DAZN (Regístrate)", "Amazon Prime Video (Prueba gratis)",
                  "GolStadium Premium (acceder)", "MAX",
                  "DAZN App Gratis (Regístrate)", "DAZN (Ver en directo)"]
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

    @staticmethod
    def generate_document_name(fecha):
        return "calendario_" + fecha.replace("/", ".")

    @staticmethod
    def guarda_partidos(eventos, fecha):
        DataManagerSupabase.guardar_calendario(eventos, fecha)

    @staticmethod
    async def guarda_partidos_async(todos_eventos):
        """
        Guarda todos los eventos de todas las fechas en un solo batch.
        Esto evita llamar asyncio.run() múltiples veces y cierra el pool correctamente.
        """
        try:
            # Inicializar pool una sola vez
            pool = await DatabasePG.initialize()

            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    ALTER TABLE calendario
                    ADD COLUMN IF NOT EXISTS imagen_evento TEXT,
                    ADD COLUMN IF NOT EXISTS subtitulo_competicion TEXT,
                    DROP COLUMN IF EXISTS imagen_local,
                    DROP COLUMN IF EXISTS imagen_visitante
                    """
                )

                for fecha_str, partidos in todos_eventos.items():
                    for fecha in fecha_str if isinstance(fecha_str, list) else [fecha_str]:
                        try:
                            fecha_date = datetime.strptime(fecha, '%d/%m/%Y').date()
                        except ValueError:
                            continue

                        for key, partido in partidos.items():
                            if isinstance(partido, dict):
                                try:
                                    await conn.execute(
                                        """
                                        INSERT INTO calendario (
                                            fecha, hora, equipos, competicion, canales, categoria,
                                            imagen_evento, subtitulo_competicion
                                        )
                                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                                        ON CONFLICT (fecha, hora, equipos) DO UPDATE SET
                                            hora = EXCLUDED.hora,
                                            competicion = EXCLUDED.competicion,
                                            canales = EXCLUDED.canales,
                                            categoria = EXCLUDED.categoria,
                                            imagen_evento = EXCLUDED.imagen_evento,
                                            subtitulo_competicion = EXCLUDED.subtitulo_competicion
                                        """,
                                        fecha_date,
                                        partido.get('hora', '00:00'),
                                        partido.get('equipos', ''),
                                        partido.get('competicion', ''),
                                        partido.get('canales', []),
                                        partido.get('categoria', ''),
                                        partido.get('imagen_evento', ''),
                                        partido.get('subtitulo_competicion', '')
                                    )
                                except Exception as e:
                                    print(f"❌ Error guardando partido '{partido.get('equipos', '')}': {e}")

        except Exception as e:
            print(f"❌ Error general guardando calendario: {e}")
            raise

    @staticmethod
    def obtener_fechas():
        today = datetime.now().strftime("%d/%m/%Y")
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")
        return [today, tomorrow]

    def __init__(self, mapeos=None):
        self.url = "https://www.futbolenlatv.es/deporte"
        try:
            response = requests.get(self.url, timeout=30)
            response.raise_for_status()
            self.soup = BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            print(f"Error accediendo a {self.url}: {e}")
            self.soup = None
            
        self.canales = []
        self._mapeos_cache = mapeos if mapeos is not None else {}
        self._football_logos_resolver = FootballLogosResolver()

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

        logo_local_descargado = self._football_logos_resolver.resolver_logo(
            nombre_local,
            contexto=contexto_competicion,
        )
        logo_visitante_descargado = self._football_logos_resolver.resolver_logo(
            nombre_visitante,
            contexto=contexto_competicion,
        )
        logo_local = logo_local_descargado or fallback_local
        logo_visitante = logo_visitante_descargado or fallback_visitante
        if not logo_local or not logo_visitante:
            return ""

        try:
            fecha_slug = datetime.strptime(fecha, "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            fecha_slug = re.sub(r"[^a-z0-9]+", "-", (fecha or "").lower()).strip("-")

        try:
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
                        nombre_visitante, fallback_visitante = self._extraer_info_imagen_equipo(visitante_td)

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
                            imagen_evento = ""
                            if categoria == "Fútbol":
                                imagen_evento = self._generar_imagen_evento_futbol(
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
                                "imagen_evento": imagen_evento
                            }
            return eventos
        except Exception as e:
            print(f"Error obteniendo partidos para {fecha}: {e}")
            return {}

    def es_evento_ufc(self, competicion: str = "", categoria: str = "", equipos: str = "") -> bool:
        texto_evento = " ".join([competicion or "", categoria or "", equipos or ""]).upper()
        return "UFC" in texto_evento

    def obtener_fase_ufc(self, competicion: str = "", categoria: str = "", equipos: str = "") -> str:
        texto_evento = " ".join([competicion or "", categoria or "", equipos or ""]).upper()

        if "PRELIMS" in texto_evento:
            return "prelims"

        if "MAIN" in texto_evento:
            return "main"

        return "default"

    def obtener_canales_manuales_evento(self, competicion: str = "", categoria: str = "",
                                       equipos: str = "") -> list:
        if self.es_evento_ufc(competicion, categoria, equipos):
            fase_ufc = self.obtener_fase_ufc(competicion, categoria, equipos)
            canales_preferidos = list(self.CANALES_UFC_PREFERIDOS.get("default", []))
            canales_preferidos.extend(self.CANALES_UFC_PREFERIDOS.get(fase_ufc, []))

            canales_disponibles = [
                canal for canal in canales_preferidos
                if canal in self._get_mapeos()
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
