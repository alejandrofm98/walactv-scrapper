import re
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from database import Database, DataManagerSupabase, ChannelMappingManager

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
    def obtener_fechas():
        today = datetime.now().strftime("%d/%m/%Y")
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")
        return [today, tomorrow]

    def __init__(self):
        self.url = "https://www.futbolenlatv.es/deporte"
        try:
            response = requests.get(self.url, timeout=30)
            response.raise_for_status()
            self.soup = BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            print(f"Error accediendo a {self.url}: {e}")
            self.soup = None
            
        self.canales = []
        try:
            self.mapeos = ChannelMappingManager.get_all_mappings_with_channels_sync()
        except Exception as e:
            print(f"Error cargando mapeos: {e}")
            self.mapeos = {}

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
                    competicion = detalles_td.text.strip() if detalles_td else ""

                    # Extraer categoría desde la imagen
                    categoria = "Otros"
                    if detalles_td:
                        img_tag = detalles_td.find("img")
                        if img_tag and img_tag.get("alt"):
                            categoria = img_tag.get("alt").strip()

                    if len(tds) > 2:
                        equipos = tds[2].text.strip()

                        # Ajustar equipos si hay columna extra
                        if num_columnas == self.COLUMNAS_EVENTO_NORMAL and len(tds) > 3:
                            equipos = f"{equipos} vs {tds[3].text.strip()}"

                        # Verificar si tiene canales mapeados
                        if self.existe_mapeo(
                            tds[num_columnas - 1],
                            competicion=competicion,
                            categoria=categoria,
                            equipos=equipos,
                        ):
                            cont += 1
                            eventos[cont] = {
                                "hora": hora,
                                "competicion": competicion,
                                "categoria": categoria,
                                "equipos": equipos,
                                "canales": self.canales
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
                if canal in self.mapeos
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

            for source_name in self.mapeos.keys():
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
