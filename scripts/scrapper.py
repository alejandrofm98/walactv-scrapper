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
            # Nuevo sistema simplificado: source_name -> [channel_ids]
            self.mapeos = ChannelMappingManager.get_all_mappings_with_channels()
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
                    # Verificar si tiene canales mapeados
                    if self.existe_mapeo(tds[num_columnas - 1]):
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

    def existe_mapeo(self, td):
        if not td:
            return False

        lista_canales = td.find_all("li")
        if not lista_canales:
            return False

        resultado = False

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

        return resultado

    def limpia_canales(self, lista_canales):
        pass
