import html
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree


SITEMAP_URL = "https://football-logos.cc/image-sitemap.xml"
USER_AGENT = "Mozilla/5.0 (compatible; walactv-scrapper/1.0)"
BASE_DIR = Path(__file__).resolve().parents[1]
PROJECT_DIR = Path(__file__).resolve().parents[2]
if not (PROJECT_DIR / "resources").exists():
    PROJECT_DIR = BASE_DIR
LOGOS_DIR = PROJECT_DIR / "resources" / "logos_equipos" / "football-logos"

SITEMAP_NS = {
    "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
    "image": "http://www.google.com/schemas/sitemap-image/1.1",
}

STOPWORDS = {
    "afc", "athletic", "ca", "cd", "cf", "club", "de", "fc", "femenino",
    "football", "fs", "team", "women",
}

COMPETICION_PAISES = {
    "premier league": ("england",),
    "la liga ea sports": ("spain",),
    "laliga ea sports": ("spain",),
    "serie a italiana": ("italy",),
    "laliga hypermotion": ("spain",),
    "saudi pro league": ("saudi-arabia",),
    "liga f": ("spain",),
    "primera division argentina": ("argentina",),
}


@dataclass(frozen=True)
class CandidatoLogo:
    nombre: str
    nombre_normalizado: str
    tokens: set[str]
    pais: str
    pagina_url: str
    imagen_url: str
    score: float = 0.0


def normalizar_texto(texto: str) -> str:
    texto = unicodedata.normalize("NFKD", texto or "")
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    texto = texto.lower().replace("&", " and ")
    texto = re.sub(r"\bat\.?\b", " atletico ", texto)
    texto = re.sub(r"\butd\b", " united ", texto)
    texto = re.sub(r"\bjeddah\s+club\b", " ", texto)
    texto = re.sub(r"\([^)]*\)", " ", texto)
    texto = re.sub(r"[^a-z0-9]+", " ", texto)
    return re.sub(r"\s+", " ", texto).strip()


def crear_slug(texto: str) -> str:
    return "-".join(normalizar_texto(texto).split()) or "logo"


def tokens_relevantes(texto: str) -> set[str]:
    return {token for token in normalizar_texto(texto).split() if token not in STOPWORDS}


def inferir_paises_preferidos(contexto: str = "") -> tuple[str, ...]:
    contexto_normalizado = normalizar_texto(contexto)
    for competicion, paises in COMPETICION_PAISES.items():
        if competicion in contexto_normalizado:
            return paises
    return ()


def descargar_texto(url: str) -> str:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=45) as response:
        return response.read().decode("utf-8")


def descargar_binario(url: str) -> bytes:
    request = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Referer": "https://football-logos.cc/",
            "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
        },
    )
    with urlopen(request, timeout=45) as response:
        return response.read()


def nombre_desde_titulo(titulo: str) -> str:
    return re.sub(r"\s+logo\s*$", "", titulo.strip(), flags=re.IGNORECASE)


def extraer_pais(pagina_url: str) -> str:
    partes = [parte for parte in urlparse(pagina_url).path.split("/") if parte]
    return partes[0] if partes else ""


def parsear_sitemap(xml: str) -> list[CandidatoLogo]:
    raiz = ElementTree.fromstring(xml)
    candidatos = []

    for url_node in raiz.findall("sm:url", SITEMAP_NS):
        loc_node = url_node.find("sm:loc", SITEMAP_NS)
        image_node = url_node.find("image:image", SITEMAP_NS)
        if loc_node is None or image_node is None:
            continue

        image_loc_node = image_node.find("image:loc", SITEMAP_NS)
        image_title_node = image_node.find("image:title", SITEMAP_NS)
        if image_loc_node is None or image_title_node is None:
            continue

        pagina_url = loc_node.text or ""
        imagen_url = image_loc_node.text or ""
        titulo = image_title_node.text or ""
        if not pagina_url or not imagen_url or not titulo:
            continue

        nombre = nombre_desde_titulo(titulo)
        candidatos.append(
            CandidatoLogo(
                nombre=nombre,
                nombre_normalizado=normalizar_texto(nombre),
                tokens=tokens_relevantes(nombre),
                pais=extraer_pais(pagina_url),
                pagina_url=pagina_url,
                imagen_url=imagen_url,
            )
        )

    return candidatos


def calcular_score(busqueda: str, candidato: CandidatoLogo, paises_preferidos: tuple[str, ...] = ()) -> float:
    busqueda_normalizada = normalizar_texto(busqueda)
    tokens_busqueda = tokens_relevantes(busqueda)
    if not busqueda_normalizada or not tokens_busqueda:
        return 0.0

    score = 0.0
    if candidato.nombre_normalizado == busqueda_normalizada:
        score += 100
    elif candidato.nombre_normalizado.startswith(busqueda_normalizada):
        score += 72
    elif busqueda_normalizada in candidato.nombre_normalizado:
        score += 62

    comunes = tokens_busqueda & candidato.tokens
    if comunes:
        score += 45 * (len(comunes) / len(tokens_busqueda))
        score += 20 * (len(comunes) / max(len(candidato.tokens), 1))

    score -= 12 * len(tokens_busqueda - candidato.tokens)
    score -= 2 * min(len(candidato.tokens - tokens_busqueda), 6)
    if "national" in candidato.tokens or "tournament" in candidato.pais:
        score -= 12
    if "unofficial" in candidato.nombre_normalizado or "white" in candidato.nombre_normalizado:
        score -= 20
    if "historical" in candidato.nombre_normalizado or "logo-history" in candidato.pagina_url:
        score -= 30
    if paises_preferidos:
        if candidato.pais in paises_preferidos:
            score += 35
        else:
            score -= 15

    return round(max(score, 0.0), 2)


def buscar_candidatos(
    nombre_equipo: str,
    candidatos: list[CandidatoLogo],
    paises_preferidos: tuple[str, ...] = (),
) -> list[CandidatoLogo]:
    puntuados = [
        CandidatoLogo(c.nombre, c.nombre_normalizado, c.tokens, c.pais, c.pagina_url, c.imagen_url,
                      calcular_score(nombre_equipo, c, paises_preferidos))
        for c in candidatos
    ]
    return sorted((c for c in puntuados if c.score > 0), key=lambda c: c.score, reverse=True)


def extraer_atributo(html_pagina: str, nombre: str) -> str | None:
    match = re.search(rf'{nombre}="([^"]+)"', html_pagina)
    return html.unescape(match.group(1)) if match else None


def resolver_url_descarga(candidato: CandidatoLogo, size: int) -> str:
    html_pagina = descargar_texto(candidato.pagina_url)
    category_id = extraer_atributo(html_pagina, "data-category-id")
    logo_id = extraer_atributo(html_pagina, "data-logo-id")
    option = re.search(rf'<option value="{size}::([^"]+)">', html_pagina)
    if category_id and logo_id and option:
        hash_imagen = html.unescape(option.group(1))
        return (
            "https://assets.football-logos.cc/logos/"
            f"{category_id}/{size}x{size}/{logo_id}.{hash_imagen}.png"
        )
    return candidato.imagen_url


class FootballLogosResolver:
    def __init__(self, size: int = 512, logos_dir: Path = LOGOS_DIR):
        self.size = size
        self.logos_dir = logos_dir
        self._candidatos = None

    def _obtener_candidatos(self) -> list[CandidatoLogo]:
        if self._candidatos is None:
            self._candidatos = parsear_sitemap(descargar_texto(SITEMAP_URL))
        return self._candidatos

    def resolver_logo(self, nombre_equipo: str, contexto: str = "") -> str:
        if not nombre_equipo:
            return ""

        salida = self.logos_dir / f"{crear_slug(nombre_equipo)}.png"
        if salida.exists() and salida.stat().st_size > 0:
            return str(salida)

        paises_preferidos = inferir_paises_preferidos(contexto)
        candidatos = buscar_candidatos(nombre_equipo, self._obtener_candidatos(), paises_preferidos)
        if not candidatos:
            return ""

        primero = candidatos[0]
        segundo = candidatos[1] if len(candidatos) > 1 else None
        if segundo and primero.score - segundo.score < 12:
            print(f"⚠️ Logo ambiguo para '{nombre_equipo}', usando fallback")
            return ""

        try:
            imagen_url = resolver_url_descarga(primero, self.size)
            datos = descargar_binario(imagen_url)
        except (HTTPError, URLError, TimeoutError) as e:
            print(f"⚠️ Error descargando logo '{nombre_equipo}': {e}")
            return ""

        if not datos.startswith(b"\x89PNG\r\n\x1a\n"):
            print(f"⚠️ Logo inválido para '{nombre_equipo}'")
            return ""

        salida.parent.mkdir(parents=True, exist_ok=True)
        salida.write_bytes(datos)
        salida.with_suffix(".json").write_text(
            json.dumps(
                {
                    "query": nombre_equipo,
                    "matched_name": primero.nombre,
                    "country": primero.pais,
                    "score": primero.score,
                    "page_url": primero.pagina_url,
                    "image_url": imagen_url,
                    "source": "football-logos.cc",
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return str(salida)

    def eliminar_logo_temporal(self, ruta_logo: str) -> None:
        if not ruta_logo:
            return

        try:
            ruta = Path(ruta_logo).resolve()
            logos_dir = self.logos_dir.resolve()
            ruta.relative_to(logos_dir)
        except (OSError, ValueError):
            return

        for archivo in (ruta, ruta.with_suffix(".json")):
            try:
                if archivo.is_file():
                    archivo.unlink()
            except OSError as e:
                print(f"⚠️ Error borrando logo temporal '{archivo}': {e}")
