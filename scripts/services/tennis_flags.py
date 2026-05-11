import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


USER_AGENT = "Mozilla/5.0 (compatible; walactv-scrapper/1.0)"
BASE_DIR = Path(__file__).resolve().parents[1]
PROJECT_DIR = Path(__file__).resolve().parents[2]
if not (PROJECT_DIR / "resources").exists():
    PROJECT_DIR = BASE_DIR
FLAGS_DIR = PROJECT_DIR / "resources" / "flags" / "flagcdn"


PAISES_ISO = {
    "alemania": "de",
    "argelia": "dz",
    "argentina": "ar",
    "armenia": "am",
    "australia": "au",
    "austria": "at",
    "belgica": "be",
    "bielorrusia": "by",
    "bolivia": "bo",
    "bosnia": "ba",
    "brasil": "br",
    "bulgaria": "bg",
    "canada": "ca",
    "chile": "cl",
    "china": "cn",
    "chipre": "cy",
    "colombia": "co",
    "corea del sur": "kr",
    "croacia": "hr",
    "dinamarca": "dk",
    "ecuador": "ec",
    "egipto": "eg",
    "eslovaquia": "sk",
    "eslovenia": "si",
    "espana": "es",
    "estados unidos": "us",
    "estonia": "ee",
    "finlandia": "fi",
    "francia": "fr",
    "georgia": "ge",
    "gran bretana": "gb",
    "grecia": "gr",
    "holanda": "nl",
    "hungria": "hu",
    "india": "in",
    "irlanda": "ie",
    "israel": "il",
    "italia": "it",
    "japon": "jp",
    "kazajistan": "kz",
    "letonia": "lv",
    "lituania": "lt",
    "mexico": "mx",
    "montenegro": "me",
    "noruega": "no",
    "nueva zelanda": "nz",
    "paises bajos": "nl",
    "peru": "pe",
    "polonia": "pl",
    "portugal": "pt",
    "reino unido": "gb",
    "republica checa": "cz",
    "rumania": "ro",
    "rusia": "ru",
    "serbia": "rs",
    "sudafrica": "za",
    "suecia": "se",
    "suiza": "ch",
    "tunez": "tn",
    "turquia": "tr",
    "ucrania": "ua",
    "uruguay": "uy",
}


@dataclass(frozen=True)
class BanderaTenis:
    pais: str
    iso2: str
    ruta: str


def normalizar_texto(texto: str) -> str:
    texto = unicodedata.normalize("NFKD", texto or "")
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    texto = texto.lower().replace("-", " ").replace("_", " ")
    texto = re.sub(r"[^a-z0-9]+", " ", texto)
    return re.sub(r"\s+", " ", texto).strip()


def extraer_pais_desde_url(url: str) -> str:
    if not url:
        return ""

    nombre_archivo = Path(urlparse(url).path).stem
    match = re.search(r"\d+-(.+)$", nombre_archivo)
    pais = match.group(1) if match else nombre_archivo
    return normalizar_texto(pais)


def descargar_binario(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=30) as response:
        return response.read()


class TennisFlagsResolver:
    def __init__(self, flags_dir: Path = FLAGS_DIR, size: int = 640):
        self.flags_dir = flags_dir
        self.size = size

    def resolver_bandera(self, origen_futbolenlatv: str) -> str:
        pais = extraer_pais_desde_url(origen_futbolenlatv)
        iso2 = PAISES_ISO.get(pais)
        if not iso2:
            return ""

        salida = self.flags_dir / f"{iso2}.png"
        if salida.exists() and salida.stat().st_size > 0:
            return str(salida)

        url = f"https://flagcdn.com/w{self.size}/{iso2}.png"
        try:
            datos = descargar_binario(url)
        except (HTTPError, URLError, TimeoutError) as e:
            print(f"⚠️ Error descargando bandera '{pais}' ({iso2}): {e}")
            return ""

        if not datos.startswith(b"\x89PNG\r\n\x1a\n"):
            print(f"⚠️ Bandera inválida para '{pais}' ({iso2})")
            return ""

        salida.parent.mkdir(parents=True, exist_ok=True)
        salida.write_bytes(datos)
        salida.with_suffix(".txt").write_text(
            f"source=https://flagcdn.com\niso2={iso2}\npais={pais}\nupdated_at={datetime.now().isoformat(timespec='seconds')}\n",
            encoding="utf-8",
        )
        return str(salida)
