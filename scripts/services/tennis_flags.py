import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from PIL import Image

USER_AGENT = "Mozilla/5.0 (compatible; walactv-scrapper/1.0)"
BASE_DIR = Path(__file__).resolve().parents[1]
PROJECT_DIR = Path(__file__).resolve().parents[2]
if not (PROJECT_DIR / "resources").exists():
    PROJECT_DIR = BASE_DIR
FLAGS_DIR = PROJECT_DIR / "resources" / "flags" / "flagcdn"
ALIASES_PATH = PROJECT_DIR / "resources" / "tennis_flag_aliases.json"


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
    "eeuu": "us",
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
    "norway": "no",
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


COUNTRY_WORD_MAP = {
    "australian": "australia",
    "belgian": "belgica",
    "british": "reino unido",
    "czech": "republica checa",
    "dutch": "paises bajos",
    "french": "francia",
    "german": "alemania",
    "italian": "italia",
    "russian": "rusia",
    "spanish": "espana",
    "swiss": "suiza",
    "american": "eeuu",
}


def extraer_pais_desde_url(url: str) -> str:
    if not url:
        return ""

    nombre_archivo = Path(urlparse(url).path).stem
    match = re.search(r"^\d+[-_](.+)$", nombre_archivo)
    pais = match.group(1) if match else nombre_archivo
    pais = normalizar_texto(pais)

    if pais not in PAISES_ISO:
        for word in pais.split():
            if word in COUNTRY_WORD_MAP:
                pais = COUNTRY_WORD_MAP[word]
                break

    return pais


def descargar_binario(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=30) as response:
        return response.read()


def fuente_desde_url(url: str) -> str:
    return urlparse(url).netloc or "desconocido"


def cargar_aliases() -> dict[str, str]:
    if not ALIASES_PATH.exists():
        return {}

    try:
        with ALIASES_PATH.open("r", encoding="utf-8") as archivo:
            aliases = json.load(archivo)
            print(f"ℹ️ Aliases de banderas tenis cargados: {len(aliases)}")
            return aliases
    except Exception as e:
        print(f"⚠️ Error leyendo aliases de banderas tenis: {e}")
        return {}


class TennisFlagsResolver:
    def __init__(self, flags_dir: Path = FLAGS_DIR, size: int = 1280):
        self.flags_dir = flags_dir
        self.size = size
        self.aliases = cargar_aliases()

    def _cache_valida(self, ruta: Path) -> bool:
        if not ruta.exists() or ruta.stat().st_size <= 0:
            return False

        try:
            with Image.open(ruta) as imagen:
                return imagen.width >= self.size
        except Exception:
            return False

    def resolver_bandera(self, origen_futbolenlatv: str, nombre_jugador: str = "") -> str:
        pais = extraer_pais_desde_url(origen_futbolenlatv)
        iso2 = PAISES_ISO.get(pais) or self.aliases.get(pais)
        if not iso2:
            jugador = f" jugador='{nombre_jugador}'" if nombre_jugador else ""
            print(
                f"⚠️ País no reconocido para bandera tenis:{jugador} "
                f"pais='{pais}' origen={origen_futbolenlatv}"
            )
            return ""

        salida = self.flags_dir / f"{iso2}.png"
        if self._cache_valida(salida):
            return str(salida)

        url = f"https://flagcdn.com/w{self.size}/{iso2}.png"
        try:
            datos = descargar_binario(url)
        except (HTTPError, URLError, TimeoutError) as e:
            jugador = f" jugador='{nombre_jugador}'" if nombre_jugador else ""
            print(
                f"⚠️ Error descargando bandera desde {fuente_desde_url(url)} "
                f"para '{pais}' ({iso2}){jugador}: {e} url={url}"
            )
            return ""

        if not datos.startswith(b"\x89PNG\r\n\x1a\n"):
            jugador = f" jugador='{nombre_jugador}'" if nombre_jugador else ""
            print(
                f"⚠️ Bandera inválida desde {fuente_desde_url(url)} "
                f"para '{pais}' ({iso2}){jugador}: url={url}"
            )
            return ""

        salida.parent.mkdir(parents=True, exist_ok=True)
        salida.write_bytes(datos)
        salida.with_suffix(".txt").write_text(
            f"source=https://flagcdn.com\niso2={iso2}\npais={pais}\nupdated_at={datetime.now().isoformat(timespec='seconds')}\n",
            encoding="utf-8",
        )
        return str(salida)
