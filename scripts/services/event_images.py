from io import BytesIO
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.request import Request, urlopen

from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageOps


W, H = 800, 450
BASE_DIR = Path(__file__).resolve().parents[1]
PROJECT_DIR = Path(__file__).resolve().parents[2]
if not (PROJECT_DIR / "resources").exists():
    PROJECT_DIR = BASE_DIR
FONT_DIR = Path("/usr/share/fonts/truetype/dejavu")
PLEX_DIR = Path("/usr/share/fonts/truetype/ibm-plex")
IMAGES_DIR = Path(os.getenv("IMAGES_DIR", PROJECT_DIR / "resources" / "images"))
PUBLIC_DOMAIN = os.getenv("PUBLIC_DOMAIN", "").rstrip("/")
IMAGES_BASE_URL = (os.getenv("IMAGES_BASE_URL") or (f"{PUBLIC_DOMAIN}/images" if PUBLIC_DOMAIN else "")).rstrip("/")
EVENTOS_DIR = IMAGES_DIR / "events" / "futbol"
EVENT_IMAGES_RETENTION_DAYS = int(os.getenv("EVENT_IMAGES_RETENTION_DAYS", "3"))
STADIUM_PATH = PROJECT_DIR / "resources" / "event_cards" / "stadium.png"


def crear_slug(texto: str) -> str:
    import re
    import unicodedata

    texto = unicodedata.normalize("NFKD", texto or "")
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    texto = re.sub(r"[^a-z0-9]+", "-", texto.lower()).strip("-")
    return texto or "evento"


def cargar_logo(origen, size=(190, 190)):
    if str(origen).startswith(("http://", "https://")):
        request = Request(str(origen), headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(request, timeout=20) as response:
            logo = Image.open(BytesIO(response.read())).convert("RGBA")
    else:
        logo = Image.open(origen).convert("RGBA")

    logo = ImageOps.contain(logo, size, method=Image.Resampling.LANCZOS)
    lienzo = Image.new("RGBA", size, (0, 0, 0, 0))
    posicion = ((size[0] - logo.width) // 2, (size[1] - logo.height) // 2)
    lienzo.paste(logo, posicion, logo)
    return lienzo


def generar_imagen_evento(
    nombre_local: str,
    nombre_visitante: str,
    logo_local,
    logo_visitante,
    fecha_slug: str,
    hora: str,
    output_dir: Path = EVENTOS_DIR,
    bg_path: Path = STADIUM_PATH,
) -> str:
    salida_dir = output_dir / fecha_slug
    salida = salida_dir / f"{crear_slug(hora)}-{crear_slug(nombre_local)}-vs-{crear_slug(nombre_visitante)}.jpg"
    if salida.exists() and salida.stat().st_size > 0:
        return url_publica_imagen(salida)

    img = Image.new("RGB", (W, H), "#11121a")
    if bg_path and bg_path.exists():
        bg = Image.open(bg_path).convert("RGB")
        bg = ImageOps.fit(bg, (W, H), method=Image.Resampling.LANCZOS, centering=(0.5, 0.58))
        bg = bg.filter(ImageFilter.GaussianBlur(1))
        img.paste(bg)

    img = img.convert("RGBA")
    img = Image.alpha_composite(img, Image.new("RGBA", (W, H), (0, 0, 0, 78)))

    grad = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    grad_px = grad.load()
    for y in range(H):
        vertical_alpha = int((y / H) * 175)
        for x in range(W):
            left_alpha = int(max(0, 1 - x / (W * 0.72)) * 135)
            edge_alpha = int(max(0, (x - W * 0.82) / (W * 0.18)) * 70)
            grad_px[x, y] = (0, 0, 0, min(235, max(vertical_alpha, left_alpha, edge_alpha)))
    img = Image.alpha_composite(img, grad)

    draw = ImageDraw.Draw(img)
    bold_path = PLEX_DIR / "IBMPlexSansCondensed-Bold.ttf"
    if not bold_path.exists():
        bold_path = FONT_DIR / "DejaVuSans-Bold.ttf"
    font_vs = ImageFont.truetype(bold_path, 38)

    logo_size = 190
    home_logo = cargar_logo(logo_local, (logo_size, logo_size))
    away_logo = cargar_logo(logo_visitante, (logo_size, logo_size))

    shadow = Image.new("RGBA", (230, 230), (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow)
    shadow_draw.ellipse((20, 20, 210, 210), fill=(0, 0, 0, 125))
    shadow = shadow.filter(ImageFilter.GaussianBlur(14))

    home_x = W // 2 - 250
    away_x = W // 2 + 60
    logo_y = 58
    img.paste(shadow, (home_x - 20, logo_y - 20), shadow)
    img.paste(shadow, (away_x - 20, logo_y - 20), shadow)
    img.paste(home_logo, (home_x, logo_y), home_logo)
    img.paste(away_logo, (away_x, logo_y), away_logo)
    draw.text(
        (W // 2, logo_y + logo_size // 2),
        "VS",
        font=font_vs,
        fill="#f8fafc",
        anchor="mm",
        stroke_width=2,
        stroke_fill=(0, 0, 0, 170),
    )

    salida_dir.mkdir(parents=True, exist_ok=True)
    img.convert("RGB").save(salida, quality=95)
    return url_publica_imagen(salida)


def url_publica_imagen(ruta: Path) -> str:
    if not IMAGES_BASE_URL:
        return str(ruta)

    try:
        relativa = ruta.resolve().relative_to(IMAGES_DIR.resolve())
    except ValueError:
        return str(ruta)

    return f"{IMAGES_BASE_URL}/{relativa.as_posix()}"


def limpiar_imagenes_eventos(retention_days: int = EVENT_IMAGES_RETENTION_DAYS) -> int:
    """Borra solo imágenes de eventos más antiguas que la ventana indicada."""
    events_dir = IMAGES_DIR / "events"
    if not events_dir.exists():
        return 0

    resolved = events_dir.resolve()
    if resolved.name != "events" or resolved.parent.name != "images":
        print(f"⚠️ Ruta de limpieza no segura, se omite: {resolved}")
        return 0

    cutoff = date.today() - timedelta(days=max(retention_days, 1) - 1)
    borrados = 0

    for fecha_dir in resolved.glob("*/*"):
        if not fecha_dir.is_dir():
            continue
        try:
            fecha = datetime.strptime(fecha_dir.name, "%Y-%m-%d").date()
        except ValueError:
            continue
        if fecha >= cutoff:
            continue

        for archivo in fecha_dir.iterdir():
            if archivo.is_file():
                archivo.unlink()
                borrados += 1
        try:
            fecha_dir.rmdir()
        except OSError:
            pass

    return borrados
