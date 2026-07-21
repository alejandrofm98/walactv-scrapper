import re

NOISE_SUFFIXES = [
    r"\b4k\b",
    r"\buhd\b",
    r"\bhq\b",
    r"\blq\b",
    r"\bcam\b",
    r"\bhdcam\b",
    r"\bsd\b",
    r"\bbluray\b",
    r"\bblu[-\s]?ray\b",
    r"\bweb[-\s]?dl\b",
    r"\bwebdl\b",
    r"\bhdtv\b",
    r"\bdvdrip\b",
    r"\bbdrip\b",
    r"\bhallmark\b",
    r"\bnetflix\b",
    r"\bamazon\b",
    r"\bhbo\b",
    r"\bapple\s*tv\b",
    r"\bmulti[-\s]?sub\b",
    r"\bno\s+sub\b",
    r"\bfrench\s+only\b",
    r"\bfrench\s+quebec\b",
    r"\bquebec\b",
    r"\beng[-\s]?sub\b",
    r"\bwith\s+sub\b",
    r"\bjason\s+statham\b",
    r"\bharvey\s+keitel\b",
    r"\bliam\s+neeson\b",
    r"\bkevin\s+james\b",
    r"\bcillian\s+murphy\b",
    r"\bdavid\s+attenborough\b",
    r"\bfrankenstein\b(?!\s+[a-z])",
    r"\bitalian\s+eng[-\s]?sub\b",
    r"\.mkv\b",
    r"\.mp4\b",
    r"\.avi\b",
    r"\.cd\d+\b",
    r"\.part\d+\b",
]

PREFIX_PATTERN = re.compile(
    r"^(?:LATAM|LAT|MULTI|ES|EN|FR|DE|IT|PT)(?:/(?:LATAM|LAT|MULTI|ES|EN|FR|DE|IT|PT))?\s*[.…\-–]?\s+",
    re.IGNORECASE,
)

SERIES_EPISODE_SUFFIX_PATTERN = re.compile(r"\s+[Ss]\d{1,2}\s*[Ee]\d{1,2}\s*$")
SERIES_SEASON_SUFFIX_PATTERN = re.compile(r"\s+[Ss]\d{1,2}\s*$")


def _remove_noise_suffixes(text: str) -> str:
    for pattern in NOISE_SUFFIXES:
        text = re.sub(pattern, " ", text, flags=re.IGNORECASE)
    return text


def clean_series_name(serie_name: str) -> str:
    if not serie_name:
        return ""
    cleaned = serie_name.strip()
    cleaned = PREFIX_PATTERN.sub("", cleaned)
    cleaned = cleaned.lower()
    year_match = re.search(r"\((\d{4})(?:\s*-\s*\d{4})?\)", cleaned)
    year = year_match.group(1) if year_match else None
    cleaned = re.sub(r"\s*\(\d{4}(?:\s*-\s*\d{4})?\s*\)", "", cleaned)
    cleaned = re.sub(r"[\[\(][^\]\)]*[\]\)]", "", cleaned)
    cleaned = _remove_noise_suffixes(cleaned)
    cleaned = re.sub(r"[^\w\s]", " ", cleaned)
    cleaned = " ".join(cleaned.split())
    if year:
        cleaned = f"{cleaned} {year}"
    return cleaned.strip()


def build_series_key(serie_name: str | None, nombre: str | None = None) -> str | None:
    series_key = clean_series_name(serie_name or "")
    if series_key:
        return series_key
    if not nombre:
        return None
    cleaned = nombre.strip()
    cleaned = PREFIX_PATTERN.sub("", cleaned)
    cleaned = SERIES_EPISODE_SUFFIX_PATTERN.sub("", cleaned)
    cleaned = SERIES_SEASON_SUFFIX_PATTERN.sub("", cleaned)
    cleaned = re.sub(r"[\[\(][^\]\)]*[\]\)]", "", cleaned)
    cleaned = _remove_noise_suffixes(cleaned.lower())
    cleaned = re.sub(r"[^\w\s]", " ", cleaned)
    cleaned = " ".join(cleaned.split())
    return cleaned.strip() or None
