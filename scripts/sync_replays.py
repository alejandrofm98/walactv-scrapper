"""
Scraper de replays UFC desde watch-wrestling.eu
"""
import argparse
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup, Tag

from config import get_settings


class WatchWrestlingUfcScraper:
    """
    Scraper basado en la API pública de WordPress de Watch Wrestling.
    """

    BASE_URL = "https://watch-wrestling.eu"
    API_POSTS_URL = f"{BASE_URL}/wp-json/wp/v2/posts"
    API_CATEGORIES_URL = f"{BASE_URL}/wp-json/wp/v2/categories"
    SOURCE_SITE = "watch-wrestling.eu"
    EMBED_BASE_URL = "https://dailywrestling.cc/embed"
    REQUEST_TIMEOUT = 30
    PRIORITY_GROUP_KEYWORDS = (
        "dailymotion",
    )
    PRIORITY_LABEL_KEYWORDS = (
        "streamw",
        "full show",
        "part 1",
        "part 2",
        "part 3",
        "part 4",
    )
    SKIP_LABEL_KEYWORDS = (
        "voe",
        "netu",
        "abyss",
    )
    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
    }

    # Workers para resolución HTTP en paralelo
    HTTP_BATCH_WORKERS = 8

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(self.DEFAULT_HEADERS)
        self._provider_url_cache: Dict[str, Dict[str, Any]] = {}

    # ──────────────────────────────────────────────────────────────
    # API WordPress
    # ──────────────────────────────────────────────────────────────

    def obtener_categoria_ufc_id(self) -> Optional[int]:
        """Obtiene el ID de la categoria UFC."""
        try:
            response = self.session.get(
                self.API_CATEGORIES_URL,
                params={"search": "UFC", "per_page": 100},
                timeout=self.REQUEST_TIMEOUT,
            )
            response.raise_for_status()

            for categoria in response.json():
                nombre = (categoria.get("name") or "").strip().upper()
                slug = (categoria.get("slug") or "").strip().lower()
                if nombre == "UFC" or slug == "ufc-78" or slug == "ufc":
                    return categoria.get("id")
        except Exception as e:
            print(f"❌ Error obteniendo categoria UFC: {e}")

        return None

    def obtener_posts_ufc(self, limite: Optional[int] = None) -> List[Dict[str, Any]]:
        """Recupera todos los posts UFC paginando sobre la API."""
        categoria_id = self.obtener_categoria_ufc_id()
        if not categoria_id:
            print("❌ No se pudo resolver la categoria UFC en watch-wrestling.eu")
            return []

        pagina = 1
        posts: List[Dict[str, Any]] = []

        while True:
            try:
                response = self.session.get(
                    self.API_POSTS_URL,
                    params={
                        "categories": categoria_id,
                        "per_page": 100,
                        "page": pagina,
                        "orderby": "date",
                        "order": "desc",
                    },
                    timeout=self.REQUEST_TIMEOUT,
                )
                response.raise_for_status()
            except Exception as e:
                print(f"❌ Error obteniendo pagina {pagina} de posts UFC: {e}")
                break

            pagina_posts = response.json()
            if not pagina_posts:
                break

            posts.extend(pagina_posts)

            if limite and len(posts) >= limite:
                return posts[:limite]

            total_paginas = int(response.headers.get("X-WP-TotalPages", pagina))
            if pagina >= total_paginas:
                break

            pagina += 1

        return posts

    # ──────────────────────────────────────────────────────────────
    # Logging
    # ──────────────────────────────────────────────────────────────

    @staticmethod
    def _log_info(message: str) -> None:
        print(f"ℹ️  {message}")

    @staticmethod
    def _log_warning(message: str) -> None:
        print(f"⚠️  {message}")

    # ──────────────────────────────────────────────────────────────
    # Parseo de posts
    # ──────────────────────────────────────────────────────────────

    def parsear_post(self, post: Dict[str, Any]) -> Dict[str, Any]:
        """Normaliza un post UFC para persistirlo en Supabase."""
        content_html = post.get("content", {}).get("rendered", "")
        title = self._html_to_text(post.get("title", {}).get("rendered", ""))
        soup = BeautifulSoup(content_html, "html.parser")
        post_meta = self._obtener_metadata_embed(post.get("link"))

        self._log_info(f"Escaneando evento: {title}")

        descripcion = self._extraer_descripcion(soup)
        match_card = self._extraer_match_card(soup)
        event_date = self._extraer_fecha_evento(title, post)
        event_type = self._detectar_tipo_evento(title)
        sources_data = self._extraer_fuentes_video(
            soup,
            post_url=post.get("link"),
            event_date=event_date,
            category_name=post_meta.get("category_name") or self._normalizar_category_name("UFC"),
            select_post=post_meta.get("select_post") or 1,
        )
        video_sources = sources_data["video_sources"]

        return {
            "slug": post.get("slug"),
            "source_site": self.SOURCE_SITE,
            "title": title,
            "event_name": title,
            "event_type": event_type,
            "event_date": event_date,
            "post_url": post.get("link"),
            "featured_image_url": post.get("jetpack_featured_media_url"),
            "description": descripcion,
            "video_sources": video_sources,
            "source_scan_total": sources_data["total_candidates"],
            "source_scan_resolved": sources_data["resolved_candidates"],
            "source_scan_skipped": sources_data["discarded_candidates"],
            "source_scan_resolved_items": sources_data["resolved_items"],
            "source_scan_skipped_items": sources_data["discarded_items"],
            "match_card": match_card,
        }

    def sincronizar_replays(self, limite: Optional[int] = None) -> int:
        """Scrapea y persiste los replays UFC."""
        posts = self.obtener_posts_ufc(limite=limite)
        if not posts:
            print("⚠️  No se encontraron posts UFC para sincronizar")
            return 0

        replays: List[Dict[str, Any]] = []
        for post in posts:
            replay = self.parsear_post(post)
            replays.append(replay)
            self._log_resumen_evento(replay)

        total_fuentes = sum(replay.get("source_scan_total", 0) for replay in replays)
        total_streams = sum(replay.get("source_scan_resolved", 0) for replay in replays)
        total_descartadas = sum(replay.get("source_scan_skipped", 0) for replay in replays)

        self._log_info(f"Posts UFC encontrados: {len(posts)}")
        self._log_info(f"Replays normalizados: {len(replays)}")
        self._log_info(f"Fuentes detectadas: {total_fuentes}")
        self._log_info(f"Fuentes resueltas: {total_streams}")
        self._log_info(f"Fuentes no recogidas: {total_descartadas}")

        guardados = self._guardar_replays(replays)
        if guardados:
            print(f"✅ Replays guardados/actualizados en Supabase: {guardados}")
        else:
            print("⚠️  No se pudo guardar ningun replay en Supabase")

        return guardados

    def _log_resumen_evento(self, replay: Dict[str, Any]) -> None:
        self._log_info(
            f"Evento: {replay['title']} | resueltas {replay['source_scan_resolved']}/"
            f"{replay['source_scan_total']} | no recogidas {replay['source_scan_skipped']}"
        )
        for item in replay.get("source_scan_resolved_items", []):
            self._log_info(f"  OBTENIDA {item['group']} / {item['label']}")
        for item in replay.get("source_scan_skipped_items", []):
            self._log_info(f"  NO OBTENIDA {item['group']} / {item['label']}")

    @staticmethod
    def _guardar_replays(replays: List[Dict[str, Any]]) -> int:
        if not replays:
            return 0

        payload = []
        for replay in replays:
            replay_copy = dict(replay)
            replay_copy.pop("source_scan_total", None)
            replay_copy.pop("source_scan_resolved", None)
            replay_copy.pop("source_scan_skipped", None)
            replay_copy.pop("source_scan_resolved_items", None)
            replay_copy.pop("source_scan_skipped_items", None)
            replay_copy.pop("source_id", None)
            replay_copy.pop("category", None)
            replay_copy.pop("published_at", None)
            replay_copy.pop("modified_at", None)
            replay_copy.pop("excerpt", None)
            replay_copy.pop("raw_payload", None)
            payload.append(replay_copy)

        try:
            supabase = get_settings().get_supabase_client()
            result = supabase.table("replays").upsert(
                payload,
                on_conflict="slug",
            ).execute()
            return len(result.data or payload)
        except Exception as e:
            print(f"❌ Error guardando replays: {e}")
            return 0

    # ──────────────────────────────────────────────────────────────
    # HTML helpers
    # ──────────────────────────────────────────────────────────────

    @staticmethod
    def _html_to_text(html: str) -> str:
        if not html:
            return ""
        texto = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
        return re.sub(r"\s+", " ", texto).strip()

    def _extraer_descripcion(self, soup: BeautifulSoup) -> str:
        descripcion: List[str] = []

        for parrafo in soup.find_all("p"):
            texto = parrafo.get_text(" ", strip=True)
            texto = re.sub(r"\s+", " ", texto).strip()
            if not texto:
                continue
            if texto.startswith("*"):
                continue
            if texto.lower().startswith("here you can"):
                continue
            if "Report" in texto and "Tell us what happened" in texto:
                continue
            if "Tips to fix the" in texto:
                continue
            descripcion.append(texto)

        return "\n\n".join(descripcion[:6])

    def _extraer_match_card(self, soup: BeautifulSoup) -> List[str]:
        anchor = soup.find(id="card1")
        if not isinstance(anchor, Tag):
            return []

        lista = anchor.find_next("ul")
        if not isinstance(lista, Tag):
            return []

        return [
            re.sub(r"\s+", " ", item.get_text(" ", strip=True)).strip()
            for item in lista.find_all("li")
            if item.get_text(" ", strip=True)
        ]

    # ──────────────────────────────────────────────────────────────
    # Extracción de fuentes de video — punto de entrada principal
    # ──────────────────────────────────────────────────────────────

    def _extraer_fuentes_video(
        self,
        soup: BeautifulSoup,
        post_url: Optional[str],
        event_date: Optional[str],
        category_name: str,
        select_post: int,
    ) -> Dict[str, Any]:
        """
        Pipeline optimizado:
          1. Recopilar todos los candidatos desde el HTML.
          2. Resolver embed URLs en paralelo via HTTP (rápido).
          3. Construir estructura final de grupos/fuentes.
        """
        # ── Paso 1: recopilar candidatos ──────────────────────────
        candidatos: List[Dict[str, Any]] = []
        group_index = 0

        for bloque in soup.select("div.src-name"):
            nombre_grupo = re.sub(r"\s+", " ", bloque.get_text(" ", strip=True)).strip()
            if not nombre_grupo or nombre_grupo.lower() == "quick links!":
                continue

            group_index += 1
            contenedor = self._buscar_srccontainer_siguiente(bloque)
            if not contenedor:
                continue

            for bi, boton in enumerate(contenedor.select("button[data-src]"), start=1):
                label = re.sub(r"\s+", " ", boton.get_text(" ", strip=True)).strip()
                token = boton.get("data-src")
                if not label or not token:
                    continue
                if not self._debe_intentar_fuente(nombre_grupo, label):
                    continue

                embed_url = self._build_embed_url(
                    category_name=category_name,
                    event_date=event_date,
                    select_post=select_post,
                    source_index=group_index,
                    button_index=bi,
                )
                candidatos.append({
                    "group_index": group_index,
                    "button_index": bi,
                    "label": label,
                    "token": token,
                    "token_enc": boton.get("data-enc"),
                    "nombre_grupo": nombre_grupo,
                    "embed_url": embed_url,
                    "post_url": post_url,
                })

        if not candidatos:
            return {
                "video_sources": [],
                "total_candidates": 0,
                "resolved_candidates": 0,
                "discarded_candidates": 0,
                "resolved_items": [],
                "discarded_items": [],
            }

        # ── Paso 2: resolver HTTP en batch (paralelo) ─────────────
        resueltos_http = self._resolver_streams_batch_http(candidatos)

        # ── Paso 3: construir grupos finales ──────────────────────
        grupos_map: Dict[int, Dict[str, Any]] = {}

        descartadas = 0
        resolved_items: List[Dict[str, str]] = []
        discarded_items: List[Dict[str, str]] = []

        for c in candidatos:
            cache_key = f"{c['group_index']}:{c['button_index']}"
            stream = resueltos_http.get(cache_key) or {}

            if not stream.get("stream_url"):
                descartadas += 1
                discarded_items.append({
                    "group": c["nombre_grupo"],
                    "label": c["label"],
                })
                continue

            gi = c["group_index"]
            if gi not in grupos_map:
                grupos_map[gi] = {"group": c["nombre_grupo"], "sources": []}

            grupos_map[gi]["sources"].append({
                "label": c["label"],
                "web_embed_url": self._seleccionar_embed_para_web(
                    c["embed_url"],
                    stream.get("provider_url"),
                ),
                **{k: stream.get(k) for k in (
                    "stream_url",
                    "stream_format",
                )},
            })
            resolved_items.append({
                "group": c["nombre_grupo"],
                "label": c["label"],
            })

        grupos = [g for g in grupos_map.values() if g["sources"]]
        resolved_candidates = sum(len(group["sources"]) for group in grupos)
        return {
            "video_sources": grupos,
            "total_candidates": len(candidatos),
            "resolved_candidates": resolved_candidates,
            "discarded_candidates": descartadas,
            "resolved_items": resolved_items,
            "discarded_items": discarded_items,
        }

    # ──────────────────────────────────────────────────────────────
    # Resolución HTTP en batch (paralelo)
    # ──────────────────────────────────────────────────────────────

    def _resolver_streams_batch_http(
        self,
        candidatos: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Resuelve todas las embed URLs en paralelo usando un ThreadPoolExecutor.
        Retorna {cache_key: stream_data} para los que tienen stream_url.
        """
        resultados: Dict[str, Dict[str, Any]] = {}

        def resolver_uno(candidato: Dict[str, Any]):
            embed_url = candidato.get("embed_url")
            cache_key = f"{candidato['group_index']}:{candidato['button_index']}"
            if not embed_url:
                return cache_key, {}

            # Comprobar cache global primero
            if embed_url in self._provider_url_cache:
                return cache_key, dict(self._provider_url_cache[embed_url])

            provider_url = self._resolver_provider_url(embed_url)
            stream_data: Dict[str, Any] = {}
            if provider_url:
                stream_data = self._resolver_provider_stream(provider_url)

            if (not stream_data.get("stream_url")) and self._es_grupo_dailymotion(
                candidato.get("nombre_grupo", "")
            ):
                provider_url = self._resolver_provider_url_desde_token(
                    token=candidato.get("token"),
                    token_enc=candidato.get("token_enc"),
                    referer_url=candidato.get("post_url"),
                )
                if provider_url:
                    stream_data = self._resolver_provider_stream(provider_url)

            if stream_data.get("stream_url"):
                self._provider_url_cache[embed_url] = stream_data
                return cache_key, stream_data

            return cache_key, {}

        with ThreadPoolExecutor(max_workers=self.HTTP_BATCH_WORKERS) as pool:
            futuros = {pool.submit(resolver_uno, c): c for c in candidatos}
            for futuro in as_completed(futuros):
                try:
                    cache_key, stream = futuro.result()
                    if stream.get("stream_url"):
                        resultados[cache_key] = stream
                except Exception as e:
                    self._log_warning(f"Error en HTTP batch worker: {e}")

        return resultados

    @staticmethod
    def _es_grupo_dailymotion(group_name: str) -> bool:
        return "dailymotion" in (group_name or "").lower()

    # ──────────────────────────────────────────────────────────────
    # Resolución de streams — métodos de soporte
    # ──────────────────────────────────────────────────────────────

    def _obtener_metadata_embed(self, post_url: Optional[str]) -> Dict[str, Any]:
        """Recupera metadata del post necesaria para construir URLs embebibles."""
        metadata: Dict[str, Any] = {
            "category_name": self._normalizar_category_name("UFC"),
            "select_post": 1,
        }

        if not post_url:
            return metadata

        try:
            response = self.session.get(post_url, timeout=self.REQUEST_TIMEOUT)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            crumbs = soup.select_one(".entry-crumbs")
            if not isinstance(crumbs, Tag):
                return metadata

            secondary_cat = crumbs.get("data-secondary-catname")
            select_post = crumbs.get("data-select-post")

            if isinstance(secondary_cat, str) and secondary_cat.strip():
                metadata["category_name"] = self._normalizar_category_name(secondary_cat)

            if isinstance(select_post, str) and select_post.isdigit():
                metadata["select_post"] = int(select_post)
        except Exception as e:
            print(f"⚠️  No se pudo obtener metadata embed de {post_url}: {e}")

        return metadata

    def _debe_intentar_fuente(self, group_name: str, label: str) -> bool:
        group_lower = group_name.lower()
        label_lower = label.lower()

        if any(keyword in label_lower for keyword in self.SKIP_LABEL_KEYWORDS):
            return False

        return any(keyword in group_lower for keyword in self.PRIORITY_GROUP_KEYWORDS)

    def _resolver_provider_url(self, embed_url: str) -> Optional[str]:
        resolver_path = Path(__file__).resolve().parent / "utils" / "replay_embed_resolver.js"
        if not resolver_path.exists():
            self._log_warning(f"No existe el resolvedor JS: {resolver_path}")
            return None

        try:
            result = subprocess.run(
                ["node", str(resolver_path), embed_url],
                capture_output=True,
                text=True,
                timeout=45,
                check=True,
            )
        except subprocess.TimeoutExpired:
            self._log_warning(f"Timeout resolviendo provider URL para {embed_url}")
            return None
        except subprocess.CalledProcessError as e:
            stderr = (e.stderr or "").strip()
            if stderr and "No se encontraron scripts inline en el embed" not in stderr:
                self._log_warning(f"Error resolviendo {embed_url}: {stderr}")
            return None
        except Exception as e:
            self._log_warning(f"Error ejecutando resolvedor JS para {embed_url}: {e}")
            return None

        salida = (result.stdout or "").strip()
        if not salida:
            if result.stderr:
                self._log_warning(
                    f"Resolvedor JS sin salida para {embed_url}: {result.stderr.strip()}"
                )
            return None

        return salida

    def _resolver_provider_url_desde_token(
        self,
        token: Optional[str],
        token_enc: Optional[str],
        referer_url: Optional[str],
    ) -> Optional[str]:
        if not token or not token_enc:
            return None

        resolver_path = Path(__file__).resolve().parent / "utils" / "replay_token_resolver.js"
        if not resolver_path.exists():
            self._log_warning(f"No existe el resolvedor JS por token: {resolver_path}")
            return None

        try:
            result = subprocess.run(
                [
                    "node",
                    str(resolver_path),
                    token,
                    token_enc,
                    referer_url or self.BASE_URL,
                ],
                capture_output=True,
                text=True,
                timeout=45,
                check=True,
            )
        except subprocess.TimeoutExpired:
            self._log_warning("Timeout resolviendo provider URL via token")
            return None
        except subprocess.CalledProcessError as e:
            stderr = (e.stderr or "").strip()
            if stderr:
                self._log_warning(f"Error resolviendo provider por token: {stderr}")
            return None
        except Exception as e:
            self._log_warning(f"Error ejecutando resolvedor JS por token: {e}")
            return None

        salida = (result.stdout or "").strip()
        return salida or None

    def _resolver_provider_stream(self, provider_url: str) -> Dict[str, Any]:
        stream_data: Dict[str, Any] = {"provider_url": provider_url}

        if "dailymotion.com/embed/video/" in provider_url:
            stream_data.update(self._resolver_dailymotion_stream(provider_url))
            return stream_data

        if provider_url.endswith(".m3u8"):
            stream_data.update({
                "provider": self._detectar_provider_desde_url(provider_url),
                "stream_url": provider_url,
                "stream_format": "application/x-mpegURL",
            })
            return stream_data

        if provider_url.endswith(".mp4"):
            stream_data.update({
                "provider": self._detectar_provider_desde_url(provider_url),
                "stream_url": provider_url,
                "stream_format": "video/mp4",
            })
            return stream_data

        return stream_data

    def _seleccionar_embed_para_web(
        self,
        embed_url: Optional[str],
        provider_url: Optional[str],
    ) -> Optional[str]:
        """Elige la URL embebible más estable para la web."""
        if provider_url and self._es_embed_web_usable(provider_url):
            return provider_url
        return embed_url

    @staticmethod
    def _es_embed_web_usable(url: str) -> bool:
        lowered = (url or "").lower()
        return any(marker in lowered for marker in (
            "dailymotion.com/embed/video/",
            "geo.dailymotion.com/player.html",
            "ok.ru/videoembed/",
            "ok.ru/video/",
        ))

    # ──────────────────────────────────────────────────────────────
    # Dailymotion
    # ──────────────────────────────────────────────────────────────

    def _resolver_dailymotion_stream(self, provider_url: str) -> Dict[str, Any]:
        parsed = urlparse(provider_url)
        path_parts = [part for part in parsed.path.split("/") if part]
        access_id = path_parts[-1] if path_parts else None

        data: Dict[str, Any] = {
            "provider": "dailymotion",
        }

        if not access_id:
            return data

        metadata = self._obtener_dailymotion_metadata(access_id)
        if not metadata:
            return data

        quality_sources = metadata.get("qualities") or {}
        selected_quality, source = self._seleccionar_mejor_stream_dailymotion(quality_sources)
        if source:
            data["stream_url"] = source.get("url")
            data["stream_format"] = source.get("type")

        available_qualities = [key for key, value in quality_sources.items() if value]

        return data

    @staticmethod
    def _seleccionar_mejor_stream_dailymotion(
        quality_sources: Dict[str, List[Dict[str, Any]]]
    ) -> tuple[Optional[str], Optional[Dict[str, Any]]]:
        if not quality_sources:
            return None, None

        numeric_qualities = []
        for key, sources in quality_sources.items():
            if not sources:
                continue
            if str(key).isdigit():
                numeric_qualities.append((int(str(key)), str(key), sources[0]))

        if numeric_qualities:
            numeric_qualities.sort(key=lambda item: item[0], reverse=True)
            _, label, source = numeric_qualities[0]
            return label, source

        auto_sources = quality_sources.get("auto") or []
        if auto_sources:
            return "auto", auto_sources[0]

        for label, sources in quality_sources.items():
            if sources:
                return str(label), sources[0]

        return None, None

    def _obtener_dailymotion_metadata(self, access_id: str) -> Dict[str, Any]:
        metadata_url = f"https://www.dailymotion.com/player/metadata/video/{access_id}"
        params = {"embedder": "https://dailywrestling.cc/"}

        try:
            response = self.session.get(
                metadata_url,
                params=params,
                timeout=self.REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self._log_warning(f"No se pudo obtener metadata Dailymotion de {access_id}: {e}")
            return {}

    # ──────────────────────────────────────────────────────────────
    # Utilidades estáticas
    # ──────────────────────────────────────────────────────────────

    @staticmethod
    def _detectar_provider_desde_url(url: str) -> str:
        lowered = url.lower()
        if "dailymotion" in lowered or "dmcdn" in lowered:
            return "dailymotion"
        if "abyss" in lowered:
            return "abyss"
        if "streamw" in lowered:
            return "streamw"
        if "voe" in lowered:
            return "voe"
        if "ok.ru" in lowered:
            return "okru"
        if "netu" in lowered:
            return "netu"
        return urlparse(url).netloc or "unknown"

    @staticmethod
    def _normalizar_category_name(category_name: str) -> str:
        limpio = (category_name or "").strip().lower().replace(" ", "")
        return limpio[::-1]

    def _build_embed_url(
        self,
        category_name: str,
        event_date: Optional[str],
        select_post: int,
        source_index: int,
        button_index: int,
    ) -> Optional[str]:
        if not event_date:
            return None

        try:
            fecha = datetime.fromisoformat(event_date).strftime("%m-%d-%Y")
        except ValueError:
            return None

        return (
            f"{self.EMBED_BASE_URL}/{category_name}/{fecha}/select-post-{select_post}/"
            f"{source_index}/{button_index}"
        )

    @staticmethod
    def _buscar_srccontainer_siguiente(bloque: Tag) -> Optional[Tag]:
        sibling = bloque.find_next_sibling()
        while sibling:
            if isinstance(sibling, Tag) and sibling.name == "div":
                sibling_classes = sibling.get("class")
                clases = [str(clase) for clase in sibling_classes] if sibling_classes else []
                if "srccontainer" in clases:
                    return sibling
                if "src-name" in clases:
                    return None
            sibling = sibling.find_next_sibling()
        return None

    @staticmethod
    def _detectar_tipo_evento(title: str) -> str:
        title_upper = title.upper()
        if "FIGHT NIGHT" in title_upper:
            return "fight_night"
        if re.search(r"\bUFC\s+\d+\b", title_upper):
            return "numbered"
        return "other"

    def _extraer_fecha_evento(self, title: str, post: Dict[str, Any]) -> Optional[str]:
        match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", title)
        if match:
            mes, dia, year = match.groups()
            year_int = int(year)
            if year_int < 100:
                year_int += 2000

            try:
                return datetime(year_int, int(mes), int(dia)).date().isoformat()
            except ValueError:
                pass

        publicado = post.get("date_gmt") or post.get("date")
        normalizado = self._normalizar_datetime(publicado)
        if not normalizado:
            return None

        return normalizado[:10]

    @staticmethod
    def _normalizar_datetime(value: Optional[str]) -> Optional[str]:
        if not value:
            return None

        try:
            return datetime.fromisoformat(value).isoformat()
        except ValueError:
            return value


# ──────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sincroniza replays UFC desde watch-wrestling.eu")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limita el numero de posts UFC a procesar",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    scraper = WatchWrestlingUfcScraper()
    try:
        guardados = scraper.sincronizar_replays(limite=args.limit)
        return 0 if guardados >= 0 else 1
    except Exception as e:
        print(f"❌ Error sincronizando replays UFC: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
