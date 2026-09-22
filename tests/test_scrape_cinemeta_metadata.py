from unittest.mock import Mock

from iptv_scrapper.scrape_cinemeta_metadata import CinemetaMetadataScraper


def test_fetches_spanish_movie_metadata_with_read_token():
    response = Mock()
    response.status_code = 200
    response.json.return_value = {"title": "Cadena perpetua", "overview": "Una sinopsis"}
    http_session = Mock()
    http_session.headers = {}
    http_session.get.return_value = response

    scraper = CinemetaMetadataScraper(
        session_factory=Mock(),
        read_token="read-token",
        http_session=http_session,
    )

    result = scraper._fetch_spanish("movie", 278)

    assert result == {"title_es": "Cadena perpetua", "overview_es": "Una sinopsis"}
    assert http_session.headers["Authorization"] == "Bearer read-token"
    assert http_session.get.call_args.args[0].endswith("/movie/278")
    assert http_session.get.call_args.kwargs["params"] == {"language": "es-ES"}


def test_fetches_series_metadata_with_api_key_when_read_token_is_absent():
    response = Mock()
    response.status_code = 200
    response.json.return_value = {"name": "Serie", "overview": "Sinopsis"}
    http_session = Mock()
    http_session.headers = {}
    http_session.get.return_value = response

    scraper = CinemetaMetadataScraper(
        session_factory=Mock(),
        api_key="api-key",
        http_session=http_session,
    )

    result = scraper._fetch_spanish("series", 123)

    assert result == {"title_es": "Serie", "overview_es": "Sinopsis"}
    assert http_session.get.call_args.args[0].endswith("/tv/123")
    assert http_session.get.call_args.kwargs["params"] == {
        "language": "es-ES",
        "api_key": "api-key",
    }
