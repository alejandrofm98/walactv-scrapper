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


def test_resolves_missing_tmdb_id_from_imdb():
    response = Mock(status_code=200)
    response.json.return_value = {"tv_results": [{"id": 30981}], "movie_results": []}
    http_session = Mock(headers={})
    http_session.get.return_value = response
    scraper = CinemetaMetadataScraper(Mock(), read_token="token", http_session=http_session)

    assert scraper._find_tmdb_id("series", "tt0434706") == 30981
    assert http_session.get.call_args.args[0].endswith("/find/tt0434706")


def test_prefers_spanish_raster_logo():
    response = Mock(status_code=200)
    response.json.return_value = {"logos": [
        {"file_path": "/english.png", "iso_639_1": "en", "vote_average": 10},
        {"file_path": "/spanish.png", "iso_639_1": "es", "vote_average": 2},
        {"file_path": "/spanish.svg", "iso_639_1": "es", "vote_average": 10},
    ]}
    http_session = Mock(headers={})
    http_session.get.return_value = response
    scraper = CinemetaMetadataScraper(Mock(), read_token="token", http_session=http_session)

    assert scraper._fetch_logo("movie", 278) == "https://image.tmdb.org/t/p/w500/spanish.png"
