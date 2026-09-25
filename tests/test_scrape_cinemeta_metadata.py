from unittest.mock import MagicMock, Mock

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


def test_can_select_a_single_title_for_backfill():
    session_factory = MagicMock()
    db = session_factory.return_value.__enter__.return_value
    db.execute.return_value.mappings.return_value.all.return_value = []
    scraper = CinemetaMetadataScraper(session_factory, read_token="token")

    assert scraper.run(imdb_id="tt0434706") == (0, 0, 0)
    assert db.execute.call_args.args[1]["imdb_id"] == "tt0434706"


def test_searches_synopsis_backup_by_title_and_year_without_using_canonical_id():
    response = Mock(status_code=200)
    response.json.return_value = {
        "results": [
            {
                "id": 336273,
                "name": "Monster",
                "original_name": "Monster",
                "first_air_date": "2022-10-02",
            },
            {
                "id": 113988,
                "name": "Monstruo: La historia de Jeffrey Dahmer",
                "original_name": "DAHMER - Monster: The Jeffrey Dahmer Story",
                "first_air_date": "2022-09-21",
            },
            {
                "id": 999,
                "name": "Monster",
                "original_name": "Monster",
                "first_air_date": "2019-01-01",
            },
        ]
    }
    http_session = Mock(headers={})
    http_session.get.return_value = response
    scraper = CinemetaMetadataScraper(Mock(), read_token="token", http_session=http_session)
    scraper._fetch_spanish = Mock(
        return_value={"title_es": "No cambiar", "overview_es": "Sinopsis española"}
    )

    result = scraper._search_spanish_synopsis_backup("series", "Monster", 2022, 336273)

    assert result == "Sinopsis española"
    assert http_session.get.call_args.args[0].endswith("/search/tv")
    assert http_session.get.call_args.kwargs["params"] == {
        "query": "Monster",
        "first_air_date_year": "2022",
        "language": "es-ES",
    }
    scraper._fetch_spanish.assert_called_once_with("series", 113988)


def test_uses_backup_synopsis_but_keeps_canonical_tmdb_id():
    session_factory = MagicMock()
    db = session_factory.return_value.__enter__.return_value
    db.execute.return_value.mappings.return_value.all.return_value = [
        {
            "content_type": "series",
            "imdb_id": "tt13207736",
            "moviedb_id": 336273,
            "title": "Monster",
            "year": 2022,
            "has_overview_es": False,
            "existing_logo": "https://image.tmdb.org/t/p/w500/logo.png",
        }
    ]
    scraper = CinemetaMetadataScraper(session_factory, read_token="token")
    scraper._fetch_spanish = Mock(return_value={"title_es": None, "overview_es": None})
    scraper._search_spanish_synopsis_backup = Mock(return_value="Sinopsis en español")

    assert scraper.run(imdb_id="tt13207736") == (1, 1, 0)

    update_params = db.execute.call_args_list[1].args[1]
    assert update_params["overview_es"] == "Sinopsis en español"
    assert update_params["title_es"] is None
    assert update_params["moviedb_id"] == 336273
    scraper._search_spanish_synopsis_backup.assert_called_once_with(
        "series", "Monster", 2022, 336273
    )
