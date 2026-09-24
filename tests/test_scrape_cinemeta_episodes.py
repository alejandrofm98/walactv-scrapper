from unittest.mock import Mock

from iptv_scrapper.scrape_cinemeta_episodes import CinemetaEpisodeScraper


def test_episode_synopsis_prefers_spanish_and_falls_back_to_english():
    scraper = CinemetaEpisodeScraper(session_factory=Mock(), read_token="token")
    scraper._tmdb_season = Mock(
        side_effect=[
            {
                "episodes": [
                    {"episode_number": 1, "name": "Primer episodio", "overview": "Texto español"},
                    {"episode_number": 2, "name": "Segundo episodio", "overview": ""},
                ]
            },
            {
                "episodes": [
                    {"episode_number": 1, "name": "First episode", "overview": "English text"},
                    {"episode_number": 2, "name": "Second episode", "overview": "English fallback"},
                ]
            },
        ]
    )

    rows, had_error = scraper._episode_rows(
        "tt1234567",
        [
            {"id": "tt1234567:1:1", "season": 1, "episode": 1, "name": "First"},
            {"id": "tt1234567:1:2", "season": 1, "episode": 2, "name": "Second"},
        ],
        123,
    )

    assert not had_error
    assert rows[0]["overview_es"] == "Texto español"
    assert rows[0]["overview_en"] == "English text"
    assert rows[1]["overview_es"] is None
    assert rows[1]["overview_en"] == "English fallback"
    assert rows[1]["video_id"] == "tt1234567:1:2"


def test_episode_falls_back_to_cinemeta_when_tmdb_fails():
    scraper = CinemetaEpisodeScraper(session_factory=Mock(), read_token="token")
    scraper._tmdb_season = Mock(side_effect=ValueError("TMDB unavailable"))

    rows, had_error = scraper._episode_rows(
        "tt1234567",
        [
            {
                "id": "tt1234567:1:1",
                "season": 1,
                "episode": 1,
                "name": "First",
                "overview": "Original text",
                "thumbnail": "image",
            }
        ],
        123,
    )

    assert had_error
    assert rows[0]["overview_en"] == "Original text"
    assert rows[0]["overview_es"] is None
    assert rows[0]["thumbnail"] == "image"
