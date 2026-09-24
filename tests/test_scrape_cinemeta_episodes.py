from unittest.mock import MagicMock, Mock

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
            {"episodes": [{"episode_number": 2, "name": "Segundo episodio", "overview": "Texto mexicano"}]},
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
    assert rows[1]["overview_es"] == "Texto mexicano"
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


def test_first_run_requeues_previously_synced_episodes_missing_spanish():
    session_factory = MagicMock()
    db = session_factory.return_value.__enter__.return_value
    db.execute.return_value.scalar.return_value = None
    db.execute.return_value.mappings.return_value.all.return_value = []
    scraper = CinemetaEpisodeScraper(session_factory=session_factory, read_token="token")

    assert scraper.run() == (0, 0, 0)
    queries = [str(call.args[0]) for call in db.execute.call_args_list]
    assert any("SET episodes_checked_at = NULL" in query for query in queries)
    assert any("cinemeta_episodes_es_v2" in query for query in queries)
    assert any("MAX(episodes_synced_at) IS NOT NULL" in query for query in queries)
    db.commit.assert_called_once()
