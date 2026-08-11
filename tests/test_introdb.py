from unittest.mock import Mock

import pytest

from iptv_scrapper.introdb import IntroDbClient, parse_segments


def test_parse_segments_ignores_missing_and_invalid_segments():
    segments = parse_segments(
        {
            "intro": {"start_ms": 1000, "end_ms": 5000, "confidence": 0.9},
            "recap": None,
            "outro": {"start_ms": 0, "end_ms": 0},
        }
    )

    assert len(segments) == 1
    assert segments[0].segment_type == "intro"
    assert segments[0].end_ms == 5000


def test_client_returns_empty_for_episode_without_data():
    session = Mock()
    session.get.return_value.status_code = 404
    client = IntroDbClient(session=session)

    assert client.get_segments("tt0903747", 1, 1) == []


def test_client_retries_rate_limit_and_parses_response(monkeypatch):
    first = Mock(status_code=429, headers={"Retry-After": "0"})
    second = Mock(
        status_code=200,
        headers={},
        json=lambda: {
            "intro": {
                "start_ms": 1000,
                "end_ms": 5000,
                "confidence": 1,
                "submission_count": 2,
            }
        },
    )
    session = Mock()
    session.get.side_effect = [first, second]
    monkeypatch.setattr("iptv_scrapper.introdb.time.sleep", lambda _: None)
    client = IntroDbClient(session=session, max_retries=1)

    result = client.get_segments("tt0903747", 1, 1)

    assert len(result) == 1
    assert result[0].start_ms == 1000
    assert session.get.call_count == 2


@pytest.mark.parametrize("imdb_id,season,episode", [("bad", 1, 1), ("tt1234567", 0, 1)])
def test_client_rejects_invalid_identifiers(imdb_id, season, episode):
    with pytest.raises(ValueError):
        IntroDbClient().get_segments(imdb_id, season, episode)
