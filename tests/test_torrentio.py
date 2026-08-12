from unittest.mock import Mock

import pytest

from iptv_scrapper.torrentio import TorrentioClient


def make_response(payload: dict) -> Mock:
    response = Mock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


def test_get_manifest_requests_public_manifest():
    session = Mock()
    session.get.return_value = make_response({"version": "0.0.15", "catalogs": []})
    client = TorrentioClient(session=session)

    manifest = client.get_manifest()

    assert manifest["catalogs"] == []
    session.get.assert_called_once_with("https://torrentio.strem.fun/manifest.json", timeout=15.0)


def test_get_movie_streams_uses_imdb_identifier():
    session = Mock()
    session.get.return_value = make_response(
        {"streams": [{"name": "provider", "url": "https://x"}]}
    )
    client = TorrentioClient(session=session)

    streams = client.get_streams("tt0111161")

    assert streams == [{"name": "provider", "url": "https://x"}]
    session.get.assert_called_once_with(
        "https://torrentio.strem.fun/stream/movie/tt0111161.json", timeout=15.0
    )


def test_get_series_streams_includes_season_and_episode():
    session = Mock()
    session.get.return_value = make_response({"streams": []})
    client = TorrentioClient(session=session)

    client.get_streams("tt0903747", content_type="series", season=1, episode=2)

    session.get.assert_called_once_with(
        "https://torrentio.strem.fun/stream/series/tt0903747:1:2.json", timeout=15.0
    )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"imdb_id": "not-an-imdb-id"}, "imdb_id"),
        ({"imdb_id": "tt1", "content_type": "other"}, "content_type"),
        ({"imdb_id": "tt1", "content_type": "series"}, "requiere"),
    ],
)
def test_get_streams_validates_request(kwargs, message):
    with pytest.raises(ValueError, match=message):
        TorrentioClient(session=Mock()).get_streams(**kwargs)


def test_summarize_streams_does_not_expose_urls():
    summaries = TorrentioClient.summarize_streams(
        [{"name": "provider", "title": "1080p", "url": "https://secret"}]
    )

    assert summaries[0].has_url is True
    assert "secret" not in repr(summaries[0])
