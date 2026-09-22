from iptv_scrapper.scrape_cinemeta_catalog import _catalog_variants


def test_expands_paginable_movie_and_series_catalogs_and_required_genres():
    manifest = {
        "catalogs": [
            {
                "type": "movie",
                "id": "top",
                "genres": ["Action", "Sci-Fi"],
                "extraSupported": ["genre", "skip"],
            },
            {
                "type": "series",
                "id": "year",
                "genres": ["2025", "2024"],
                "extra": [{"name": "genre", "isRequired": True}],
                "extraSupported": ["genre", "skip"],
            },
            {"type": "series", "id": "calendar-videos", "extraSupported": []},
            {"type": "channel", "id": "official", "extraSupported": ["skip"]},
        ]
    }

    assert list(_catalog_variants(manifest)) == [
        ("movie", "top", "top", None),
        ("movie", "top", "top_Action", "Action"),
        ("movie", "top", "top_Sci-Fi", "Sci-Fi"),
        ("series", "year", "year_2025", "2025"),
        ("series", "year", "year_2024", "2024"),
    ]
