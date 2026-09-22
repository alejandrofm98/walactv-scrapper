from iptv_scrapper.scrape_cinemeta_catalog import CinemetaCatalogScraper, _catalog_variants


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


def test_upsert_generates_the_required_database_uuid():
    class FakeSession:
        statement = ""
        parameters = None

        def execute(self, statement, parameters):
            self.statement = str(statement)
            self.parameters = parameters

    db = FakeSession()
    scraper = CinemetaCatalogScraper(session_factory=None)

    inserted = scraper._upsert_page(
        db,
        "movie",
        "top",
        0,
        [{"id": "tt1234567", "name": "Película", "moviedb_id": 123}],
        {},
    )

    assert inserted == 1
    assert "gen_random_uuid()" in db.statement
    assert db.parameters[0]["imdb_id"] == "tt1234567"
