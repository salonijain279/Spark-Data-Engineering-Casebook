from pathlib import Path


def test_pipeline_entrypoints_are_present() -> None:
    root = Path(__file__).parents[1] / "src"
    expected = {
        "yelp_medallion.py",
        "wikipedia_clickstream.py",
        "flight_route_metrics.py",
    }
    assert expected == {path.name for path in root.glob("*.py")}


def test_no_databricks_only_magics_in_portable_sources() -> None:
    root = Path(__file__).parents[1] / "src"
    source = "\n".join(path.read_text() for path in root.glob("*.py"))
    assert "%sql" not in source
    assert "dbutils" not in source

