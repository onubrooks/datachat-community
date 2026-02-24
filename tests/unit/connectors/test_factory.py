"""Unit tests for connector factory helpers."""

from backend.connectors import factory as connector_factory
from backend.connectors.postgres import PostgresConnector


def test_infer_database_type_from_url():
    assert (
        connector_factory.infer_database_type("postgresql://u:p@localhost:5432/app") == "postgresql"
    )
    assert (
        connector_factory.infer_database_type("clickhouse://u:p@localhost:8123/default")
        == "clickhouse"
    )
    assert connector_factory.infer_database_type("mysql://u:p@localhost:3306/app") == "mysql"


def test_resolve_database_type_normalizes_aliases():
    assert (
        connector_factory.resolve_database_type("postgres", "postgresql://u:p@localhost:5432/app")
        == "postgresql"
    )
    assert (
        connector_factory.resolve_database_type("POSTGRESQL", "postgresql://u:p@localhost:5432/app")
        == "postgresql"
    )


def test_create_connector_postgres():
    connector = connector_factory.create_connector(
        database_url="postgresql://u:p@db.example.com:5432/warehouse",
    )
    assert isinstance(connector, PostgresConnector)
    assert connector.host == "db.example.com"
    assert connector.database == "warehouse"


def test_create_connector_clickhouse(monkeypatch):
    captured = {}

    class DummyClickHouse:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(connector_factory, "ClickHouseConnector", DummyClickHouse)
    connector = connector_factory.create_connector(
        database_url="clickhouse://user:pass@click.local:8123/analytics",
    )
    assert isinstance(connector, DummyClickHouse)
    assert captured["host"] == "click.local"
    assert captured["port"] == 8123
    assert captured["database"] == "analytics"


def test_create_connector_mysql(monkeypatch):
    captured = {}

    class DummyMySQL:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(connector_factory, "MySQLConnector", DummyMySQL)
    connector = connector_factory.create_connector(
        database_url="mysql://user:pass@mysql.local:3307/app",
    )
    assert isinstance(connector, DummyMySQL)
    assert captured["host"] == "mysql.local"
    assert captured["port"] == 3307
    assert captured["database"] == "app"


def test_create_connector_invalid_scheme_raises():
    try:
        connector_factory.create_connector(database_url="sqlite:///tmp/test.db")
    except ValueError as exc:
        assert "Invalid" in str(exc) or "Unsupported" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("Expected ValueError for unsupported scheme")
