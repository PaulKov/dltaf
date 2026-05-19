from __future__ import annotations

from pathlib import Path

try:
    import tomllib
except ImportError:  # pragma: no cover - Python < 3.11 local fallback
    try:
        import tomli as tomllib
    except ImportError:  # pragma: no cover - local bootstrap fallback
        from pip._vendor import tomli as tomllib


def _load_pyproject() -> dict:
    with Path("pyproject.toml").open("rb") as fh:
        return tomllib.load(fh)


def test_core_dependencies_stay_runtime_light() -> None:
    project = _load_pyproject()["project"]
    dependencies = set(project["dependencies"])

    assert dependencies == {
        "dlt>=1.18.2,<2",
        "PyYAML>=6.0.1",
        "pydantic>=2.8,<3",
        "prettytable>=3.12,<4",
    }


def test_optional_dependency_profiles_cover_public_runtime_scenarios() -> None:
    optional = _load_pyproject()["project"]["optional-dependencies"]

    assert optional["clickhouse"] == ["dlt[clickhouse]>=1.18.2,<2"]
    assert optional["sqldb"] == [
        "dlt[sql_database]>=1.18.2,<2",
        "sqlalchemy>=2.0.25",
    ]
    assert optional["postgres"] == ["psycopg2-binary>=2.9.9"]
    assert optional["oracle"] == [
        "oracledb>=2.0.0",
        "sqlalchemy>=2.0.25",
    ]
    assert optional["mongodb"] == ["pymongo>=4.6.0"]
    assert optional["kafka"] == ["kafka-python>=2.3.0,<3"]
    assert optional["vault"] == ["vault-kv-client>=0.1.0"]
    assert optional["runtime"] == [
        "vault-kv-client>=0.1.0",
        "dlt[clickhouse]>=1.18.2,<2",
    ]


def test_installation_profiles_docs_match_supported_profiles() -> None:
    text = Path("docs/installation-profiles.md").read_text(encoding="utf-8")

    assert 'pip install "dltaf[runtime]"' in text
    assert 'pip install "dltaf[clickhouse,sqldb,postgres]"' in text
    assert 'pip install "dltaf[clickhouse,sqldb,oracle]"' in text
    assert 'pip install "dltaf[clickhouse,mongodb]"' in text
    assert 'pip install "dltaf[runtime,kafka]"' in text
