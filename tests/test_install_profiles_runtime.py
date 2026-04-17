from __future__ import annotations

from dlt_utils.install_profiles import (
    expand_dltaf_requirement,
    infer_dltaf_extras,
    manifest_needs_sqlalchemy_upgrade,
)


def test_expand_dltaf_requirement_for_postgres_sqldb_manifest() -> None:
    manifest = {
        "pipeline": {"destination": "clickhouse"},
        "connections": {
            "source": {"kind": "postgres"},
            "destination": {"kind": "clickhouse"},
        },
        "source": {"kind": "sql_database"},
    }

    expanded = expand_dltaf_requirement("dltaf==0.2.2", manifest)

    assert expanded == "dltaf[clickhouse,sqldb,postgres]==0.2.2"


def test_expand_dltaf_requirement_for_private_clickhouse_vault_manifest() -> None:
    manifest = {
        "connections": {
            "destination": {
                "kind": "clickhouse",
                "vault": "${ENV:CLICKHOUSE__VAULT_REF|company:clickhouse/example}",
            }
        },
        "source": {"kind": "internal_private_rows"},
    }

    expanded = expand_dltaf_requirement("dltaf==0.2.2", manifest)

    assert expanded == "dltaf[runtime]==0.2.2"


def test_expand_dltaf_requirement_merges_existing_extras() -> None:
    manifest = {
        "pipeline": {"destination": "clickhouse"},
        "connections": {"destination": {"kind": "clickhouse"}},
        "source": {"kind": "mongodb"},
    }

    expanded = expand_dltaf_requirement("dltaf[clickhouse]==0.2.2", manifest)

    assert expanded == "dltaf[clickhouse,mongodb]==0.2.2"


def test_infer_dltaf_extras_marks_sql_manifests_for_sqlalchemy_upgrade() -> None:
    manifest = {
        "pipeline": {"destination": "clickhouse"},
        "connections": {"source": {"kind": "oracle"}, "destination": {"kind": "clickhouse"}},
        "source": {"kind": "oracle_custom_sql"},
    }

    extras = infer_dltaf_extras(manifest)

    assert extras == {"clickhouse", "sqldb", "oracle"}
    assert manifest_needs_sqlalchemy_upgrade(manifest) is True
