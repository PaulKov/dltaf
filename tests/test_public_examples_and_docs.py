from __future__ import annotations

from pathlib import Path

from dlt_utils.clickhouse_helpers import get_expected_table_names
from dlt_utils.vault_env import parse_vault_ref
from dltaf.services.manifests.loader import load_manifest
from dltaf.services.manifests.schema import validate_manifest_schema
from dltaf.services.manifests.validator import validate_manifest


def test_public_examples_exist() -> None:
    manifests_dir = Path("dltaf/examples/manifests")
    expected = {
        "smoke_sqldb_catalog.yaml",
        "smoke_sqldb_query.yaml",
        "smoke_mongodb.yaml",
        "smoke_sql_database_catalog.yaml",
        "smoke_oracle_custom_sql.yaml",
        "smoke_mongodb_catalog.yaml",
    }
    assert expected <= {path.name for path in manifests_dir.glob("*.yaml")}


def test_sqldb_expected_tables_supports_canonical_catalog() -> None:
    manifest = {
        "source": {
            "kind": "sqldb",
            "mode": "catalog",
            "catalog": {"schema": "public", "tables": ["orders", "customers"]},
        }
    }
    assert get_expected_table_names(manifest) == ["orders", "customers"]


def test_sqldb_expected_tables_supports_canonical_query() -> None:
    manifest = {
        "source": {
            "kind": "sqldb",
            "mode": "query",
            "query": {"queries": [{"name": "q1", "table_name": "orders_snapshot"}]},
        }
    }
    assert get_expected_table_names(manifest) == ["orders_snapshot"]


def test_public_vault_refs_use_supported_mount_colon_form() -> None:
    ref = parse_vault_ref("company:postgres/example")
    assert ref.mount_point == "company"
    assert ref.path == "postgres/example"


def test_public_vault_refs_support_explicit_ref_and_kv_version_mapping() -> None:
    ref = parse_vault_ref(
        {
            "ref": "company:postgres/example",
            "kv_version": "2",
        }
    )
    assert ref.mount_point == "company"
    assert ref.path == "postgres/example"
    assert ref.kv_version == "2"


def test_manifest_schema_accepts_vault_mapping_contract() -> None:
    validated = validate_manifest_schema(
        {
            "version": 1,
            "pipeline": {
                "name": "dlt__sample__to__clickhouse__raw",
                "destination": "clickhouse",
                "dataset": "raw",
            },
            "connections": {
                "source": {
                    "kind": "postgres",
                    "vault": {
                        "ref": "${ENV:POSTGRES__VAULT_REF|company:postgres/example}",
                        "kv_version": "2",
                    },
                },
                "destination": {
                    "kind": "clickhouse",
                    "vault": {
                        "mount_point": "company",
                        "path": "clickhouse/example",
                        "kv_version": 2,
                    },
                },
            },
            "source": {
                "kind": "sqldb",
                "dialect": "generic",
                "mode": "catalog",
                "catalog": {"schema": "public", "tables": ["orders"]},
            },
        },
        strict=True,
        strict_source=True,
    )
    assert validated.connections is not None


def test_docs_examples_page_mentions_canonical_examples() -> None:
    text = Path("docs/examples.md").read_text(encoding="utf-8")
    assert "smoke_sqldb_catalog.yaml" in text
    assert "smoke_sqldb_query.yaml" in text
    assert "smoke_mongodb.yaml" in text


def test_shipped_example_can_be_linted_with_filename_mismatch_allowed() -> None:
    manifest = load_manifest(Path("dltaf/examples/manifests/smoke_sqldb_catalog.yaml"))
    validate_manifest(manifest, enforce_filename_match=False)
