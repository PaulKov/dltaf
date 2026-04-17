from __future__ import annotations

import sys
from types import SimpleNamespace
from types import ModuleType

from dltaf.integrations.mongodb.workflow import MongoDBWorkflow


def test_mongodb_workflow_uses_public_runtime_module(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_mongodb(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(kind="mongodb")

    module = ModuleType("dlt_pipelines.mongodb_runtime.mongodb")
    module.mongodb = fake_mongodb
    monkeypatch.setitem(sys.modules, "dlt_pipelines.mongodb_runtime.mongodb", module)

    config = type(
        "Cfg",
        (),
        {
            "connection_url": "mongodb://localhost:27017",
            "database": "analytics",
            "collection_names": ("users", "events"),
            "max_table_nesting": 2,
        },
    )()

    factory = MongoDBWorkflow().build_source_factory(config, write_disposition="replace")
    source = factory()

    assert source.kind == "mongodb"
    assert source.max_table_nesting == 2
    assert captured["database"] == "analytics"
    assert captured["collection_names"] == ["users", "events"]
    assert captured["write_disposition"] == "replace"
