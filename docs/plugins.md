# Plugins and Private Integrations

`dltaf` uses a unified extension model. The public core can be extended in three places:

- runner plugins
- hook plugins
- infra-check plugins

That lets private integrations stay in a monorepo or private package without forking the public framework.

## Resolution model

Each registry starts with public built-ins and then loads any private modules requested by:

- environment variables
- manifest plugin lists

Environment variables:

- `DLT_RUNNER_PLUGINS`
- `DLT_HOOK_PLUGINS`
- `DLT_INFRA_CHECK_PLUGINS`

Manifest keys:

- `run.runners.plugins`
- `run.hooks.plugins`
- `run.online_checks.plugins`

Plugin specs are Python module import strings.

## Monorepo-friendly private catalog

If your private catalog still lives inside a monorepo, the easiest path is:

1. make the plugin package importable in that environment
2. reference it by module path

Example:

```bash
export PYTHONPATH="/path/to/monorepo:$PYTHONPATH"
export DLT_RUNNER_PLUGINS="internal.dltaf_plugins.customer_export.runner_plugin"
```

Then your manifest can stay stable:

```yaml
run:
  runners:
    plugins:
      - internal.dltaf_plugins.customer_export.runner_plugin

source:
  kind: internal.customer_export
```

## Minimal runner plugin

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from dltaf.app.runtime import RunContext
from dltaf.extensions.runners.registry import RunnerRegistry


@dataclass
class CustomerExportRunner:
    kind: str = "internal.customer_export"

    def validate(self, manifest: Mapping[str, Any]) -> None:
        source = manifest.get("source") or {}
        if str(source.get("kind") or "").strip() != self.kind:
            raise ValueError("source.kind mismatch")

    def run(self, manifest: Mapping[str, Any], ctx: RunContext) -> Any:
        return {"status": "replace with real implementation"}


def register_runners(registry: RunnerRegistry) -> None:
    registry.register(CustomerExportRunner())
```

## Why the registry model matters

This gives you a clean migration path:

- start with a module inside a private monorepo
- later move the same code into a private wheel
- keep the same `source.kind`
- keep the same manifest contract

Only the delivery mechanism changes. The YAML does not.

## Built-in vs private responsibility

The public core should own:

- generic execution services
- reusable SQL and MongoDB integrations
- manifest schema and migration helpers
- generic secret resolution

Private plugins should own:

- business APIs
- tenant-specific Kafka conventions
- company-specific auth flows
- customer payload contracts

That split keeps the OSS package reusable while private teams keep their own delivery logic.
