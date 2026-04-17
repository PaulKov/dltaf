# Security

## Public repository rules

The public `dltaf` repository should never contain:

- production credentials
- real Vault refs for private environments
- internal hostnames
- private Kafka topics
- tenant-specific API defaults

All shipped examples and templates must stay sanitized.

## Secret resolution

The recommended approach is:

- keep credentials out of manifests
- resolve them through Vault
- let `vault-kv-client` handle authentication and transport details

Supported ref forms:

- `vault://mount/path`
- `mount:path`
- mapping form with `mount_point`, `path`, `kv_version`

## Private integrations

Private business logic should live in private plugin modules. That gives you a clean separation:

- OSS core stays reusable and publishable
- internal transport logic stays private

This is both an architectural boundary and a security boundary.

## Release hygiene

Before every public release:

- run `ruff check .`
- run `pytest`
- run `python -m build`
- run `mkdocs build --strict`
- scan docs and examples for internal names, hosts, and secrets

If a secret was ever pasted into a terminal, chat, or issue tracker, rotate it instead of assuming it is still safe.
