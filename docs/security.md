# Security

## General principles

- never commit secrets, tokens, or production credentials
- keep all shipped examples sanitized
- avoid publishing internal hostnames, private package registries, or tenant-specific metadata

## Vault usage

Prefer Vault refs over inline credentials in manifests. `dltaf` resolves Vault refs through `vault-kv-client`.

## Private integrations

Keep customer-specific connectors in:
- a local private plugin catalog
- or a private Python package

The public OSS core should stay generic and reusable.
