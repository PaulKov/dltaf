# Security Policy

## Supported versions

The latest published minor release receives fixes first. Earlier releases may receive fixes at maintainer discretion depending on severity.

## Reporting a vulnerability

Please do not open a public issue for vulnerabilities that could expose users, secrets, or infrastructure.

Instead:
- use GitHub private vulnerability reporting when available
- or contact the maintainer privately through the repository security contact route

Please include:
- affected version
- reproduction steps
- expected impact
- any suggested mitigation or patch idea

## Safe contribution guidelines

- never commit real credentials or Vault tokens
- keep example manifests sanitized
- avoid publishing internal hostnames, topic names, or dataset identifiers
- prefer environment-variable or Vault-based configuration for anything sensitive
