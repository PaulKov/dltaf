# Contributing to dltaf

Thanks for considering a contribution.

## Development setup

```bash
git clone https://github.com/PaulKov/dltaf.git
cd dltaf
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .[dev]
```

## Local checks

```bash
ruff check .
pytest
python -m build
mkdocs build
```

## Design principles

- keep the public core generic and reusable
- keep private integrations out of the OSS repository
- preserve manifest stability when moving a private connector between local catalogs and private packages
- prefer simple, explicit configuration over hidden conventions
- document new behavior in both user-facing docs and developer-facing guides

## Pull requests

Please keep pull requests focused and easy to review:
- explain the problem and the chosen tradeoff
- include tests for behavior changes
- update docs when commands, manifests, or workflows change
- avoid mixing unrelated refactors with functional changes

## Plugins

If you contribute plugin-related improvements:
- keep built-in kinds generic
- do not add customer-specific integrations to the public repo
- prefer extension hooks and registry improvements over hardcoded special cases

## Security

Do not commit secrets, internal hostnames, private package indexes, or production credentials. If you find a security issue, follow the process in [SECURITY.md](SECURITY.md).
