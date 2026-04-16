# Release Process

## Local release checklist

```bash
ruff check .
pytest
python -m build
mkdocs build
```

## GitHub Actions

The repository ships three main workflows:
- `ci.yml` for lint, tests, and wheel smoke-install
- `pages.yml` for GitHub Pages deployment
- `publish.yml` for PyPI release publishing

## PyPI strategy

The repository is ready for PyPI publication. A trusted publishing workflow is the preferred long-term setup. A token-based manual bootstrap is acceptable for the first release.

## Versioning

Start with small, explicit semantic version bumps:
- patch for fixes
- minor for new built-ins, CLI features, or extension APIs
- major for manifest-breaking changes
