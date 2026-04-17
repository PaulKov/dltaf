# Release Process

## Local release checklist

```bash
ruff check .
pytest
python -m build
mkdocs build --strict
```

## Packaging expectations

Every public release should ship:

- clean wheel and sdist
- working `dltaf` CLI
- canonical examples under `dltaf/examples/`
- documentation that matches the actual runtime contract

## Branching

The public repository is intended to use `master` as the default branch.

Recommended flow:

1. prepare and validate changes in a feature branch
2. merge into `master`
3. let CI, docs, and publish workflows run from `master`
4. cut a tagged release from the validated commit

## GitHub Actions

The repository should keep three main workflows healthy:

- `ci.yml`
- `pages.yml`
- `publish.yml`

## PyPI strategy

Prefer explicit version bumps and immutable releases:

- patch for fixes
- minor for new public capabilities or better UX
- major only for real manifest-breaking changes

If a token was used for bootstrap, rotate it after the public release flow is fully configured.
