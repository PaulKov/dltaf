from __future__ import annotations

from dltaf.app.services import SecretsProvider as NativeSecretsProvider


class CompositeSecretsProvider(NativeSecretsProvider):
    """Compatibility wrapper around the canonical dltaf secrets service."""

    pass
