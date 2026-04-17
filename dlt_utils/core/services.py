"""Compatibility shim for application service factories.

Native implementation now lives in :mod:`dltaf.app.services`.
"""

from dltaf.app.deprecation import warn_legacy_module

warn_legacy_module(__name__, replacement="dltaf.app.services")

from dltaf.app.services import (
    ApplicationServices,
    ApplicationServicesFactory,
    HttpClientFactory,
    KafkaWaiterFactory,
    SecretsProvider,
)


class ServiceContainer(ApplicationServices):
    @staticmethod
    def default(*, request_id=None):
        return ApplicationServicesFactory.default(request_id=request_id)


__all__ = [
    "ServiceContainer",
    "ApplicationServices",
    "ApplicationServicesFactory",
    "SecretsProvider",
    "HttpClientFactory",
    "KafkaWaiterFactory",
]
