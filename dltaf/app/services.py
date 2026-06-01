from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Sequence

from dltaf.services.secrets import ResolvedEnv, build_resolved_env_from_connections
from dltaf.services.secrets.vault import VaultGetter


@dataclass(frozen=True)
class SecretsProvider:
    vault_getter: VaultGetter | None = None

    def resolve_connections(self, connections: Mapping[str, Any]) -> ResolvedEnv:
        return build_resolved_env_from_connections(connections, vault_getter=self.vault_getter)


@dataclass(frozen=True)
class HttpClientFactory:
    timeout_seconds: int = 30
    verify_ssl: bool = True
    retries: int = 3
    backoff_seconds: float = 1.0
    default_headers: Mapping[str, str] = field(default_factory=dict)

    def create(
        self,
        *,
        base_url: str = "",
        default_headers: Optional[Mapping[str, str]] = None,
        timeout_seconds: Optional[int] = None,
        verify_ssl: Optional[bool] = None,
        retries: Optional[int] = None,
        backoff_seconds: Optional[float] = None,
        logger: Any = None,
    ) -> Any:
        from dltaf.adapters.http import RequestsHttpClient

        merged_headers: Dict[str, str] = dict(self.default_headers)
        if default_headers:
            merged_headers.update(dict(default_headers))

        return RequestsHttpClient(
            base_url=str(base_url or ""),
            default_headers=merged_headers,
            timeout_seconds=int(timeout_seconds) if timeout_seconds is not None else int(self.timeout_seconds),
            verify_ssl=bool(verify_ssl) if verify_ssl is not None else bool(self.verify_ssl),
            retries=int(retries) if retries is not None else int(self.retries),
            backoff_seconds=float(backoff_seconds) if backoff_seconds is not None else float(self.backoff_seconds),
            logger=logger,
        )


@dataclass(frozen=True)
class KafkaWaiterFactory:
    metadata_timeout_s: float = 10.0
    default_client_id: Optional[str] = None

    def create(
        self,
        *,
        bootstrap_servers: Sequence[str],
        consumer_kwargs: Optional[Dict[str, Any]] = None,
        logger: Any = None,
    ) -> Any:
        try:
            from dltaf.adapters.kafka import KafkaPythonWaiter
        except ModuleNotFoundError as e:  # pragma: no cover
            raise RuntimeError(
                "Kafka support requires dependency 'kafka-python'. Install project dependencies or add kafka-python to your environment."
            ) from e

        kw = dict(consumer_kwargs or {})
        if self.default_client_id and "client_id" not in kw:
            kw["client_id"] = str(self.default_client_id)

        return KafkaPythonWaiter(
            bootstrap_servers=list(bootstrap_servers),
            consumer_kwargs=kw,
            logger=logger,
            metadata_timeout_s=float(self.metadata_timeout_s),
        )


@dataclass(frozen=True)
class ApplicationServices:
    secrets: SecretsProvider
    http: HttpClientFactory
    kafka: KafkaWaiterFactory


class ApplicationServicesFactory:
    @staticmethod
    def default(*, request_id: Optional[str] = None) -> ApplicationServices:
        default_headers: Dict[str, str] = {}
        default_client_id: Optional[str] = None
        if request_id:
            default_headers["X-Request-ID"] = str(request_id)
            default_client_id = f"dlt-{request_id}"
        return ApplicationServices(
            secrets=SecretsProvider(),
            http=HttpClientFactory(default_headers=default_headers),
            kafka=KafkaWaiterFactory(default_client_id=default_client_id),
        )
