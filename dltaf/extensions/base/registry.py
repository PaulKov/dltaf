from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Generic, Iterator, List, Optional, TypeVar

from .common import ExtensionInfo, infer_description

T = TypeVar("T")


@dataclass
class NamedObjectRegistry(Generic[T]):
    """Registry for named singleton-style extensions."""

    key_getter: Callable[[T], str]
    validator: Callable[[Any], bool]
    kind_label: str = "extension"
    _items: Dict[str, T] = field(default_factory=dict)
    _meta: Dict[str, ExtensionInfo] = field(default_factory=dict)
    _active_origin: Optional[str] = None

    @contextmanager
    def default_origin(self, origin: str) -> Iterator[None]:
        prev = self._active_origin
        self._active_origin = str(origin).strip() or None
        try:
            yield
        finally:
            self._active_origin = prev

    def register(self, item: T, *, origin: Optional[str] = None, description: Optional[str] = None) -> None:
        if not self.validator(item):
            raise ValueError(f"Object is not a valid {self.kind_label}")
        key = str(self.key_getter(item) or "").strip()
        if not key:
            raise ValueError(f"{self.kind_label} key must be a non-empty string")
        if key in self._items:
            raise ValueError(f"{self.kind_label.capitalize()} already registered: {key}")
        self._items[key] = item

        origin_label = str(origin).strip() if origin is not None else ""
        if not origin_label:
            origin_label = self._active_origin or "builtin"

        desc = str(description).strip() if description is not None else ""
        if not desc:
            desc = infer_description(item)

        self._meta[key] = ExtensionInfo(name=key, origin=origin_label, description=desc)

    def get(self, name: str) -> T:
        key = str(name or "").strip()
        if key not in self._items:
            raise KeyError(f"Unknown {self.kind_label}: {key}")
        return self._items[key]

    def names(self) -> List[str]:
        return sorted(self._items.keys())

    def all(self) -> List[T]:
        return [self._items[name] for name in self.names()]

    def info(self, name: str) -> ExtensionInfo:
        key = str(name or "").strip()
        if key not in self._meta:
            raise KeyError(f"Unknown {self.kind_label}: {key}")
        return self._meta[key]

    def infos(self) -> List[ExtensionInfo]:
        return [self._meta[name] for name in self.names() if name in self._meta]

    def clone(self) -> "NamedObjectRegistry[T]":
        cloned: NamedObjectRegistry[T] = NamedObjectRegistry(
            key_getter=self.key_getter,
            validator=self.validator,
            kind_label=self.kind_label,
        )
        for name in self.names():
            meta = self.info(name)
            cloned.register(self.get(name), origin=meta.origin, description=meta.description)
        return cloned


@dataclass
class NamedFactoryRegistry(Generic[T]):
    """Registry for extensions that should be created from factories."""

    validator: Callable[[Any], bool]
    kind_label: str = "extension"
    _factories: Dict[str, Callable[[], T]] = field(default_factory=dict)
    _meta: Dict[str, ExtensionInfo] = field(default_factory=dict)
    _active_origin: Optional[str] = None

    @contextmanager
    def default_origin(self, origin: str) -> Iterator[None]:
        prev = self._active_origin
        self._active_origin = str(origin).strip() or None
        try:
            yield
        finally:
            self._active_origin = prev

    def register_factory(
        self,
        name: str,
        factory: Callable[[], T],
        *,
        origin: Optional[str] = None,
        description: Optional[str] = None,
    ) -> None:
        key = str(name or "").strip()
        if not key:
            raise ValueError(f"{self.kind_label} name must be a non-empty string")
        if key in self._factories:
            raise ValueError(f"{self.kind_label.capitalize()} already registered: {key}")

        try:
            instance = factory()
        except Exception as exc:  # pragma: no cover - defensive
            raise ValueError(f"Factory for {key!r} failed to create {self.kind_label}: {exc}") from exc
        if not self.validator(instance):
            raise ValueError(f"Factory for {key!r} did not produce a valid {self.kind_label}")

        self._factories[key] = factory

        origin_label = str(origin).strip() if origin is not None else ""
        if not origin_label:
            origin_label = self._active_origin or "builtin"

        desc = str(description).strip() if description is not None else ""
        if not desc:
            desc = infer_description(instance)

        self._meta[key] = ExtensionInfo(name=key, origin=origin_label, description=desc)

    def create(self, name: str) -> T:
        key = str(name or "").strip()
        if key not in self._factories:
            raise KeyError(f"Unknown {self.kind_label}: {key}")
        return self._factories[key]()

    def names(self) -> List[str]:
        return sorted(self._factories.keys())

    def info(self, name: str) -> ExtensionInfo:
        key = str(name or "").strip()
        if key not in self._meta:
            raise KeyError(f"Unknown {self.kind_label}: {key}")
        return self._meta[key]

    def infos(self) -> List[ExtensionInfo]:
        return [self._meta[name] for name in self.names() if name in self._meta]

    def clone(self) -> "NamedFactoryRegistry[T]":
        cloned: NamedFactoryRegistry[T] = NamedFactoryRegistry(validator=self.validator, kind_label=self.kind_label)
        for name in self.names():
            meta = self.info(name)
            cloned.register_factory(name, self._factories[name], origin=meta.origin, description=meta.description)
        return cloned


__all__ = ["NamedFactoryRegistry", "NamedObjectRegistry"]
