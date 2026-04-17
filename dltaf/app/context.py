from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .services import ApplicationServicesFactory


@dataclass(frozen=True)
class AppContext:
    """Composition root for the `dltaf` command surface."""

    repo_root: Path
    logger: logging.Logger
    services_factory: ApplicationServicesFactory = field(default_factory=ApplicationServicesFactory)

    @classmethod
    def build(cls, repo_root: Optional[str] = None, logger: Optional[logging.Logger] = None) -> "AppContext":
        root = Path(repo_root or ".").expanduser().resolve()
        log = logger or logging.getLogger("dltaf")
        if not log.handlers:
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
        return cls(repo_root=root, logger=log)

    def create_runtime_services(self, *, request_id: Optional[str] = None):
        return self.services_factory.default(request_id=request_id)
