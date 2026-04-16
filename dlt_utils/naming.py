from __future__ import annotations

import re


_VALID_TOKEN_RE = re.compile(r"^[a-z0-9]+(?:_[a-z0-9]+)*$")


def normalize_token(token: str) -> str:
    """Normalize an identifier token to snake_case-ish, safe for Airflow + dlt.

    - lowercases
    - replaces non-alnum with underscore
    - collapses multiple underscores
    - strips leading/trailing underscores
    """
    t = (token or "").strip().lower()
    t = re.sub(r"[^a-z0-9]+", "_", t)
    t = re.sub(r"_+", "_", t).strip("_")
    if not t:
        raise ValueError("token is empty after normalization")
    if not _VALID_TOKEN_RE.match(t):
        raise ValueError(f"token '{token}' normalized to '{t}', but still invalid")
    return t


def build_pipeline_name(*, source: str, destination: str, dataset: str, prefix: str = "dlt") -> str:
    """Company naming convention for both dlt pipeline_name and airflow dag_id.

    Example:
        dlt__oracle_colvir__to__clickhouse__colvir
    """
    src = normalize_token(source)
    dst = normalize_token(destination)
    ds = normalize_token(dataset)
    pre = normalize_token(prefix)
    return f"{pre}__{src}__to__{dst}__{ds}"


def validate_pipeline_name(name: str) -> None:
    """Raises ValueError if name does not follow the convention."""
    if not name or "__" not in name:
        raise ValueError("pipeline name must contain '__' separators")
    # minimal check: each part should be safe
    parts = [p for p in name.split("__") if p]
    for p in parts:
        normalize_token(p)