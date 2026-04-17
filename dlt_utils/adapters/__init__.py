"""Adapters layer.

Stage 3 introduces a small *adapters* package.

Why:
- isolate integrations with external systems (Vault, Airflow Variables, Kafka, HTTP, etc.)
- make it easier to swap implementations later (ports & adapters / hexagonal style)

Currently implemented adapters:
- dlt_utils.adapters.secrets (Vault -> Airflow Variables fallback)
- dlt_utils.adapters.kafka (Kafka waiter for request/response pattern)
"""

from __future__ import annotations
