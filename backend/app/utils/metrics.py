"""
Metrics utility with optional Prometheus exporter.

If `observability.prometheus.enabled` is true in config, registers a `/metrics`
route on the FastAPI app and exposes counters/gauges.
"""

from __future__ import annotations

from typing import Optional
import logging

logger = logging.getLogger(__name__)

try:
    from prometheus_client import Counter, Gauge, CollectorRegistry, CONTENT_TYPE_LATEST, generate_latest
    PROM_AVAILABLE = True
except Exception:
    PROM_AVAILABLE = False
    Counter = Gauge = CollectorRegistry = object  # type: ignore
    CONTENT_TYPE_LATEST = "text/plain; charset=utf-8"
    def generate_latest(_reg=None):  # type: ignore
        return b""


class Metrics:
    def __init__(self, enabled: bool = False, namespace: str = "xbrl_validator") -> None:
        self.enabled = enabled and PROM_AVAILABLE
        self.namespace = namespace
        self.registry: Optional[CollectorRegistry] = None
        self.messages_catalog_ids_loaded: Optional[Gauge] = None
        self.messages_resolved_total: Optional[Counter] = None
        self.messages_unresolved_total: Optional[Counter] = None
        if self.enabled:
            try:
                self.registry = CollectorRegistry()
                self.messages_catalog_ids_loaded = Gauge(
                    f"{namespace}_messages_catalog_ids_loaded",
                    "Number of message ids loaded in catalog",
                    registry=self.registry,
                )
                self.messages_resolved_total = Counter(
                    f"{namespace}_messages_resolved_total",
                    "Total resolved catalog messages",
                    registry=self.registry,
                )
                self.messages_unresolved_total = Counter(
                    f"{namespace}_messages_unresolved_total",
                    "Total unresolved catalog messages",
                    registry=self.registry,
                )
            except Exception as e:
                logger.warning("Prometheus initialization failed: %s", e)
                self.enabled = False

    def set_catalog_ids_loaded(self, value: int) -> None:
        try:
            if self.enabled and self.messages_catalog_ids_loaded is not None:
                self.messages_catalog_ids_loaded.set(float(value))
        except Exception:
            logger.debug("set_catalog_ids_loaded failed", exc_info=True)

    def inc_messages_resolved(self, value: int = 1) -> None:
        try:
            if self.enabled and self.messages_resolved_total is not None and value:
                self.messages_resolved_total.inc(value)
        except Exception:
            logger.debug("inc_messages_resolved failed", exc_info=True)

    def inc_messages_unresolved(self, value: int = 1) -> None:
        try:
            if self.enabled and self.messages_unresolved_total is not None and value:
                self.messages_unresolved_total.inc(value)
        except Exception:
            logger.debug("inc_messages_unresolved failed", exc_info=True)

    def mount_endpoint(self, app, path: str = "/metrics", require_secret: bool = False, secret_header: str = "X-Prom-Secret", secret_value: str = "") -> None:
        if not self.enabled or not self.registry:
            return
        try:
            from fastapi import Response, HTTPException, Request
            @app.get(path)
            async def metrics_endpoint(request: Request):
                if require_secret:
                    provided = request.headers.get(secret_header)
                    if not provided or provided != secret_value:
                        raise HTTPException(status_code=403, detail="Forbidden")
                data = generate_latest(self.registry)
                return Response(content=data, media_type=CONTENT_TYPE_LATEST)
            logger.info("Prometheus metrics endpoint mounted at %s", path)
        except Exception:
            logger.warning("Failed to mount Prometheus endpoint", exc_info=True)


