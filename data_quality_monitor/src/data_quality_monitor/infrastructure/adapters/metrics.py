from prometheus_client import CollectorRegistry, Counter
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

registry = CollectorRegistry()

run_counter = Counter("dq_runs_total", "Total DQ runs", registry=registry)

endpoint_counter = Counter(
    "dq_endpoint_calls_total",
    "Total calls per endpoint",
    ["endpoint", "method", "status_code"],
    registry=registry,
)


class PrometheusMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)
        endpoint_counter.labels(
            endpoint=request.url.path,
            method=request.method,
            status_code=response.status_code,
        ).inc()
        return response
