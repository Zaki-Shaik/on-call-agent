"""Build adapters from a profile. Adding a backend type = one class + one registry entry."""

from dataclasses import dataclass

from oncall.flags.provider import FlagdUiProvider, FlagProvider
from oncall.profile import SystemProfile
from oncall.telemetry.logs import LogBackend, OpenSearchLogs
from oncall.telemetry.metrics import MetricsBackend, PrometheusMetrics
from oncall.telemetry.traces import JaegerTraces, TraceBackend

_METRICS = {"prometheus": PrometheusMetrics}
_LOGS = {"opensearch": OpenSearchLogs}
_TRACES = {"jaeger": JaegerTraces}
_FLAGS = {"flagd_ui": FlagdUiProvider}


@dataclass(frozen=True)
class Backends:
    metrics: MetricsBackend
    traces: TraceBackend
    logs: LogBackend | None
    flags: FlagProvider | None


def _pick(registry: dict, kind: str, type_: str):
    if type_ not in registry:
        raise ValueError(f"unsupported {kind} backend type {type_!r}; known: {', '.join(registry)}")
    return registry[type_]


def build_backends(profile: SystemProfile) -> Backends:
    b = profile.backends
    metrics_cls = _pick(_METRICS, "metrics", b.metrics.type)
    return Backends(
        metrics=metrics_cls(b.metrics, profile.metric_overrides, profile.service_containers),
        traces=_pick(_TRACES, "traces", b.traces.type)(b.traces),
        logs=_pick(_LOGS, "logs", b.logs.type)(b.logs) if b.logs else None,
        flags=_pick(_FLAGS, "flags", profile.flags.provider.type)(profile.flags.provider) if profile.flags else None,
    )
