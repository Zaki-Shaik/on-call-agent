"""Adapters against recorded backend responses (tests/fixtures/backends), via httpx.MockTransport."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

from oncall.flags.provider import FlagdUiProvider, parse_flagd_flags
from oncall.profile import Backend, FlagProvider
from oncall.telemetry.logs import OpenSearchLogs
from oncall.telemetry.metrics import PrometheusMetrics
from oncall.telemetry.models import BackendError, Signal
from oncall.telemetry.traces import JaegerTraces, parse_jaeger_trace

FIXTURES = Path(__file__).parent / "fixtures" / "backends"
NOW = datetime.now(timezone.utc)


def load(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def respond(body, status: int = 200):
    return lambda request: httpx.Response(status, json=body)


# Prometheus

def prometheus(handler, **kw) -> PrometheusMetrics:
    return PrometheusMetrics(Backend(type="prometheus", url="http://prom:9090"), client=client(handler), **kw)


def test_prometheus_range_query_parses_series() -> None:
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["query"] = request.url.params["query"]
        return httpx.Response(200, json=load("prometheus_error_rate_payment.json"))

    series = prometheus(handler).series("payment", Signal.ERROR_RATE, NOW - timedelta(hours=1), NOW, 30)

    assert 'service_name="payment"' in seen["query"] and "or vector(0)" in seen["query"]
    assert series[0][1] == 0.0  # zero is a value, not a gap
    assert series[-1][1] > 0
    assert all(a[0] < b[0] for a, b in zip(series, series[1:]))
    assert series[0][0].tzinfo is not None


def test_prometheus_empty_result_is_empty_series() -> None:
    assert prometheus(respond(load("prometheus_empty.json"))).series(
        "nope", Signal.TRAFFIC, NOW - timedelta(hours=1), NOW, 30) == []


def test_prometheus_nan_points_are_gaps() -> None:
    body = {"status": "success", "data": {"resultType": "matrix", "result": [
        {"metric": {}, "values": [[1, "0.5"], [2, "NaN"], [3, "0.7"]]}]}}
    assert [v for _, v in prometheus(respond(body)).series(
        "a", Signal.ERROR_RATE, NOW - timedelta(hours=1), NOW, 30)] == [0.5, 0.7]


def test_prometheus_bad_query_raises_backend_error() -> None:
    recorded = load("prometheus_bad_query_http400.json")
    with pytest.raises(BackendError, match="HTTP 400"):
        prometheus(respond(recorded["body"], recorded["http_status"])).series(
            "a", Signal.TRAFFIC, NOW - timedelta(hours=1), NOW, 30)


def test_prometheus_timeout_raises_backend_error() -> None:
    def handler(request):
        raise httpx.ReadTimeout("slow", request=request)

    with pytest.raises(BackendError, match="timed out"):
        prometheus(handler).series("a", Signal.TRAFFIC, NOW - timedelta(hours=1), NOW, 30)


def test_prometheus_service_versions() -> None:
    versions = prometheus(respond(load("prometheus_service_versions.json"))).service_versions(NOW, 300)
    assert versions["payment"] == {"3.1.0"}
    assert all(isinstance(v, set) for v in versions.values())


def test_label_values_are_escaped_and_names_overridable() -> None:
    p = prometheus(respond({}), overrides={"calls": "my_calls_total"}, service_containers={"svc": "svc-1"})
    assert 'service_name="we\\"ird"' in p.query_text('we"ird', Signal.TRAFFIC)
    assert p.query_text("x", Signal.TRAFFIC).startswith("sum(rate(my_calls_total{")
    assert 'container_name="svc-1"' in p.query_text("svc", Signal.MEMORY)


# Jaeger

def test_jaeger_error_trace_parses_spans_and_errors() -> None:
    [raw] = load("jaeger_checkout_error_traces.json")["data"]
    trace = parse_jaeger_trace(raw)

    services = {s.service for s in trace.spans}
    assert {"checkout", "payment"} <= services
    errored = [s for s in trace.spans if s.error]
    assert any(s.service == "payment" for s in errored)
    ids = {s.span_id for s in trace.spans}
    assert sum(1 for s in trace.spans if s.parent_id not in ids) >= 1  # at least one root
    assert all(s.kind in {"server", "client", "producer", "consumer", "internal"} for s in trace.spans)


def test_jaeger_consumer_span_keeps_link_to_producer() -> None:
    [raw] = load("jaeger_fraud_detection_trace.json")["data"]
    trace = parse_jaeger_trace(raw)
    linked = [s for s in trace.spans if s.links]
    assert linked, "consumer span should carry a link to the producer in another trace"
    assert all(link[0] != trace.trace_id for s in linked for link in s.links)
    assert linked[0].attributes.get("messaging.destination.name") == "orders"


def test_jaeger_find_traces_sends_error_filter() -> None:
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(request.url.params)
        return httpx.Response(200, json=load("jaeger_checkout_error_traces.json"))

    j = JaegerTraces(Backend(type="jaeger", url="http://jaeger/ui"), client=client(handler))
    traces = j.find_traces("checkout", NOW - timedelta(minutes=30), NOW, 50, errors_only=True)
    assert len(traces) == 1
    assert json.loads(seen["tags"]) == {"error": "true"} and seen["service"] == "checkout"


# OpenSearch

def opensearch(handler) -> OpenSearchLogs:
    return OpenSearchLogs(Backend(type="opensearch", url="http://os:9200", options={"index_pattern": "otel-logs-*"}),
                          client=client(handler))


def test_opensearch_sample_reads_otel_fields() -> None:
    total, records = opensearch(respond(load("opensearch_payment_sample.json"))).sample(
        "payment", NOW - timedelta(minutes=30), NOW, 25)
    assert total >= len(records) > 0
    assert all(r.body and r.timestamp.tzinfo for r in records)
    assert {r.severity_text for r in records} <= {"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "info", "error", "warn"}
    assert any(r.trace_id for r in records)


def test_opensearch_counts_by_severity() -> None:
    counts = opensearch(respond(load("opensearch_payment_severity_counts.json"))).counts_by_severity(
        "payment", NOW - timedelta(minutes=30), NOW)
    assert counts and all(isinstance(v, int) for v in counts.values())


def test_opensearch_query_filters() -> None:
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json=load("opensearch_payment_sample.json"))

    opensearch(handler).sample("payment", NOW - timedelta(minutes=30), NOW, 10, min_severity="error", text="failed")
    filters = seen["body"]["query"]["bool"]["filter"]
    assert {"term": {"resource.service.name.keyword": "payment"}} in filters
    assert {"range": {"severity.number": {"gte": 17}}} in filters
    assert {"match_phrase": {"body": "failed"}} in filters


# flagd-ui

def test_flagd_ui_read_and_targeting() -> None:
    raw = load("flagd_ui_read.json")
    provider = FlagdUiProvider(FlagProvider(type="flagd_ui", url="http://flagd-ui:4000"), client=client(respond(raw)))
    flags = provider.read_flags()
    assert flags["paymentFailure"].default_variant in raw["flags"]["paymentFailure"]["variants"]
    # As shipped, the targeting rule serves "off" on both branches: no targeted variants.
    assert flags["productCatalogFailure"].targeted_variants == ()

    # flagd-ui switches it on by rewriting the rule's "then" branch.
    raw["flags"]["productCatalogFailure"]["targeting"]["if"][1] = "on"
    assert parse_flagd_flags(raw["flags"])["productCatalogFailure"].describe() == "off (targeting: on)"
