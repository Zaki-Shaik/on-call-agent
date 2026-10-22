import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import pytest

from oncall.evidence import EvidenceDraft, EvidenceStore
from oncall.links import Linker
from oncall.profile import Links


def new_incident(pool):
    with pool.connection() as conn:
        return conn.execute(
            "INSERT INTO incidents (status, service, system_profile, opened_at)"
            " VALUES ('investigating', 'svc', 'p', now()) RETURNING id").fetchone()["id"]


def draft(n: int) -> EvidenceDraft:
    return EvidenceDraft("query_metrics", {"n": n}, f"finding {n}", f"http://link/{n}", {"n": n})


def test_refs_are_sequential_per_incident(pool) -> None:
    store = EvidenceStore(pool)
    a, b = new_incident(pool), new_incident(pool)
    assert store.save(a, [draft(1), draft(2)]) == ["E1", "E2"]
    assert store.save(b, [draft(3)]) == ["E1"]
    assert store.save(a, [draft(4)]) == ["E3"]
    assert store.get(a, "E3")["summary"] == "finding 4"


def test_concurrent_saves_never_share_a_ref(pool) -> None:
    store = EvidenceStore(pool)
    incident = new_incident(pool)
    with ThreadPoolExecutor(max_workers=8) as ex:
        refs = [r for batch in ex.map(lambda n: store.save(incident, [draft(n), draft(n)]), range(16)) for r in batch]
    assert sorted(refs, key=lambda r: int(r[1:])) == [f"E{i}" for i in range(1, 33)]


def test_unknown_incident_is_rejected(pool) -> None:
    with pytest.raises(ValueError):
        EvidenceStore(pool).save(uuid4(), [draft(1)])


LINKS = Links(grafana="http://g/grafana", jaeger="http://j/jaeger/ui", flags="http://f/",
              grafana_datasources={"metrics": "prom-uid", "logs": "os-uid"})
START = datetime(2026, 10, 22, 3, 0, tzinfo=timezone.utc)
END = START + timedelta(minutes=30)


def test_grafana_explore_link_carries_query_and_range() -> None:
    url = Linker(LINKS).metrics('sum(rate(x{service_name="a"}[2m]))', START, END)
    parts = urlsplit(url)
    assert url.startswith("http://g/grafana/explore?")
    pane = json.loads(parse_qs(parts.query)["panes"][0])["a"]
    assert pane["datasource"] == "prom-uid"
    assert pane["queries"][0]["expr"] == 'sum(rate(x{service_name="a"}[2m]))'
    assert pane["range"] == {"from": str(int(START.timestamp() * 1000)), "to": str(int(END.timestamp() * 1000))}


def test_jaeger_links() -> None:
    linker = Linker(LINKS)
    assert linker.trace("abc123") == "http://j/jaeger/ui/trace/abc123"
    query = parse_qs(urlsplit(linker.trace_search("checkout", START, END, "PlaceOrder", errors_only=True)).query)
    assert query["service"] == ["checkout"] and query["operation"] == ["PlaceOrder"]
    assert json.loads(query["tags"][0]) == {"error": "true"}


def test_missing_link_config_gives_no_link() -> None:
    linker = Linker(Links(grafana="http://g"))
    assert linker.metrics("up", START, END) is None
    assert linker.trace("abc") is None
