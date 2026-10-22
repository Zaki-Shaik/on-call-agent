from datetime import datetime, timedelta, timezone

from oncall.changes import ChangeRecorder
from oncall.flags.provider import FlagState

T0 = datetime(2026, 10, 22, 3, 0, tzinfo=timezone.utc)


class FakeFlags:
    def __init__(self, **variants: str) -> None:
        self.states = {n: FlagState(n, v, description=f"{n} flag") for n, v in variants.items()}

    def set(self, name: str, variant: str, targeted: tuple[str, ...] = ()) -> None:
        self.states[name] = FlagState(name, variant, targeted, f"{name} flag")

    def read_flags(self):
        return dict(self.states)


class FakeMetrics:
    def __init__(self, **versions: set[str]) -> None:
        self.versions = versions

    def service_versions(self, at, lookback_s):
        return dict(self.versions)


def recorder(pool, flags=None, metrics=None) -> ChangeRecorder:
    return ChangeRecorder(pool, "test-profile", metrics or FakeMetrics(), flags, "http://flags.example/")


def events(pool) -> list[dict]:
    with pool.connection() as conn:
        return conn.execute("SELECT * FROM change_events ORDER BY id").fetchall()


def test_first_poll_is_a_baseline(pool) -> None:
    assert recorder(pool, FakeFlags(a="off", b="off")).poll_flags(T0) == 0
    assert events(pool) == []


def test_flag_change_is_recorded_once(pool) -> None:
    flags = FakeFlags(a="off", b="off")
    r = recorder(pool, flags)
    r.poll_flags(T0)
    flags.set("a", "on")

    assert r.poll_flags(T0 + timedelta(seconds=5)) == 1
    assert r.poll_flags(T0 + timedelta(seconds=10)) == 0  # unchanged since

    [event] = events(pool)
    assert event["type"] == "flag_change"
    assert event["occurred_at"] == T0 + timedelta(seconds=5)
    assert event["service"] is None
    assert event["actor"] == "unknown"
    assert event["description"] == "flag a: off -> on (a flag)"
    assert event["link"] == "http://flags.example/"
    assert event["details"]["new"] == {"default_variant": "on", "targeted_variants": []}


def test_targeting_change_counts_as_a_flag_change(pool) -> None:
    flags = FakeFlags(a="off")
    r = recorder(pool, flags)
    r.poll_flags(T0)
    flags.set("a", "off", targeted=("on",))

    assert r.poll_flags(T0 + timedelta(seconds=5)) == 1
    assert events(pool)[0]["description"] == "flag a: off -> off (targeting: on) (a flag)"


def test_new_version_is_a_deploy(pool) -> None:
    metrics = FakeMetrics(svc_a={"1.0"}, svc_b={"2.0"})
    r = recorder(pool, metrics=metrics)
    assert r.poll_versions(T0) == 0  # baseline

    metrics.versions = {"svc_a": {"1.0", "1.1"}, "svc_b": {"2.0"}, "svc_c": {"0.1"}}
    assert r.poll_versions(T0 + timedelta(minutes=1)) == 1  # svc_c is new, not a deploy

    [event] = events(pool)
    assert (event["type"], event["service"]) == ("deploy", "svc_a")
    assert event["description"] == "svc_a version 1.0 -> 1.1"

    # Rollout completes (old version gone): no new event. A quiet service keeps its last version.
    metrics.versions = {"svc_a": {"1.1"}}
    assert r.poll_versions(T0 + timedelta(minutes=2)) == 0
    metrics.versions = {"svc_b": {"2.0"}}
    assert r.poll_versions(T0 + timedelta(minutes=3)) == 0
