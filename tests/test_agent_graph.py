"""The investigation graph with a scripted fake model and fake tools (no LLM, no telemetry backends)."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.checkpoint.memory import InMemorySaver
from pydantic import BaseModel

from oncall.agent import graph as g
from oncall.agent.factory import create_checkpointer
from oncall.agent.graph import Budgets, Investigator
from oncall.agent.tools import TOOLS, ToolSpec
from oncall.evidence import EvidenceDraft
from oncall.links import Linker
from oncall.llm import Pricing
from oncall.profile import load_profile

ROOT = Path(__file__).resolve().parents[1]
PROFILE = load_profile(ROOT / "profiles" / "otel-demo.yaml", env={})
FLAG = PROFILE.flags.allowed[0].name
NOW = datetime.now(timezone.utc)
USAGE = {"input_tokens": 1000, "output_tokens": 100, "total_tokens": 1100}
E1 = {"text": "checkout -> payment errors", "evidence": ["E1"]}


class Crash(BaseException):
    """Stands in for the worker process dying: not an Exception, so nothing in the graph catches it."""


class FakeModel:
    """Returns scripted replies in order and records what each call was given."""

    def __init__(self, *replies) -> None:
        self.replies = list(replies)
        self.calls: list[tuple[list, dict]] = []

    def bind_tools(self, tools, **kwargs):
        return _Bound(self, {"tools": tools, **kwargs})


class _Bound:
    def __init__(self, model: FakeModel, kwargs: dict) -> None:
        self.model, self.kwargs = model, kwargs

    def bind(self, **kwargs):
        return _Bound(self.model, self.kwargs | kwargs)

    def invoke(self, messages):
        self.model.calls.append((list(messages), self.kwargs))
        reply = self.model.replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply


def tool_call(name: str, call_id: str = "c1", **args) -> AIMessage:
    defaults = {"get_traces": {"operation": None, "errors_only": False, "window": None},
                "query_metrics": {"signals": None, "window": None}}
    return AIMessage("", tool_calls=[{"name": name, "args": defaults.get(name, {}) | args, "id": call_id}],
                     usage_metadata=USAGE)


def text(content: str) -> AIMessage:
    return AIMessage(content, usage_metadata=USAGE)


def report(**overrides) -> AIMessage:
    body = {
        "headline": "payment rejects charges after a flag change", "summary": "One sentence.",
        "root_cause_service": "payment", "confidence": "high",
        "root_cause": {"text": "payment errors started with the flag change", "evidence": ["E1", "E3"]},
        "started_at": None, "duration_minutes": None,
        "correlated_change": {"text": "flag changed", "evidence": ["E2"]},
        "timeline": [], "ruled_out": [],
        "proposed_fix": {"kind": "set_flag", "flag": FLAG, "value": "off", "description": "Set it back."},
    } | overrides
    return AIMessage(json.dumps(body), usage_metadata=USAGE)


class Finding(BaseModel):
    finding: str
    evidence: str


def fake_tools(counter: list) -> dict[str, ToolSpec]:
    """Real argument schemas, fake behavior: each call saves one evidence row."""

    def make(spec: ToolSpec) -> ToolSpec:
        def run(ctx, args, window):
            counter.append(spec.name)
            [ref] = ctx.save([EvidenceDraft(spec.name, {}, f"{spec.name} on {getattr(args, 'service', None)}", None)])
            return Finding(finding=f"{spec.name} result", evidence=ref)

        return ToolSpec(spec.name, spec.description, spec.args, run)

    return {name: make(spec) for name, spec in TOOLS.items()}


def fake_starter(counter: list):
    def build(ctx, incident, alerts, window):
        counter.append("starter")
        refs = ctx.save([EvidenceDraft("get_service_map", {}, "checkout -> payment errors_up", None),
                         EvidenceDraft("list_recent_changes", {}, "flag changed", None)])
        return {"alerts": [a["summary"] for a in alerts], "refs": refs,
                "service_map": {"edges": [{"source": "checkout", "target": "payment", "type": "call",
                                           "changes": ["errors_up"]}]}}

    return build


@pytest.fixture
def incident_id(pool):
    with pool.connection() as conn:
        incident = conn.execute(
            "INSERT INTO incidents (status, service, system_profile, opened_at)"
            " VALUES ('investigating', 'checkout', 'otel-demo', %s) RETURNING id",
            (NOW - timedelta(minutes=5),)).fetchone()["id"]
        conn.execute(
            "INSERT INTO alerts (source, fingerprint, starts_at, status, service, severity, summary, incident_id,"
            " raw_payload) VALUES ('alertmanager', 'f1', %s, 'firing', 'checkout', 'critical',"
            " 'checkout: errors', %s, '{}')", (NOW - timedelta(minutes=5), incident))
    return incident


def investigator(pool, redis_client, model, budgets=Budgets(), checkpointer=None, counter=None,
                 pricing=Pricing(0.4, 1.6)) -> Investigator:
    counter = [] if counter is None else counter
    return Investigator(profile=PROFILE, backends=None, links=Linker(PROFILE.links), pool=pool,
                        redis_client=redis_client, maps=None, model=model, pricing=pricing, budgets=budgets,
                        checkpointer=checkpointer or InMemorySaver(), tools=fake_tools(counter),
                        starter=fake_starter(counter))


def stored(pool, incident_id) -> tuple[dict, dict]:
    with pool.connection() as conn:
        row = conn.execute("SELECT * FROM reports WHERE incident_id = %s", (incident_id,)).fetchone()
        incident = conn.execute("SELECT * FROM incidents WHERE id = %s", (incident_id,)).fetchone()
    return row, incident


def state(inv: Investigator, incident_id) -> dict:
    return inv.graph.get_state({"configurable": {"thread_id": str(incident_id)}}).values


def test_happy_path_cites_evidence_and_proposes_an_allowed_flag(pool, redis_client, incident_id) -> None:
    model = FakeModel(tool_call("get_traces", service="payment"), text("payment rejects charges."), report())
    inv = investigator(pool, redis_client, model)

    inv.run(incident_id)

    row, incident = stored(pool, incident_id)
    assert row["confidence"] == "high" and row["forced_stop"] is None
    assert row["proposed_action"] == {"flag": FLAG, "value": "off"}
    assert incident["status"] == "awaiting_approval"
    assert (row["tool_calls"], row["llm_calls"]) == (1, 3)
    assert float(row["cost_usd"]) == pytest.approx(3 * (1000 * 0.4 + 100 * 1.6) / 1e6)
    assert row["time_to_report_seconds"] >= 300
    final = state(inv, incident_id)
    assert final["approval_status"] == "pending"
    assert [e["ref"] for e in final["evidence"]] == ["E1", "E2", "E3"]
    # Tool output reaches the model wrapped as untrusted data.
    tool_message = next(m for m in final["messages"] if isinstance(m, ToolMessage))
    assert tool_message.content.startswith('<data source="get_traces" trust="untrusted">')
    # The agent call had the tools, one at a time; the report call had the schema and no tool use.
    agent_kwargs, report_kwargs = model.calls[0][1], model.calls[2][1]
    assert agent_kwargs["parallel_tool_calls"] is False and len(agent_kwargs["tools"]) == len(TOOLS)
    assert report_kwargs["tool_choice"] == "none"
    assert report_kwargs["response_format"]["json_schema"]["strict"] is True


def test_invalid_citations_are_repaired(pool, redis_client, incident_id) -> None:
    # No tool ran, so only the starter pack's E1 and E2 exist.
    model = FakeModel(text("done"), report(root_cause={"text": "x", "evidence": ["E1", "E99"]}), report(root_cause=E1))
    inv = investigator(pool, redis_client, model)

    inv.run(incident_id)

    repair_request = model.calls[2][0][-1]
    assert isinstance(repair_request, HumanMessage) and "E99" in repair_request.content
    row, _ = stored(pool, incident_id)
    assert row["confidence"] == "high" and row["llm_calls"] == 3


def test_log_only_conclusion_is_rejected_and_falls_back_after_repairs(pool, redis_client, incident_id) -> None:
    logs_only = report(root_cause={"text": "the log says so", "evidence": ["E3"]})
    model = FakeModel(tool_call("search_logs", service="payment", min_severity=None, text=None, window=None),
                      text("done"), logs_only, logs_only, logs_only)
    inv = investigator(pool, redis_client, model)

    inv.run(incident_id)

    row, incident = stored(pool, incident_id)
    assert row["forced_stop"] == g.INVALID_REPORT
    assert row["confidence"] == "inconclusive" and row["proposed_action"] is None
    assert row["report"]["root_cause_service"] == "payment"  # leading suspect from the starter map
    assert incident["status"] == "reported"


def test_tool_budget_forces_a_partial_report_without_action(pool, redis_client, incident_id) -> None:
    model = FakeModel(tool_call("get_traces", "c1", service="a"), tool_call("get_traces", "c2", service="b"),
                      report())
    inv = investigator(pool, redis_client, model, Budgets(max_tool_calls=2))

    inv.run(incident_id)

    row, incident = stored(pool, incident_id)
    assert row["forced_stop"] == g.TOOL_BUDGET
    assert row["confidence"] == "low"  # the model said high; a forced stop caps it
    assert row["proposed_action"] is None and row["report"]["proposed_fix"]["kind"] == "report_only"
    assert "stopped before you finished" in model.calls[-1][0][-1].content
    assert incident["status"] == "reported"


def test_repeated_calls_return_the_earlier_result_then_stop(pool, redis_client, incident_id) -> None:
    counter = []
    same = lambda i: tool_call("get_traces", f"c{i}", service="payment")  # noqa: E731
    model = FakeModel(same(1), same(2), same(3), report())
    inv = investigator(pool, redis_client, model, Budgets(max_repeats=2), counter=counter)

    inv.run(incident_id)

    assert counter.count("get_traces") == 1
    tool_messages = [m for m in state(inv, incident_id)["messages"] if isinstance(m, ToolMessage)]
    assert tool_messages[1].content.startswith("Repeat of an earlier identical call")
    assert '<data source="get_traces"' in tool_messages[1].content  # the cached result, from Redis
    assert stored(pool, incident_id)[0]["forced_stop"] == g.REPEATS


def test_cost_budget_answers_pending_calls_before_the_report(pool, redis_client, incident_id) -> None:
    model = FakeModel(tool_call("get_traces", service="payment"), report(root_cause=E1))
    inv = investigator(pool, redis_client, model, pricing=Pricing(1000, 1000))  # $1.1 per call

    inv.run(incident_id)

    assert len(model.calls) == 2
    report_messages = model.calls[1][0]
    assert isinstance(report_messages[-2], ToolMessage) and "not run" in report_messages[-2].content
    assert stored(pool, incident_id)[0]["forced_stop"] == g.COST_BUDGET


def test_model_failure_ends_in_a_code_written_report(pool, redis_client, incident_id) -> None:
    model = FakeModel(RuntimeError("503 from the model"))
    inv = investigator(pool, redis_client, model)

    inv.run(incident_id)

    row, _ = stored(pool, incident_id)
    assert row["forced_stop"] == g.MODEL_ERROR and row["confidence"] == "inconclusive"
    assert len(model.calls) == 1  # no report call to a model that is failing


def test_without_a_model_the_pipeline_still_completes(pool, redis_client, incident_id) -> None:
    inv = investigator(pool, redis_client, None)

    inv.run(incident_id)

    row, incident = stored(pool, incident_id)
    assert row["forced_stop"] == g.NO_MODEL and incident["status"] == "reported"


def test_crashed_worker_resumes_from_the_postgres_checkpoint(pool, redis_client, incident_id, database_url) -> None:
    counter = []
    crashing = FakeModel(tool_call("get_traces", service="payment"), Crash())
    with pytest.raises(Crash):
        investigator(pool, redis_client, crashing, checkpointer=create_checkpointer(database_url),
                     counter=counter).run(incident_id)
    assert stored(pool, incident_id)[0] is None

    # A new process: new checkpointer, new model client, same database.
    model = FakeModel(text("done"), report())  # cites E3, from the tool call made before the crash
    inv = investigator(pool, redis_client, model, checkpointer=create_checkpointer(database_url), counter=counter)
    inv.run(incident_id)

    assert counter == ["starter", "get_traces"]  # nothing done before the crash ran again
    assert len(model.calls) == 2
    assert stored(pool, incident_id)[0]["confidence"] == "high"
    inv.run(incident_id)  # a duplicate job for a finished investigation does nothing
    assert len(model.calls) == 2
