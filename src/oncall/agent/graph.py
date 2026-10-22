"""The investigation graph: a fixed workflow outside, an agent loop inside.

    build_context -> agent <-> tools
                       |        | (finished, or a budget ran out)
                       v        v
                    write_report <-> validate_report (repair) -> publish -> [action?] -> await_approval

State is checkpointed in Postgres after every node under thread_id = incident id, so a worker
that dies mid-investigation resumes from the last completed node.
"""

import json
import logging
import operator
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Annotated, Any, TypedDict
from uuid import UUID

import redis
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, AnyMessage, HumanMessage, SystemMessage, ToolMessage
from langgraph.errors import GraphRecursionError
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.types import Checkpointer
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from pydantic import ValidationError

from oncall.agent import prompts
from oncall.agent.context import LEAD, build_starter_pack
from oncall.agent.report import Report, action_for, enforce_forced_stop, fallback_report, validate
from oncall.agent.tools import TOOLS, ToolSpec, call_key, resolve_window, untrusted
from oncall.evidence import EvidenceStore
from oncall.links import Linker
from oncall.llm import Pricing, response_format
from oncall.profile import SystemProfile
from oncall.servicemap.store import ServiceMaps
from oncall.telemetry.factory import Backends
from oncall.telemetry.models import TimeWindow
from oncall.tools.base import ToolContext

log = logging.getLogger(__name__)

REPORTS_CHANNEL = "oncall:reports"  # pub/sub: the report page listens here (Phase 5)
TOOL_CACHE_TTL_SECONDS = 24 * 3600
RECURSION_LIMIT = 60  # a backstop only; the budgets stop runs long before this

# Why a run stopped early. The text goes into the prompt and the stored report.
TOOL_BUDGET = "tool-call budget used up"
TIME_BUDGET = "time budget used up"
COST_BUDGET = "cost budget used up"
REPEATS = "too many repeated tool calls"
NO_MODEL = "no model configured"
MODEL_ERROR = "the model failed"
INVALID_REPORT = "the report failed validation after repairs"
RECURSION = "step limit reached"
_NO_MODEL_STOPS = {NO_MODEL, MODEL_ERROR}


@dataclass(frozen=True)
class Budgets:
    max_tool_calls: int = 10
    max_active_seconds: float = 180
    max_cost_usd: float = 0.50
    max_repeats: int = 3
    max_report_attempts: int = 3  # the first draft plus two repairs


class InvestigationState(TypedDict, total=False):
    incident_id: str
    service: str
    alerting_services: list[str]
    opened_at: str
    window: dict[str, str]  # incident window; its end moves to "now" for each tool call
    starter_pack: dict[str, Any]
    messages: Annotated[list[AnyMessage], add_messages]
    evidence: Annotated[list[dict[str, str]], operator.add]  # {ref, tool, summary} of this run
    seen_calls: dict[str, int]  # repeat detection: call key -> times made
    tool_calls: int
    repeats: int
    llm_calls: int
    input_tokens: int
    output_tokens: int
    cost_usd: float
    active_seconds: float
    forced_stop: str | None
    report_draft: str | None
    report_problems: list[str]
    report_attempts: int
    report: dict[str, Any] | None
    proposed_action: dict[str, str] | None
    approval_status: str | None


class _RecordingEvidence:
    """Evidence store that remembers what it saved, so the refs can go into graph state."""

    def __init__(self, store: EvidenceStore) -> None:
        self._store = store
        self.saved: list[dict[str, str]] = []

    def save(self, incident_id: UUID, drafts) -> list[str]:
        refs = self._store.save(incident_id, drafts)
        self.saved += [{"ref": r, "tool": d.tool, "summary": d.summary} for r, d in zip(refs, drafts)]
        return refs

    def get(self, incident_id: UUID, ref: str):
        return self._store.get(incident_id, ref)


def _timed(node: Callable[["Investigator", InvestigationState], dict]) -> Callable:
    """Add the node's run time to active_seconds: the time budget counts work, not downtime."""

    def wrapper(self: "Investigator", state: InvestigationState) -> dict:
        started = time.monotonic()
        update = node(self, state)
        update["active_seconds"] = state.get("active_seconds", 0.0) + time.monotonic() - started
        return update

    wrapper.__name__ = node.__name__
    return wrapper


class Investigator:
    def __init__(self, *, profile: SystemProfile, backends: Backends, links: Linker, pool: ConnectionPool,
                 redis_client: redis.Redis, maps: ServiceMaps | None, model: BaseChatModel | None,
                 pricing: Pricing, budgets: Budgets, checkpointer: Checkpointer,
                 tools: dict[str, ToolSpec] = TOOLS, starter=build_starter_pack,
                 clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> None:
        self._profile = profile
        self._backends = backends
        self._links = links
        self._pool = pool
        self._redis = redis_client
        self._maps = maps
        self._model = model
        self._pricing = pricing
        self._budgets = budgets
        self._tools = tools
        self._starter = starter
        self._clock = clock
        self.graph = self._build().compile(checkpointer=checkpointer)

    # Running

    def run(self, incident_id: UUID) -> None:
        """Investigate, or resume an investigation a dead worker left behind."""
        config = {"configurable": {"thread_id": str(incident_id)}, "recursion_limit": RECURSION_LIMIT}
        snapshot = self.graph.get_state(config)
        if snapshot.values and not snapshot.next:
            log.info("incident %s: investigation already finished", incident_id)
            return
        inputs = None if snapshot.next else {"incident_id": str(incident_id)}
        if inputs is None:
            log.warning("incident %s: resuming the investigation at %s", incident_id, ", ".join(snapshot.next))
        try:
            self.graph.invoke(inputs, config)
        except GraphRecursionError:
            # Backstop: end like any other forced stop, with a partial report.
            log.error("incident %s: step limit reached; writing a partial report", incident_id)
            self.graph.update_state(config, {"forced_stop": RECURSION}, as_node="tools")
            self.graph.invoke(None, config)

    def _build(self) -> StateGraph:
        g = StateGraph(InvestigationState)
        g.add_node("build_context", self._build_context)
        g.add_node("agent", self._agent)
        g.add_node("tools", self._run_tools)
        g.add_node("write_report", self._write_report)
        g.add_node("validate_report", self._validate_report)
        g.add_node("publish", self._publish)
        g.add_node("await_approval", self._await_approval)
        g.add_edge(START, "build_context")
        g.add_edge("build_context", "agent")
        g.add_conditional_edges("agent", self._after_agent, ["tools", "write_report"])
        g.add_conditional_edges("tools", self._after_tools, ["agent", "write_report"])
        g.add_edge("write_report", "validate_report")
        g.add_conditional_edges("validate_report", self._after_validate, ["write_report", "publish"])
        g.add_conditional_edges("publish", self._after_publish, ["await_approval", END])
        g.add_edge("await_approval", END)
        return g

    def _ctx(self, incident_id: str) -> tuple[ToolContext, _RecordingEvidence]:
        recorder = _RecordingEvidence(EvidenceStore(self._pool))
        ctx = ToolContext(self._profile, self._backends, self._links, recorder,  # type: ignore[arg-type]
                          self._pool, UUID(incident_id), self._maps)
        return ctx, recorder

    def _incident_window(self, state: InvestigationState) -> TimeWindow:
        """The incident window, extended to now: the incident is still going on."""
        return TimeWindow(start=datetime.fromisoformat(state["window"]["start"]), end=self._clock())

    def _exhausted(self, state: dict) -> str | None:
        b = self._budgets
        if state.get("tool_calls", 0) >= b.max_tool_calls:
            return TOOL_BUDGET
        if state.get("repeats", 0) >= b.max_repeats:
            return REPEATS
        if state.get("cost_usd", 0.0) >= b.max_cost_usd:
            return COST_BUDGET
        if state.get("active_seconds", 0.0) >= b.max_active_seconds:
            return TIME_BUDGET
        return None

    # Nodes

    @_timed
    def _build_context(self, state: InvestigationState) -> dict:
        incident_id = state["incident_id"]
        with self._pool.connection() as conn:
            incident = conn.execute("SELECT * FROM incidents WHERE id = %s", (incident_id,)).fetchone()
            alerts = conn.execute("SELECT * FROM alerts WHERE incident_id = %s ORDER BY starts_at, id",
                                  (incident_id,)).fetchall()
        if incident is None:
            raise ValueError(f"incident {incident_id} does not exist")
        window = TimeWindow(start=incident["opened_at"] - LEAD, end=self._clock())
        ctx, recorder = self._ctx(incident_id)
        pack = self._starter(ctx, incident, alerts, window)
        services = list(dict.fromkeys(a["service"] for a in alerts if a["service"])) or [incident["service"]]
        intro = (f"Incident opened at {incident['opened_at'].isoformat()} with alerts on {', '.join(services)}."
                 f" Incident window: {window.start.isoformat()} to now. Starter pack:")
        return {
            "service": incident["service"], "alerting_services": services,
            "opened_at": incident["opened_at"].isoformat(), "window": window.model_dump(mode="json"),
            "starter_pack": pack,
            "messages": [SystemMessage(prompts.system_prompt(self._profile, self._budgets.max_tool_calls)),
                         HumanMessage(f"{intro}\n{untrusted('starter_pack', json.dumps(pack))}")],
            "evidence": recorder.saved,
            "seen_calls": {}, "tool_calls": 0, "repeats": 0, "llm_calls": 0, "input_tokens": 0,
            "output_tokens": 0, "cost_usd": 0.0, "forced_stop": None, "report_attempts": 0,
        }

    def _usage(self, state: InvestigationState, reply: AIMessage) -> dict:
        tokens_in, tokens_out, cost = self._pricing.cost(reply)
        return {"llm_calls": state.get("llm_calls", 0) + 1,
                "input_tokens": state.get("input_tokens", 0) + tokens_in,
                "output_tokens": state.get("output_tokens", 0) + tokens_out,
                "cost_usd": state.get("cost_usd", 0.0) + cost}

    @_timed
    def _agent(self, state: InvestigationState) -> dict:
        if self._model is None:
            return {"forced_stop": NO_MODEL}
        if stop := self._exhausted(state):
            return {"forced_stop": stop}
        try:
            model = self._model.bind_tools([t.definition() for t in self._tools.values()], parallel_tool_calls=False)
            reply = model.invoke(state["messages"])
        except Exception:
            # The client already retried with backoff; what reaches here keeps failing.
            log.exception("incident %s: model call failed", state["incident_id"])
            return {"forced_stop": MODEL_ERROR}
        update = self._usage(state, reply) | {"messages": [reply]}
        if (stop := self._exhausted(update)) and _pending_calls(reply):
            # Every tool call needs an answer before the next model call.
            update["messages"] = [reply, *(ToolMessage(f"not run: {stop}", tool_call_id=i)
                                           for i in _pending_calls(reply))]
            update["forced_stop"] = stop
        return update

    @staticmethod
    def _after_agent(state: InvestigationState) -> str:
        last = state["messages"][-1]
        if state.get("forced_stop") or not (isinstance(last, AIMessage) and _pending_calls(last)):
            return "write_report"
        return "tools"

    @_timed
    def _run_tools(self, state: InvestigationState) -> dict:
        last: AIMessage = state["messages"][-1]  # type: ignore[assignment]
        counters = {"tool_calls": state.get("tool_calls", 0), "repeats": state.get("repeats", 0)}
        seen = dict(state.get("seen_calls", {}))
        messages, evidence = [], []
        for bad in last.invalid_tool_calls:
            counters["tool_calls"] += 1
            messages.append(ToolMessage(f"error: arguments were not valid JSON: {bad.get('error')}",
                                        tool_call_id=bad["id"]))
        for call in last.tool_calls:
            content, saved = self._run_tool(state, call, seen, counters)
            messages.append(ToolMessage(content, tool_call_id=call["id"], name=call["name"]))
            evidence += saved
        update = {"messages": messages, "evidence": evidence, "seen_calls": seen, **counters}
        update["forced_stop"] = self._exhausted(state | update)
        return update

    def _run_tool(self, state: InvestigationState, call: dict, seen: dict[str, int],
                  counters: dict[str, int]) -> tuple[str, list[dict]]:
        spec = self._tools.get(call["name"])
        if spec is None:
            counters["tool_calls"] += 1
            return f"error: unknown tool {call['name']!r}", []
        try:
            args = spec.args.model_validate(call["args"])
            window = resolve_window(getattr(args, "window", None), self._incident_window(state), self._clock())
        except (ValidationError, ValueError) as e:
            counters["tool_calls"] += 1
            return f"error: invalid arguments: {e}", []
        key = call_key(spec.name, args, window)
        cache_key = f"oncall:toolcache:{state['incident_id']}:{key}"
        if key in seen:
            seen[key] += 1
            counters["repeats"] += 1
            earlier = self._cache_get(cache_key)
            return ("Repeat of an earlier identical call; the result is unchanged. Try a different call."
                    + (f"\n{earlier}" if earlier else " See the earlier result above.")), []
        seen[key] = 1
        counters["tool_calls"] += 1
        ctx, recorder = self._ctx(state["incident_id"])
        try:
            result = spec.run(ctx, args, window)
        except Exception as e:
            log.exception("incident %s: tool %s failed", state["incident_id"], spec.name)
            return f"error: {spec.name} failed: {type(e).__name__}", recorder.saved
        content = untrusted(spec.name, result.model_dump_json(exclude_none=True))
        self._cache_set(cache_key, content)
        return content, recorder.saved

    def _cache_get(self, key: str) -> str | None:
        try:
            value = self._redis.get(key)
        except redis.RedisError:
            return None
        return value.decode() if isinstance(value, bytes) else value

    def _cache_set(self, key: str, value: str) -> None:
        try:
            self._redis.set(key, value, ex=TOOL_CACHE_TTL_SECONDS)
        except redis.RedisError:
            log.warning("tool cache unavailable; repeats will point at the earlier message instead")

    @staticmethod
    def _after_tools(state: InvestigationState) -> str:
        return "write_report" if state.get("forced_stop") else "agent"

    @_timed
    def _write_report(self, state: InvestigationState) -> dict:
        attempts = state.get("report_attempts", 0) + 1
        forced = state.get("forced_stop")
        if self._model is None or forced in _NO_MODEL_STOPS:
            return {"report_attempts": attempts, "report_draft": None}
        instruction = prompts.REPORT + (f"\n\n{prompts.FORCED.format(reason=forced)}" if forced else "")
        messages = [*state["messages"], HumanMessage(instruction)]
        if state.get("report_problems") and state.get("report_draft"):
            messages += [AIMessage(state["report_draft"]),
                         HumanMessage(prompts.REPAIR.format(problems="\n".join(f"- {p}" for p in state["report_problems"])))]
        try:
            # Tools stay declared (the history contains tool calls) but can't be called now.
            model = self._model.bind_tools([t.definition() for t in self._tools.values()], tool_choice="none")
            reply = model.bind(response_format=response_format("incident_report", Report)).invoke(messages)
        except Exception:
            log.exception("incident %s: report call failed", state["incident_id"])
            return {"report_attempts": attempts, "report_draft": None, "forced_stop": MODEL_ERROR}
        return self._usage(state, reply) | {"report_attempts": attempts, "report_draft": _text(reply)}

    @_timed
    def _validate_report(self, state: InvestigationState) -> dict:
        forced = state.get("forced_stop")
        evidence_tools = {e["ref"]: e["tool"] for e in state.get("evidence", [])}
        draft = state.get("report_draft")
        report, problems = None, []
        if draft is not None:
            try:
                report = Report.model_validate_json(draft)
                problems = validate(report, evidence_tools)
            except ValidationError as e:
                problems = [f"the JSON does not match the schema: {e}"]
        if draft is not None and problems and state.get("report_attempts", 0) < self._budgets.max_report_attempts:
            return {"report_problems": problems}
        if report is None or problems:
            forced = forced if draft is None else INVALID_REPORT
            if problems:
                log.warning("incident %s: report still invalid after repairs: %s", state["incident_id"], problems)
            report = fallback_report(forced or MODEL_ERROR, state.get("alerting_services", []),
                                     state.get("evidence", []), _leading_suspect(state))
        report, action = action_for(enforce_forced_stop(report, forced), self._profile, forced)
        return {"report": report.model_dump(mode="json"), "proposed_action": action, "report_problems": [],
                "forced_stop": forced}

    @staticmethod
    def _after_validate(state: InvestigationState) -> str:
        return "write_report" if state.get("report_problems") else "publish"

    @_timed
    def _publish(self, state: InvestigationState) -> dict:
        incident_id = state["incident_id"]
        report = state["report"]
        action = state.get("proposed_action")
        now = self._clock()
        status = "awaiting_approval" if action else "reported"
        with self._pool.connection() as conn, conn.transaction():
            # Upsert: if the worker dies after this commit but before the checkpoint, the rerun
            # overwrites the same row.
            conn.execute(
                """
                INSERT INTO reports (incident_id, report, confidence, forced_stop, proposed_action, tool_calls,
                                     llm_calls, input_tokens, output_tokens, cost_usd, active_seconds,
                                     time_to_report_seconds)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (incident_id) DO UPDATE SET
                    report = EXCLUDED.report, confidence = EXCLUDED.confidence,
                    forced_stop = EXCLUDED.forced_stop, proposed_action = EXCLUDED.proposed_action,
                    tool_calls = EXCLUDED.tool_calls, llm_calls = EXCLUDED.llm_calls,
                    input_tokens = EXCLUDED.input_tokens, output_tokens = EXCLUDED.output_tokens,
                    cost_usd = EXCLUDED.cost_usd, active_seconds = EXCLUDED.active_seconds,
                    time_to_report_seconds = EXCLUDED.time_to_report_seconds, updated_at = now()
                """,
                (incident_id, Jsonb(report), report["confidence"], state.get("forced_stop"),
                 Jsonb(action) if action else None, state.get("tool_calls", 0), state.get("llm_calls", 0),
                 state.get("input_tokens", 0), state.get("output_tokens", 0), state.get("cost_usd", 0.0),
                 state.get("active_seconds", 0.0),
                 (now - datetime.fromisoformat(state["opened_at"])).total_seconds()),
            )
            conn.execute("UPDATE incidents SET status = %s, updated_at = now() WHERE id = %s",
                         (status, incident_id))
        try:
            self._redis.publish(REPORTS_CHANNEL, json.dumps({"incident_id": incident_id, "status": status}))
        except redis.RedisError:
            log.warning("incident %s: could not push the report update; the page will show it on reload",
                        incident_id)
        log.info("incident %s: report published (%s, confidence %s, %d tool calls, $%.4f)", incident_id,
                 status, report["confidence"], state.get("tool_calls", 0), state.get("cost_usd", 0.0))
        return {}

    @staticmethod
    def _after_publish(state: InvestigationState) -> str:
        return "await_approval" if state.get("proposed_action") else END

    def _await_approval(self, state: InvestigationState) -> dict:
        # Phase 5 replaces this with interrupt(): the run pauses here until a human decides.
        return {"approval_status": "pending"}


def _pending_calls(message: AIMessage) -> list[str]:
    return [c["id"] for c in message.tool_calls] + [c["id"] for c in message.invalid_tool_calls if c.get("id")]


def _text(message: AIMessage) -> str:
    if isinstance(message.content, str):
        return message.content
    return "".join(part.get("text", "") for part in message.content if isinstance(part, dict))


def _leading_suspect(state: InvestigationState) -> str | None:
    """For a code-written report: the callee of the strongest failing edge in the starter map.

    Errors beat missing calls beat latency; a merely new edge is not a failure.
    """
    edges = (state.get("starter_pack") or {}).get("service_map", {}).get("edges", [])
    for change in ("errors_up", "missing", "slower", "lag_up"):
        for e in edges:
            if change in e.get("changes", []):
                return e["source"] if e.get("type") == "publish" else e["target"]
    return state.get("service")
