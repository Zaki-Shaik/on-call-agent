"""The report: what the model must produce, how code checks it, and the code-written fallback."""

from typing import Literal

from pydantic import BaseModel, Field

from oncall.profile import SystemProfile

Confidence = Literal["high", "medium", "low", "inconclusive"]
# Tools whose evidence can carry a conclusion: metrics and trace timing, not log text.
SUPPORTING_TOOLS = {"query_metrics", "get_traces", "get_service_map"}


class Cited(BaseModel):
    text: str
    evidence: list[str] = Field(description="Evidence ids (E1, E2, ...) that support this")


class TimelineEvent(BaseModel):
    time: str = Field(description="ISO 8601 UTC")
    event: str
    evidence: list[str]


class RuledOut(BaseModel):
    hypothesis: str
    reason: str
    evidence: list[str]


class ProposedFix(BaseModel):
    kind: Literal["set_flag", "report_only"] = Field(description="report_only: a written recommendation")
    flag: str | None = Field(description="set_flag only: a flag from the allowed list")
    value: str | None = Field(description="set_flag only: the value to set, from the allowed list")
    description: str = Field(description="What to do and why, for the engineer")


class Report(BaseModel):
    headline: str = Field(description="The cause, not the symptom: the alert already says what is failing")
    summary: str = Field(description="One sentence")
    root_cause_service: str | None = Field(description="Often not the alerting service; null only if inconclusive")
    confidence: Confidence
    root_cause: Cited = Field(description="The causal explanation, citing metric or trace evidence")
    started_at: str | None = Field(description="When the problem started, ISO 8601 UTC")
    duration_minutes: float | None = Field(description="How long it has lasted so far")
    correlated_change: Cited | None = Field(description="The change that caused it, if any")
    timeline: list[TimelineEvent]
    ruled_out: list[RuledOut] = Field(description="Hypotheses checked and why they were rejected")
    proposed_fix: ProposedFix


def cited_ids(report: Report) -> set[str]:
    ids = set(report.root_cause.evidence)
    if report.correlated_change:
        ids |= set(report.correlated_change.evidence)
    for item in [*report.timeline, *report.ruled_out]:
        ids |= set(item.evidence)
    return ids


def validate(report: Report, evidence_tools: dict[str, str]) -> list[str]:
    """Problems the model must repair. `evidence_tools` maps each evidence id in state to its tool."""
    problems = []
    unknown = sorted(cited_ids(report) - evidence_tools.keys(), key=lambda r: (len(r), r))
    if unknown:
        problems.append(f"these evidence ids do not exist: {', '.join(unknown)}. Cite only ids from tool results.")
    if report.confidence != "inconclusive":
        if not report.root_cause_service:
            problems.append("root_cause_service is required unless confidence is inconclusive")
        if not report.root_cause.evidence:
            problems.append("root_cause must cite evidence")
        elif not any(evidence_tools.get(ref) in SUPPORTING_TOOLS for ref in report.root_cause.evidence):
            problems.append("root_cause must cite at least one metric, trace or service-map finding;"
                            " log text alone cannot establish a cause")
    if report.correlated_change and not report.correlated_change.evidence:
        problems.append("correlated_change must cite the change's evidence")
    fix = report.proposed_fix
    if fix.kind == "set_flag" and not (fix.flag and fix.value):
        problems.append("a set_flag fix needs both flag and value")
    return problems


def action_for(report: Report, profile: SystemProfile, forced_stop: str | None) -> tuple[Report, dict | None]:
    """The flag change to put up for approval, if any. Code decides, never the model alone.

    A set_flag fix becomes a written recommendation when the flag/value is not allowed, the
    run was forced to stop, or confidence is low.
    """
    fix = report.proposed_fix
    if fix.kind != "set_flag":
        return report, None
    reason = None
    if forced_stop:
        reason = "the investigation stopped early"
    elif report.confidence not in ("high", "medium"):
        reason = f"confidence is {report.confidence}"
    elif not (profile.flags and profile.flags.is_allowed(fix.flag, fix.value)):
        reason = f"setting {fix.flag} to {fix.value} is not on the allowed list"
    if reason:
        downgraded = fix.model_copy(update={
            "kind": "report_only",
            "description": f"{fix.description} (Proposed setting {fix.flag} to {fix.value}; not actionable"
                           f" because {reason}.)"})
        return report.model_copy(update={"proposed_fix": downgraded}), None
    return report, {"flag": fix.flag, "value": fix.value}


def enforce_forced_stop(report: Report, forced_stop: str | None) -> Report:
    """A forced stop never claims more than low confidence."""
    if forced_stop and report.confidence in ("high", "medium"):
        return report.model_copy(update={"confidence": "low"})
    return report


def fallback_report(reason: str, alerting_services: list[str], evidence: list[dict],
                    leading_suspect: str | None) -> Report:
    """Written by code when the model can't write one (unreachable, or invalid after repairs)."""
    checked = sorted({e["tool"] for e in evidence})
    return Report(
        headline=f"Inconclusive: the investigation stopped early ({reason})",
        summary=f"Alerts on {', '.join(alerting_services) or 'unknown services'}; no cause established"
                f" because {reason}.",
        root_cause_service=leading_suspect,
        confidence="inconclusive",
        root_cause=Cited(text=f"Leading suspect: {leading_suspect}" if leading_suspect else "No leading suspect",
                         evidence=[]),
        started_at=None, duration_minutes=None, correlated_change=None, timeline=[],
        ruled_out=[RuledOut(hypothesis="(not evaluated)", reason=f"checked so far: {', '.join(checked) or 'nothing'}",
                            evidence=[])],
        proposed_fix=ProposedFix(kind="report_only", flag=None, value=None,
                                 description="Investigate manually, starting from the evidence collected."),
    )
