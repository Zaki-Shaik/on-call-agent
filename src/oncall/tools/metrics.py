"""query_metrics: golden signals for one service, each compared with the service's own baseline.

No fixed "bad" thresholds: the tool reports baseline, current value, the change and when it
started; the model judges how bad that is. The onset detector does use a statistical
threshold (3 robust standard deviations), which says "this departs from normal", not "this
is bad".
"""

from datetime import datetime, timedelta
from statistics import median

from pydantic import BaseModel

from oncall.evidence import EvidenceDraft
from oncall.telemetry.models import SIGNAL_UNITS, BackendError, Series, Signal, TimeWindow
from oncall.tools.base import Status, ToolContext, sig

BASELINE = timedelta(hours=1)
CURRENT = timedelta(minutes=5)  # "current" = recent part of the window when there is no onset
ONSET_RUN = 3  # consecutive deviating points needed to call an onset
ONSET_SIGMAS = 3.0
MIN_RELATIVE_NOISE = 0.05  # treat changes under 5% of baseline as noise, even if the series is flat


class SignalResult(BaseModel):
    signal: Signal
    unit: str
    status: Status
    baseline: float | None = None
    current: float | None = None
    change_pct: float | None = None
    direction: str | None = None  # up, down, flat
    robust_z: float | None = None
    onset: datetime | None = None
    evidence: str | None = None
    note: str | None = None


class MetricsResult(BaseModel):
    tool: str = "query_metrics"
    service: str
    window: TimeWindow
    baseline_window: TimeWindow
    signals: list[SignalResult]


class Analysis(BaseModel):
    baseline: float | None
    current: float | None
    change_pct: float | None
    direction: str | None
    robust_z: float | None
    onset: datetime | None
    points_in_window: int


def analyze(series: Series, window: TimeWindow, baseline_window: TimeWindow) -> Analysis:
    base = [v for t, v in series if baseline_window.start <= t < baseline_window.end]
    win = [(t, v) for t, v in series if window.start <= t <= window.end]
    if not win:
        return Analysis(baseline=None, current=None, change_pct=None, direction=None,
                        robust_z=None, onset=None, points_in_window=0)

    baseline = median(base) if base else None
    onset = None
    sigma = noise = 0.0
    if baseline is not None:
        sigma = 1.4826 * median(abs(v - baseline) for v in base)  # robust (MAD-based) std dev
        noise = max(sigma, MIN_RELATIVE_NOISE * abs(baseline), 1e-9)
        deviating = [abs(v - baseline) > ONSET_SIGMAS * noise for _, v in win]
        # First point that starts a run of deviating points. A deviation only in the last
        # point or two is not confirmed yet.
        run = min(ONSET_RUN, len(win))
        for i in range(len(win) - run + 1):
            if all(deviating[i:i + run]):
                onset = win[i][0]
                break

    recent = [v for t, v in win if t >= (onset or window.end - CURRENT)]
    current = median(recent or [win[-1][1]])

    change_pct = robust_z = direction = None
    if baseline is not None:
        if baseline != 0:
            change_pct = (current - baseline) / abs(baseline) * 100
        if baseline != 0 or sigma > 0:
            # Against a perfectly flat zero baseline a z-score is meaningless (any value is
            # "infinitely" unusual); "baseline 0 -> current x" says it better.
            robust_z = (current - baseline) / noise
        direction = "flat" if abs(current - baseline) <= ONSET_SIGMAS * noise else (
            "up" if current > baseline else "down")
    return Analysis(baseline=baseline, current=current, change_pct=change_pct, direction=direction,
                    robust_z=robust_z, onset=onset, points_in_window=len(win))


def _round1(value: float | None) -> float | None:
    return None if value is None else round(value, 1) + 0.0  # + 0.0 turns -0.0 into 0.0


def _step_seconds(span: timedelta) -> int:
    # About 300 points over baseline + window, never finer than 30 s.
    return max(30, int(span.total_seconds() / 300) // 15 * 15)


def query_metrics(ctx: ToolContext, service: str, window: TimeWindow,
                  signals: list[Signal] | None = None) -> MetricsResult:
    signals = signals or list(Signal)
    baseline_window = TimeWindow(start=window.start - BASELINE, end=window.start)
    step = _step_seconds(window.end - baseline_window.start)
    results: list[SignalResult] = []
    drafts: list[tuple[int, EvidenceDraft]] = []

    for signal in signals:
        unit = SIGNAL_UNITS[signal]
        try:
            series = ctx.backends.metrics.series(service, signal, baseline_window.start, window.end, step)
        except BackendError as e:
            results.append(SignalResult(signal=signal, unit=unit, status=Status.ERROR, note=str(e)))
            continue

        a = analyze(series, window, baseline_window)
        link = ctx.links.metrics(ctx.backends.metrics.query_text(service, signal),
                                 baseline_window.start, window.end)
        inputs = {"service": service, "signal": signal.value, "window": window.model_dump(mode="json")}
        if a.points_in_window == 0:
            results.append(SignalResult(signal=signal, unit=unit, status=Status.NO_DATA))
            drafts.append((len(results) - 1, EvidenceDraft(
                "query_metrics", inputs, f"{service} {signal.value}: no data in window", link)))
            continue

        r = SignalResult(
            signal=signal, unit=unit, status=Status.OK, baseline=sig(a.baseline), current=sig(a.current),
            change_pct=_round1(a.change_pct), direction=a.direction, robust_z=_round1(a.robust_z), onset=a.onset,
            note=None if a.baseline is not None else "no baseline data before the window",
        )
        results.append(r)
        onset = f", onset {a.onset:%H:%M:%SZ}" if a.onset else ""
        drafts.append((len(results) - 1, EvidenceDraft(
            "query_metrics", inputs,
            f"{service} {signal.value}: baseline {r.baseline} -> current {r.current} {unit}{onset}",
            link, r.model_dump(mode="json"))))

    refs = ctx.save([d for _, d in drafts])
    for (index, _), ref in zip(drafts, refs):
        results[index].evidence = ref
    return MetricsResult(service=service, window=window, baseline_window=baseline_window, signals=results)
