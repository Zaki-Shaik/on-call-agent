from datetime import datetime, timedelta, timezone

from oncall.telemetry.models import TimeWindow
from oncall.tools.metrics import analyze

T0 = datetime(2026, 10, 22, 3, 0, tzinfo=timezone.utc)
STEP = timedelta(seconds=30)
BASELINE = TimeWindow(start=T0 - timedelta(hours=1), end=T0)
WINDOW = TimeWindow(start=T0, end=T0 + timedelta(minutes=20))


def series(base_values, window_values):
    """Baseline points (ending at T0) followed by window points (starting at T0)."""
    base = [(T0 - STEP * (len(base_values) - i), v) for i, v in enumerate(base_values)]
    win = [(T0 + STEP * i, v) for i, v in enumerate(window_values)]
    return base + win


def test_flat_series_has_no_onset() -> None:
    a = analyze(series([10.0, 10.2, 9.9, 10.1] * 30, [10.0, 10.1, 9.9] * 10), WINDOW, BASELINE)
    assert a.onset is None
    assert a.direction == "flat"
    assert abs(a.change_pct) < 5


def test_step_change_reports_onset_and_change() -> None:
    a = analyze(series([100.0, 102.0, 98.0, 101.0] * 30, [100.0] * 6 + [300.0] * 20), WINDOW, BASELINE)
    assert a.onset == T0 + STEP * 6
    assert a.direction == "up"
    assert a.baseline == 100.5  # median of the baseline values
    assert a.current == 300.0
    assert round(a.change_pct, 1) == 198.5


def test_rise_from_zero_baseline() -> None:
    # Error rate: 0 for an hour, then failures. No percentage change from zero, but an onset.
    a = analyze(series([0.0] * 120, [0.0] * 4 + [0.45, 0.5, 0.48, 0.52] * 4), WINDOW, BASELINE)
    assert a.baseline == 0.0
    assert a.change_pct is None
    assert a.onset == T0 + STEP * 4
    assert a.direction == "up"
    assert a.robust_z is None  # meaningless against a perfectly flat zero baseline


def test_drop_is_reported_as_down() -> None:
    a = analyze(series([5.0, 5.2, 4.8] * 40, [5.0] * 4 + [0.2] * 10), WINDOW, BASELINE)
    assert a.direction == "down"
    assert a.onset == T0 + STEP * 4


def test_single_spike_is_not_an_onset() -> None:
    a = analyze(series([10.0, 10.5, 9.5] * 40, [10.0] * 5 + [50.0] + [10.0] * 10), WINDOW, BASELINE)
    assert a.onset is None


def test_deviation_only_at_the_very_end_is_not_confirmed() -> None:
    a = analyze(series([10.0] * 120, [10.0] * 10 + [50.0, 50.0]), WINDOW, BASELINE)
    assert a.onset is None


def test_no_points_in_window_is_no_data_not_zero() -> None:
    a = analyze(series([10.0] * 120, []), WINDOW, BASELINE)
    assert a.points_in_window == 0
    assert a.current is None and a.baseline is None


def test_missing_baseline() -> None:
    a = analyze(series([], [10.0] * 10), WINDOW, BASELINE)
    assert a.baseline is None
    assert a.current == 10.0
    assert a.onset is None and a.change_pct is None and a.direction is None
