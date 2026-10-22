"""Call the agent's tools by hand against a live incident.

    python -m oncall.cli incidents
    python -m oncall.cli metrics  <incident> --service payment [--signals error_rate,latency_p95]
    python -m oncall.cli traces   <incident> --service checkout [--operation X] [--errors-only]
    python -m oncall.cli logs     <incident> --service payment [--min-severity error] [--text "..."]
    python -m oncall.cli changes  <incident> [--services a,b]
    python -m oncall.cli evidence <incident> [E3]

The window defaults to 15 minutes before the incident opened until now (at most 1 hour
after it opened); override with --start/--end (ISO 8601). Results print as the model would
receive them; evidence is saved to the incident like a real investigation.
"""

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from uuid import UUID

from oncall.config import Settings
from oncall.db import create_pool
from oncall.evidence import EvidenceStore
from oncall.links import Linker
from oncall.profile import load_profile
from oncall.telemetry.factory import build_backends
from oncall.telemetry.models import Signal, TimeWindow
from oncall.tools.base import ToolContext
from oncall.tools.changes import list_recent_changes
from oncall.tools.logs import search_logs
from oncall.tools.metrics import query_metrics
from oncall.tools.traces import get_traces


def _window(args, opened_at: datetime) -> TimeWindow:
    now = datetime.now(timezone.utc)
    start = datetime.fromisoformat(args.start) if args.start else opened_at - timedelta(minutes=15)
    end = datetime.fromisoformat(args.end) if args.end else min(now, opened_at + timedelta(hours=1))
    return TimeWindow(start=start, end=end)


def _print(model) -> None:
    print(model.model_dump_json(indent=2, exclude_none=True))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="oncall.cli", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("incidents", help="list recent incidents")
    for name in ("metrics", "traces", "logs", "changes", "evidence"):
        p = sub.add_parser(name)
        p.add_argument("incident", type=UUID)
        if name == "evidence":
            p.add_argument("ref", nargs="?")
            continue
        p.add_argument("--start")
        p.add_argument("--end")
        if name != "changes":
            p.add_argument("--service", required=True)
    sub.choices["metrics"].add_argument("--signals", help=f"comma-separated: {', '.join(s.value for s in Signal)}")
    sub.choices["traces"].add_argument("--operation")
    sub.choices["traces"].add_argument("--errors-only", action="store_true")
    sub.choices["logs"].add_argument("--min-severity", choices=["debug", "info", "warn", "error", "fatal"])
    sub.choices["logs"].add_argument("--text")
    sub.choices["changes"].add_argument("--services")
    args = parser.parse_args(argv)

    settings = Settings()
    profile = load_profile(settings.profile_path)
    pool = create_pool(settings.database_url)
    try:
        if args.command == "incidents":
            with pool.connection() as conn:
                for r in conn.execute("SELECT id, status, service, opened_at FROM incidents"
                                      " ORDER BY opened_at DESC LIMIT 20"):
                    print(f"{r['id']}  {r['status']:<18} {r['service']:<20} {r['opened_at']:%Y-%m-%d %H:%M:%SZ}")
            return 0

        with pool.connection() as conn:
            incident = conn.execute("SELECT * FROM incidents WHERE id = %s", (args.incident,)).fetchone()
        if incident is None:
            print(f"no incident {args.incident}", file=sys.stderr)
            return 1
        evidence = EvidenceStore(pool)

        if args.command == "evidence":
            with pool.connection() as conn:
                rows = conn.execute(
                    "SELECT ref, tool, summary, link FROM evidence WHERE incident_id = %s"
                    " AND (%s::text IS NULL OR ref = %s) ORDER BY id",
                    (args.incident, args.ref, args.ref),
                ).fetchall()
            for r in rows:
                print(json.dumps(r, default=str, indent=2) if args.ref else f"{r['ref']:<5} {r['tool']:<20} {r['summary']}")
            return 0

        ctx = ToolContext(profile, build_backends(profile), Linker(profile.links), evidence, pool, args.incident)
        window = _window(args, incident["opened_at"])
        if args.command == "metrics":
            signals = [Signal(s.strip()) for s in args.signals.split(",")] if args.signals else None
            _print(query_metrics(ctx, args.service, window, signals))
        elif args.command == "traces":
            _print(get_traces(ctx, args.service, window, args.operation, args.errors_only))
        elif args.command == "logs":
            _print(search_logs(ctx, args.service, window, args.min_severity, args.text))
        elif args.command == "changes":
            services = [s.strip() for s in args.services.split(",")] if args.services else None
            _print(list_recent_changes(ctx, window, services))
        return 0
    finally:
        pool.close()


if __name__ == "__main__":
    sys.exit(main())
