"""Print a service map as a text table or as a Mermaid diagram (renders on GitHub)."""

import re

from oncall.servicemap.model import EdgeType, NodeType, ServiceMap

_ARROWS = {EdgeType.CALL: "-->", EdgeType.PUBLISH: "-.->", EdgeType.CONSUME: "-.->"}


def as_text(m: ServiceMap) -> str:
    width = max((len(e.source) + len(e.target) for e in m.edges), default=10) + 4
    lines = [f"{'edge':<{width}} {'type':<8} {'calls':>6} {'errors':>6} {'p50 ms':>8} {'p95 ms':>8}  lag p95 ms"]
    for e in m.edges:
        lag = f"{e.lag_p95_ms:>10}" if e.lag_p95_ms is not None else ""
        lines.append(f"{e.source + ' -> ' + e.target:<{width}} {e.type:<8} {e.calls:>6} {e.errors:>6}"
                     f" {e.p50_ms:>8} {e.p95_ms:>8}  {lag}".rstrip())
    externals = [n.name for n in m.nodes if n.type is NodeType.EXTERNAL]
    topics = [n.name for n in m.nodes if n.type is NodeType.TOPIC]
    lines.append("")
    lines.append(f"services: {', '.join(sorted(m.services()))}")
    lines.append(f"untraced peers: {', '.join(externals) or '-'}; topics: {', '.join(topics) or '-'}")
    return "\n".join(lines)


def _id(name: str) -> str:
    return "n_" + re.sub(r"\W", "_", name)


def as_mermaid(m: ServiceMap) -> str:
    shapes = {NodeType.SERVICE: '["{}"]', NodeType.EXTERNAL: '[("{}")]', NodeType.TOPIC: '[/"{}"/]'}
    lines = ["graph LR"]
    lines += [f"  {_id(n.name)}{shapes[n.type].format(n.name)}" for n in m.nodes]
    for e in m.edges:
        label = f"|{e.errors}/{e.calls} err|" if e.errors else ""
        lines.append(f"  {_id(e.source)} {_ARROWS[e.type]}{label} {_id(e.target)}")
    return "\n".join(lines)
