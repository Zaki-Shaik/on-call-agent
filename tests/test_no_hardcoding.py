"""Guard for the project rule: nothing system-specific in code.

Flag names and service names from every shipped profile must not appear in src/.
Flag names are distinctive, so any occurrence fails. Service names are often plain
words ("cart", "email"), so only quoted string literals are checked.
"""

import re
from pathlib import Path

from oncall.profile import load_profile

ROOT = Path(__file__).resolve().parents[1]


def _source_files() -> list[Path]:
    return sorted((ROOT / "src").rglob("*.py"))


def _profiles():
    return [load_profile(p, env={}) for p in sorted((ROOT / "profiles").glob("*.yaml"))]


def test_no_profile_flag_names_in_source() -> None:
    names = {f.name for p in _profiles() if p.flags for f in p.flags.allowed}
    offenders = [
        f"{path.relative_to(ROOT)}: {name}"
        for path in _source_files()
        for name in names
        if name in path.read_text(encoding="utf-8")
    ]
    assert not offenders, "system-specific flag names in code:\n" + "\n".join(offenders)


def test_no_profile_service_names_as_literals_in_source() -> None:
    services = {s for p in _profiles() for s in p.owners}
    offenders = []
    for path in _source_files():
        text = path.read_text(encoding="utf-8")
        for service in services:
            if re.search(rf"""["']{re.escape(service)}["']""", text):
                offenders.append(f"{path.relative_to(ROOT)}: {service}")
    assert not offenders, "system-specific service names in code:\n" + "\n".join(offenders)
