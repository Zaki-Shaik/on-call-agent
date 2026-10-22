"""System profile: everything system-specific the agent needs, loaded from one YAML file.

Code must never hardcode service, flag or metric names of a monitored system. Those live
here, so adding a new system means adding a new profile file, not changing code.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


class ProfileError(Exception):
    """The profile file is missing, malformed, or fails validation."""


class _Strict(BaseModel):
    # Reject unknown keys so a typo in a profile fails loudly instead of being ignored.
    model_config = ConfigDict(extra="forbid", frozen=True)


class Backend(_Strict):
    """One telemetry backend. `type` selects the adapter; `options` are adapter-specific."""

    type: str
    url: str
    timeout_seconds: float = Field(default=10.0, gt=0)
    options: dict[str, Any] = Field(default_factory=dict)


class Backends(_Strict):
    metrics: Backend
    traces: Backend
    logs: Backend | None = None


class Links(_Strict):
    """Browser-facing base URLs used to build evidence deep links (not the API endpoints)."""

    grafana: str
    jaeger: str | None = None
    # Grafana datasource UIDs by role (metrics, traces, logs), for Explore links.
    grafana_datasources: dict[str, str] = Field(default_factory=dict)


class AlertMapping(_Strict):
    # Alert label that names the affected service.
    service_label: str
    severity_label: str = "severity"


class FlagProvider(_Strict):
    type: str
    url: str
    timeout_seconds: float = Field(default=5.0, gt=0)
    options: dict[str, Any] = Field(default_factory=dict)


class AllowedFlag(_Strict):
    name: str
    # Variants the agent may propose. None means any variant the provider defines.
    values: list[str] | None = None


class Flags(_Strict):
    provider: FlagProvider
    # The only flags the agent may ever propose setting.
    allowed: list[AllowedFlag] = Field(default_factory=list)

    @model_validator(mode="after")
    def _unique_names(self) -> Flags:
        names = [f.name for f in self.allowed]
        duplicates = sorted({n for n in names if names.count(n) > 1})
        if duplicates:
            raise ValueError(f"duplicate allowed flags: {', '.join(duplicates)}")
        return self

    def is_allowed(self, name: str, value: str) -> bool:
        for flag in self.allowed:
            if flag.name == name:
                return flag.values is None or value in flag.values
        return False


class SystemProfile(_Strict):
    name: str
    description: str = ""
    backends: Backends
    links: Links
    alerts: AlertMapping
    # Optional: a system without feature flags gets report-only recommendations.
    flags: Flags | None = None
    # Optional metric name overrides, keyed by signal. Interpreted by the metrics adapter.
    metric_overrides: dict[str, str] = Field(default_factory=dict)
    runbooks: str | None = None
    owners: dict[str, str] = Field(default_factory=dict)


# ${VAR} or ${VAR:-default}
_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _expand_env(value: Any, env: dict[str, str]) -> Any:
    """Substitute ${VAR} references in string values, so secrets stay out of profile files."""
    if isinstance(value, str):

        def replace(match: re.Match[str]) -> str:
            name, default = match.group(1), match.group(2)
            if name in env:
                return env[name]
            if default is not None:
                return default
            raise ProfileError(f"environment variable {name} is not set")

        return _ENV_REF.sub(replace, value)
    if isinstance(value, dict):
        return {k: _expand_env(v, env) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v, env) for v in value]
    return value


def load_profile(path: str | Path, env: dict[str, str] | None = None) -> SystemProfile:
    path = Path(path)
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as e:
        raise ProfileError(f"cannot read profile {path}: {e}") from e
    except yaml.YAMLError as e:
        raise ProfileError(f"invalid YAML in profile {path}: {e}") from e
    if not isinstance(raw, dict):
        raise ProfileError(f"profile {path} must be a YAML mapping")

    expanded = _expand_env(raw, dict(os.environ) if env is None else env)
    try:
        return SystemProfile.model_validate(expanded)
    except ValidationError as e:
        raise ProfileError(f"invalid profile {path}:\n{e}") from e
