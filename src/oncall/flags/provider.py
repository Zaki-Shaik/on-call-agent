"""Feature flag providers. Phase 2 reads flag state; setting a value arrives with approvals (Phase 5)."""

from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from oncall.profile import FlagProvider as FlagProviderConfig
from oncall.telemetry.http import request_json


@dataclass(frozen=True)
class FlagState:
    name: str
    # Variant served when no targeting rule matches.
    default_variant: str
    # Other variants that targeting rules can serve, e.g. "on" for some contexts only.
    targeted_variants: tuple[str, ...] = ()
    description: str = ""

    def describe(self) -> str:
        if not self.targeted_variants:
            return self.default_variant
        return f"{self.default_variant} (targeting: {', '.join(self.targeted_variants)})"

    def as_json(self) -> dict[str, Any]:
        return {"default_variant": self.default_variant, "targeted_variants": list(self.targeted_variants)}


class FlagProvider(Protocol):
    def read_flags(self) -> dict[str, FlagState]:
        """Current state of every flag. Raises BackendError."""
        ...


class FlagdUiProvider:
    """flagd flag definitions, read through flagd-ui's REST API."""

    def __init__(self, config: FlagProviderConfig, client: httpx.Client | None = None) -> None:
        self._url = config.url.rstrip("/")
        self._client = client or httpx.Client(timeout=config.timeout_seconds)

    def read_flags(self) -> dict[str, FlagState]:
        body = request_json(self._client, "GET", f"{self._url}/api/read", backend="flagd-ui")
        return parse_flagd_flags(body.get("flags", {}))


def parse_flagd_flags(flags: dict[str, Any]) -> dict[str, FlagState]:
    states = {}
    for name, definition in flags.items():
        default = str(definition.get("defaultVariant", ""))
        variants = set(definition.get("variants", {}))
        targeted = sorted(_variant_names(definition.get("targeting"), variants) - {default})
        states[name] = FlagState(name, default, tuple(targeted), str(definition.get("description", "")))
    return states


def _variant_names(rule: Any, variants: set[str]) -> set[str]:
    """Variant names a flagd (JsonLogic) targeting rule can return."""
    if isinstance(rule, str):
        return {rule} & variants
    if isinstance(rule, list):
        return set().union(*(_variant_names(r, variants) for r in rule)) if rule else set()
    if isinstance(rule, dict):
        # Skip {"var": ...} lookups: their strings are context keys, not variants.
        return set().union(*(_variant_names(v, variants) for k, v in rule.items() if k != "var")) if rule else set()
    return set()
