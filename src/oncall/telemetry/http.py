"""Shared HTTP helper: every backend call has a timeout and fails as BackendError."""

from typing import Any

import httpx

from oncall.telemetry.models import BackendError


def request_json(
    client: httpx.Client, method: str, url: str, *, backend: str, **kwargs: Any
) -> Any:
    try:
        response = client.request(method, url, **kwargs)
    except httpx.TimeoutException as e:
        raise BackendError(f"{backend}: timed out ({e.__class__.__name__})") from e
    except httpx.HTTPError as e:
        raise BackendError(f"{backend}: unreachable ({e})") from e
    if response.status_code >= 400:
        raise BackendError(f"{backend}: HTTP {response.status_code}: {response.text[:200]}")
    try:
        return response.json()
    except ValueError as e:
        raise BackendError(f"{backend}: invalid JSON response") from e
