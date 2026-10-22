"""Process settings, read from ONCALL_* environment variables."""

from pathlib import Path

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ONCALL_")

    log_level: str = "INFO"
    profile_path: Path = Path("profiles/otel-demo.yaml")
    database_url: str = "postgresql://oncall:oncall@localhost:5433/oncall"
    redis_url: str = "redis://localhost:6380/0"
    # If set, webhooks must send `Authorization: Bearer <token>`.
    webhook_token: str | None = None
    # A firing alert joins an open incident (any service) opened within this window.
    group_window_minutes: int = 10
    # Healthy service map: traces from this far back (or whatever exists), rebuilt this often.
    map_window_hours: int = 24
    map_refresh_minutes: int = 60

    # Azure OpenAI. Without an endpoint and agent deployment, investigations end in an
    # inconclusive report written by code. Without an API key, the Azure login is used.
    azure_openai_endpoint: str | None = None
    azure_openai_api_key: SecretStr | None = None
    azure_openai_api_version: str = "2024-10-21"
    agent_deployment: str | None = None
    judge_deployment: str | None = None  # Phase 6: a different, stronger model
    # None: the model's default. Some reasoning models accept no other value.
    agent_temperature: float | None = None
    llm_timeout_seconds: float = 60
    llm_max_retries: int = 3
    # USD per million tokens of the agent deployment, for the cost budget. Defaults: gpt-4.1-mini.
    agent_price_input_per_mtok: float = 0.40
    agent_price_output_per_mtok: float = 1.60

    # Investigation budgets.
    max_tool_calls: int = 10
    max_active_seconds: float = 180
    max_cost_usd: float = 0.50
    max_repeats: int = 3
