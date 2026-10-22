"""Process settings, read from ONCALL_* environment variables."""

from pathlib import Path

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
