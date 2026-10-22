"""Assemble an Investigator from settings, for the worker and the CLI."""

import logging

import redis
from langgraph.checkpoint.postgres import PostgresSaver
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from oncall.agent.graph import Budgets, Investigator
from oncall.config import Settings
from oncall.links import Linker
from oncall.llm import Pricing, build_chat_model
from oncall.profile import SystemProfile
from oncall.servicemap.store import ServiceMaps
from oncall.telemetry.factory import Backends

log = logging.getLogger(__name__)


def create_checkpointer(database_url: str) -> PostgresSaver:
    """LangGraph's Postgres checkpointer, on its own pool: it needs autocommit connections."""
    pool = ConnectionPool(database_url, min_size=1, max_size=5, open=True,
                          kwargs={"autocommit": True, "prepare_threshold": 0, "row_factory": dict_row})
    checkpointer = PostgresSaver(pool)
    checkpointer.setup()  # creates or migrates the checkpoint tables it owns
    return checkpointer


def create_investigator(settings: Settings, profile: SystemProfile, pool: ConnectionPool,
                        redis_client: redis.Redis, backends: Backends, maps: ServiceMaps,
                        checkpointer: PostgresSaver) -> Investigator:
    model = build_chat_model(settings)
    if model is None:
        log.warning("no Azure OpenAI endpoint/deployment configured: investigations will end in an"
                    " inconclusive report written by code")
    return Investigator(
        profile=profile, backends=backends, links=Linker(profile.links), pool=pool, redis_client=redis_client,
        maps=maps, model=model, checkpointer=checkpointer,
        pricing=Pricing(settings.agent_price_input_per_mtok, settings.agent_price_output_per_mtok),
        budgets=Budgets(max_tool_calls=settings.max_tool_calls, max_active_seconds=settings.max_active_seconds,
                        max_cost_usd=settings.max_cost_usd, max_repeats=settings.max_repeats),
    )
