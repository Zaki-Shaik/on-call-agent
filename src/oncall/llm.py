"""The chat model: Azure OpenAI through LangChain's AzureChatOpenAI, used by the LangGraph nodes.

Credentials stay inside the model object: they never enter graph state, checkpoints or prompts.
"""

from dataclasses import dataclass
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage
from pydantic import BaseModel

from oncall.config import Settings


@dataclass(frozen=True)
class Pricing:
    """USD per million tokens."""

    input_per_mtok: float
    output_per_mtok: float

    def cost(self, message: AIMessage) -> tuple[int, int, float]:
        """(input tokens, output tokens, USD) of one model reply."""
        usage = message.usage_metadata or {}
        tokens_in, tokens_out = usage.get("input_tokens", 0), usage.get("output_tokens", 0)
        return tokens_in, tokens_out, (tokens_in * self.input_per_mtok + tokens_out * self.output_per_mtok) / 1e6


def strict_schema(model: type[BaseModel]) -> dict[str, Any]:
    """A pydantic model's JSON schema in the form OpenAI strict mode accepts.

    Strict mode needs every property listed as required (optional ones are nullable instead)
    and no additional properties on any object; it rejects `default`.
    """

    def fix(node: Any) -> Any:
        if isinstance(node, dict):
            node = {k: fix(v) for k, v in node.items() if k != "default"}
            if node.get("type") == "object" and "properties" in node:
                node["required"] = list(node["properties"])
                node["additionalProperties"] = False
            return node
        if isinstance(node, list):
            return [fix(v) for v in node]
        return node

    return fix(model.model_json_schema())


def function_tool(name: str, description: str, args: type[BaseModel]) -> dict[str, Any]:
    """A tool definition for `bind_tools`, with arguments validated by the API (strict mode)."""
    return {"type": "function",
            "function": {"name": name, "description": description, "parameters": strict_schema(args), "strict": True}}


def response_format(name: str, model: type[BaseModel]) -> dict[str, Any]:
    """Structured output: the reply must be JSON matching `model`."""
    return {"type": "json_schema", "json_schema": {"name": name, "schema": strict_schema(model), "strict": True}}


def build_chat_model(settings: Settings) -> BaseChatModel | None:
    """The agent's model, or None when no deployment is configured (investigations then end in
    an inconclusive report written by code)."""
    if not (settings.azure_openai_endpoint and settings.agent_deployment):
        return None
    from langchain_openai import AzureChatOpenAI

    if settings.azure_openai_api_key and settings.azure_openai_api_key.get_secret_value():
        auth: dict[str, Any] = {"api_key": settings.azure_openai_api_key.get_secret_value()}
    else:
        # No key: the Azure login of whoever runs the worker (az login, managed identity, ...).
        from azure.identity import DefaultAzureCredential, get_bearer_token_provider

        auth = {"azure_ad_token_provider": get_bearer_token_provider(
            DefaultAzureCredential(), "https://cognitiveservices.azure.com/.default")}
    return AzureChatOpenAI(
        azure_endpoint=settings.azure_openai_endpoint,
        azure_deployment=settings.agent_deployment,
        api_version=settings.azure_openai_api_version,
        temperature=settings.agent_temperature,
        timeout=settings.llm_timeout_seconds,
        # Connection errors, 429s and 5xx are retried with exponential backoff.
        max_retries=settings.llm_max_retries,
        **auth,
    )
