import os
from typing import Iterable, Optional

import requests

# a model that supports tool calling and fits an 8 GB GPU with room for an 8k context (see docs/modules/chat.md)
DEFAULT_MODEL = "qwen3:8b"
# EMBRACE's own Ollama port, so it never clashes with an Ollama already running on 11434 (see scripts/llm/llm.sh)
DEFAULT_PORT = 11435
DEFAULT_HOSTS = f"http://localhost:{DEFAULT_PORT}"
# Ollama's default context is small enough for tool results and chat history to be silently cut off
DEFAULT_NUM_CTX = 8192
# how long the model stays in (GPU) memory after the last message
DEFAULT_KEEP_ALIVE = "15m"


class LLMUnavailable(Exception):
    """No configured Ollama host is up, or the model isn't pulled on it"""


class OllamaClient:
    """
    Chat completions with tool calling from Ollama (https://github.com/ollama/ollama/blob/main/docs/api.md#chat).

    Several hosts can be configured (EMBRACE_LLM_HOSTS, comma-separated, in order of preference), since the LLM is only
    running when someone has started it (scripts/llm/llm.sh): each request goes to the host that last answered, and
    falls back to the first one that's up when that one stops answering.
    """

    def __init__(
        self,
        hosts: Optional[str | Iterable[str]] = None,
        model: Optional[str] = None,
        num_ctx: Optional[int] = None,
        think: bool = False,
        timeout: float = 300,
    ):
        """
        Args:
            hosts (str | Iterable[str], optional): Ollama base URLs - DEFAULT: EMBRACE_LLM_HOSTS or DEFAULT_HOSTS
            model (str, optional): DEFAULT: EMBRACE_LLM_MODEL or DEFAULT_MODEL
            num_ctx (int, optional): context window in tokens - DEFAULT: EMBRACE_LLM_NUM_CTX or DEFAULT_NUM_CTX
            think (bool, optional): let reasoning models think before answering - better answers, much slower
            timeout (float, optional): seconds to wait for one reply
        """
        hosts = hosts or os.getenv("EMBRACE_LLM_HOSTS") or DEFAULT_HOSTS
        if isinstance(hosts, str):
            hosts = hosts.split(",")
        self.hosts = [h.strip().rstrip("/") for h in hosts if h.strip()]
        self.model = model or os.getenv("EMBRACE_LLM_MODEL") or DEFAULT_MODEL
        self.num_ctx = int(num_ctx or os.getenv("EMBRACE_LLM_NUM_CTX") or DEFAULT_NUM_CTX)
        self.keep_alive = os.getenv("EMBRACE_LLM_KEEP_ALIVE") or DEFAULT_KEEP_ALIVE
        self.think = think
        self.timeout = timeout
        self.host: Optional[str] = None

    def find_host(self) -> Optional[str]:
        """The first configured host that answers, or None"""
        for host in self.hosts:
            try:
                if requests.get(f"{host}/api/version", timeout=2).ok:
                    return host
            except requests.RequestException:
                continue
        return None

    def chat(self, messages: list[dict], tools: Optional[list[dict]] = None) -> dict:
        """
        One assistant turn.

        Args:
            messages (list[dict]): the conversation so far, in Ollama's format ({role, content[, tool_calls]})
            tools (list[dict], optional): JSON-schema function definitions the model may call

        Returns:
            dict: the assistant message - `content`, plus `tool_calls` when the model wants tools run

        Raises:
            LLMUnavailable: no host is up, or the model isn't pulled on the one that is
        """
        body = {
            "model": self.model, "messages": messages, "stream": False, "think": self.think,
            "keep_alive": self.keep_alive, "options": {"num_ctx": self.num_ctx},
        }
        if tools:
            body["tools"] = tools
        for attempt in range(2):     # the remembered host may have been switched off: look again once
            host = self.host if attempt == 0 and self.host else self.find_host()
            if host is None:
                raise LLMUnavailable("No LLM host is running (tried " + ", ".join(self.hosts) + ")")
            try:
                r = requests.post(f"{host}/api/chat", json=body, timeout=self.timeout)
            except requests.ConnectionError:
                self.host = None
                continue
            if r.status_code == 404:
                raise LLMUnavailable(f"Model {self.model} isn't pulled on {host} (scripts/llm/llm.sh start pulls it)")
            if r.status_code == 400 and "think" in r.text and "think" in body:
                # a model without a thinking mode may refuse the flag: ask again without it
                del body["think"]
                r = requests.post(f"{host}/api/chat", json=body, timeout=self.timeout)
            r.raise_for_status()
            self.host = host
            return r.json()["message"]
        raise LLMUnavailable("The LLM host stopped answering")
