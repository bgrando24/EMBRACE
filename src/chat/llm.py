import json
import os
import time
from typing import Optional

import numpy as np
import requests

# a llama.cpp server (llama-server, https://github.com/ggml-org/llama.cpp/tree/master/tools/server) - EMBRACE uses one
# that's already running for other things, so it may be off or busy at any time (see docs/modules/chat.md)
DEFAULT_URL = "http://localhost:8080"
# text embeddings for search by meaning (see docs/modules/chat.md): small, so fast enough on the server's CPU
DEFAULT_EMBED_MODEL = "Qwen3-Embedding-0.6B-Q8_0"
# texts per embedding request
EMBED_BATCH = 64
# a server that's switched off shows up as a connection timeout: give up on it quickly
CONNECT_TIMEOUT = 3
# a reply can wait for the server to load the model (after it slept) and to finish someone else's request first
REPLY_TIMEOUT = 180
# the model list is asked for before each chat request at most this often
MODELS_TTL = 30


class LLMUnavailable(Exception):
    """The LLM server is off, unreachable, busy past the timeout, or couldn't run the model: try again later"""


class LLMClient:
    """
    Chat completions with tool calling, structured replies and text embeddings from a llama.cpp server's
    OpenAI-compatible API (EMBRACE_LLM_URL).

    The server may hold several models but load only one or two at a time (router mode), and someone else may be using
    it: chat uses whichever chat model is loaded already, so EMBRACE never swaps out the model someone else is using,
    and only asks for its default (EMBRACE_LLM_MODEL) when none is. Every failure to get an answer - server off,
    unreachable, too slow, model failing to load - raises LLMUnavailable, so callers can carry on without the LLM.
    """

    def __init__(self, url: Optional[str] = None, model: Optional[str] = None, embed_model: Optional[str] = None,
                 think: bool = False, timeout: float = REPLY_TIMEOUT):
        """
        Args:
            url (str, optional): the server's base URL - DEFAULT: EMBRACE_LLM_URL or DEFAULT_URL
            model (str, optional): chat model to load when none is - DEFAULT: EMBRACE_LLM_MODEL, else the first listed
            embed_model (str, optional): DEFAULT: EMBRACE_EMBED_MODEL or DEFAULT_EMBED_MODEL
            think (bool, optional): let reasoning models think before answering - better answers, much slower
            timeout (float, optional): seconds to wait for one reply
        """
        self.url = (url or os.getenv("EMBRACE_LLM_URL") or DEFAULT_URL).strip().rstrip("/")
        self.model = model or os.getenv("EMBRACE_LLM_MODEL") or None
        self.embed_model = embed_model or os.getenv("EMBRACE_EMBED_MODEL") or DEFAULT_EMBED_MODEL
        self.think = think
        self.timeout = timeout
        self._models: list[dict] = []
        self._models_at = 0.0

    def models(self, fresh: bool = False) -> list[dict]:
        """
        The server's models ({id, status: {value: loaded / unloaded / ...}}), cached for MODELS_TTL seconds

        Raises:
            LLMUnavailable: the server isn't answering
        """
        if fresh or time.time() - self._models_at > MODELS_TTL:
            try:
                r = requests.get(f"{self.url}/v1/models", timeout=(CONNECT_TIMEOUT, 10))
                r.raise_for_status()
            except requests.RequestException as e:
                raise LLMUnavailable(f"The LLM server at {self.url} isn't answering ({type(e).__name__})") from e
            self._models, self._models_at = r.json().get("data", []), time.time()
        return self._models

    def available(self) -> bool:
        try:
            return bool(self.models(fresh=True))
        except LLMUnavailable:
            return False

    def chat_model(self) -> str:
        """The loaded chat model, else the configured one, else the first chat model listed"""
        chat = [m for m in self.models() if m.get("id") != self.embed_model and "embed" not in m.get("id", "").lower()]
        if not chat:
            raise LLMUnavailable(f"The LLM server at {self.url} has no chat model")
        loaded = [m["id"] for m in chat if (m.get("status") or {}).get("value") == "loaded"]
        ids = [m["id"] for m in chat]
        return loaded[0] if loaded else self.model if self.model in ids else ids[0]

    def chat(self, messages: list[dict], tools: Optional[list[dict]] = None, format: Optional[dict] = None) -> dict:
        """
        One assistant turn.

        Args:
            messages (list[dict]): the conversation so far ({role, content[, tool_calls | tool_call_id]})
            tools (list[dict], optional): JSON-schema function definitions the model may call
            format (dict, optional): a JSON schema the reply must follow, answered at temperature 0 so the same
                question gets the same answer

        Returns:
            dict: the assistant message - `content`, plus `tool_calls` ({id, type, function: {name, arguments}})
                when the model wants tools run

        Raises:
            LLMUnavailable: see the class docstring
        """
        body = {"model": self.chat_model(), "messages": [self._message(m) for m in messages],
                "chat_template_kwargs": {"enable_thinking": self.think}}
        if tools:
            body["tools"] = tools
        if format:
            body["response_format"] = {"type": "json_schema", "json_schema": {"name": "reply", "schema": format}}
            body["temperature"] = 0
        message = self._post("/v1/chat/completions", body)["choices"][0]["message"]
        reply = {"role": "assistant", "content": message.get("content") or ""}
        if message.get("tool_calls"):
            reply["tool_calls"] = [{**c, "id": c.get("id") or f"call_{i}"} for i, c in enumerate(message["tool_calls"])]
        return reply

    def embed(self, texts: list[str]) -> np.ndarray:
        """
        Text embeddings from `embed_model`.

        Returns:
            ndarray: one L2-normalised float32 row per text, so dot products are cosine similarities

        Raises:
            LLMUnavailable: see the class docstring, or the server doesn't have the embedding model
        """
        if self.embed_model not in {m.get("id") for m in self.models()}:
            raise LLMUnavailable(f"The LLM server at {self.url} doesn't have {self.embed_model}")
        out = []
        for i in range(0, len(texts), EMBED_BATCH):
            data = self._post("/v1/embeddings", {"model": self.embed_model, "input": texts[i:i + EMBED_BATCH]})["data"]
            out.extend(d["embedding"] for d in sorted(data, key=lambda d: d["index"]))
        e = np.asarray(out, dtype=np.float32).reshape(len(out), -1)
        return e / np.maximum(np.linalg.norm(e, axis=1, keepdims=True), 1e-12)

    def _post(self, path: str, body: dict) -> dict:
        try:
            r = requests.post(f"{self.url}{path}", json=body, timeout=(CONNECT_TIMEOUT, self.timeout))
        except requests.ConnectionError as e:       # includes connection timeouts: the server or its machine is off
            raise LLMUnavailable(f"The LLM server at {self.url} isn't reachable") from e
        except requests.Timeout as e:
            raise LLMUnavailable(f"The LLM server didn't answer within {self.timeout:.0f} s (busy?)") from e
        if r.status_code >= 500 or r.status_code == 404:     # e.g. the model failed to load, or isn't there any more
            self._models_at = 0.0
            raise LLMUnavailable(f"The LLM server couldn't answer ({r.status_code}: {r.text[:200]})")
        r.raise_for_status()        # anything else is a bad request: a bug, not an outage
        return r.json()

    @staticmethod
    def _message(m: dict) -> dict:
        """A conversation message in the API's format: tool calls with JSON-encoded arguments, tool results by call ID"""
        out = {"role": m["role"], "content": m.get("content") or ""}
        if m.get("tool_calls"):
            out["tool_calls"] = []
            for i, c in enumerate(m["tool_calls"]):
                args = c["function"].get("arguments") or {}
                out["tool_calls"].append({"id": c.get("id") or f"call_{i}", "type": "function", "function": {
                    "name": c["function"]["name"], "arguments": args if isinstance(args, str) else json.dumps(args)}})
        if m["role"] == "tool":
            out["tool_call_id"] = m.get("tool_call_id") or "call_0"
        return out
