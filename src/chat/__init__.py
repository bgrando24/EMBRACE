"""
Chat package: the conversational recommender - an LLM (Ollama) with tools over the library and the user's history
"""
from .llm import OllamaClient, LLMUnavailable
from .tools import LibraryTools
from .session import ChatSession

__all__ = ["OllamaClient", "LLMUnavailable", "LibraryTools", "ChatSession"]
