"""
Chat package: the conversational recommender - an LLM (Ollama) with tools over the library and the user's history
"""
from .llm import OllamaClient, LLMUnavailable
from .tools import LibraryTools, tmdb_lookup
from .session import ChatSession
from .embeddings import TitleEmbeddings

__all__ = ["OllamaClient", "LLMUnavailable", "LibraryTools", "tmdb_lookup", "ChatSession", "TitleEmbeddings"]
