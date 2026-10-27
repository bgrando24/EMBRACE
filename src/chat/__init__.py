"""
Chat package: the conversational recommender - an LLM (a llama.cpp server) with tools over the library and the user's history
"""
from .llm import LLMClient, LLMUnavailable
from .tools import LibraryTools, tmdb_lookup
from .session import ChatSession
from .embeddings import TitleEmbeddings

__all__ = ["LLMClient", "LLMUnavailable", "LibraryTools", "tmdb_lookup", "ChatSession", "TitleEmbeddings"]
