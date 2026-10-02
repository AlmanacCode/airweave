"""Public retrieval mode values, independent of execution and backend settings."""

from enum import Enum


class RetrievalStrategy(str, Enum):
    """Supported retrieval strategies."""

    SEMANTIC = "semantic"
    KEYWORD = "keyword"
    HYBRID = "hybrid"
