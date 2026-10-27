import hashlib
from pathlib import Path
from typing import Callable, Optional

import numpy as np
import pandas as pd

# Qwen3-Embedding is trained with an instruction in front of each query (documents get none)
QUERY_INSTRUCTION = "Instruct: Given what someone is in the mood to watch, retrieve movies and TV shows that match it\nQuery: "
# keywords and cast members in a title's text: enough to say what it is, short enough to embed quickly
DOC_KEYWORDS = 20
DOC_CAST = 4
DEFAULT_DIR = "data/cache"


def title_text(name, year, kind: str, genres, keywords, people, overview) -> str:
    """What a title's embedding is made from, e.g. 'Heat (1995). Movie. Genres: ... Starring: ... A group of ...'"""
    def items(x):
        return [] if x is None or isinstance(x, float) else list(x)

    makers = [p["name"] for p in items(people) if p.get("role") in ("director", "creator")][:2]
    cast = [p["name"] for p in items(people) if p.get("role") == "cast"][:DOC_CAST]
    parts = [f"{name}{f' ({int(year)})' if pd.notna(year) else ''}.", "TV series." if kind == "show" else "Movie."]
    if items(genres):
        parts.append(f"Genres: {', '.join(items(genres))}.")
    if items(keywords):
        parts.append(f"Keywords: {', '.join(items(keywords)[:DOC_KEYWORDS])}.")
    if makers:
        parts.append(f"{'Created' if kind == 'show' else 'Directed'} by {', '.join(makers)}.")
    if cast:
        parts.append(f"Starring {', '.join(cast)}.")
    if isinstance(overview, str) and overview.strip():
        parts.append(overview.strip())
    return " ".join(parts)


class TitleEmbeddings:
    """
    An embedding per title, for search by meaning, cached in `data/cache/title_embeddings_<model>.parquet`. Each is
    stored with a hash of the text it was made from, so a title is embedded again only when its details change.

    Made by the Ollama host that runs the chat model (`OllamaClient.embed`), not by the nightly refresh: the bot needs
    that host to answer anyway, and the refresh then never depends on it being on.
    """

    def __init__(self, model: str, directory: str = DEFAULT_DIR):
        self.model = model
        self.path = Path(directory) / f"title_embeddings_{model.replace(':', '_').replace('/', '_')}.parquet"
        self._hash: dict[str, str] = {}
        self._vectors: dict[str, np.ndarray] = {}
        if self.path.exists():
            df = pd.read_parquet(self.path)
            for t, h, v in zip(df["t_const"], df["doc_hash"], df["vector"]):
                self._hash[t], self._vectors[t] = h, np.frombuffer(v, dtype=np.float32)

    @staticmethod
    def texts(titles: pd.DataFrame) -> pd.Series:
        """`title_text()` of each row of `LibraryTools.titles`, by t_const"""
        return pd.Series([title_text(r.primary_name, r.year, r.kind, r.genres, r.keywords, r.people, r.overview)
                          for r in titles.itertuples()], index=titles.index)

    @staticmethod
    def _digest(text: str) -> str:
        return hashlib.sha1(text.encode()).hexdigest()[:16]

    def missing(self, texts: pd.Series) -> pd.Series:
        """The texts with no embedding yet, or a different one from when they were embedded"""
        return texts[[self._hash.get(t) != self._digest(x) for t, x in texts.items()]]

    def update(self, texts: pd.Series, embed: Callable[[list[str]], np.ndarray], batch: int = 512,
               log: Callable[[str], None] = print) -> int:
        """
        Embeds what's missing (`missing()`), saving after every `batch` so an interrupted run keeps its progress.

        Returns:
            int: titles embedded
        """
        todo = self.missing(texts)
        for i in range(0, len(todo), batch):
            part = todo.iloc[i:i + batch]
            for t, x, v in zip(part.index, part.to_numpy(), embed(part.tolist())):
                self._hash[t], self._vectors[t] = self._digest(x), v
            self.save()
            if len(todo) > batch:
                log(f"[INFO] embedded {min(i + batch, len(todo))}/{len(todo)} titles")
        return len(todo)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        pd.DataFrame({"t_const": list(self._vectors), "doc_hash": [self._hash[t] for t in self._vectors],
                      "vector": [v.astype(np.float32).tobytes() for v in self._vectors.values()]}).to_parquet(tmp, index=False)
        tmp.replace(self.path)

    def matrix(self, t_consts: pd.Index) -> Optional[np.ndarray]:
        """One row per t_const (zeros where there's none), or None if nothing is embedded"""
        if not self._vectors:
            return None
        dim = len(next(iter(self._vectors.values())))
        zeros = np.zeros(dim, dtype=np.float32)
        return np.vstack([self._vectors.get(t, zeros) for t in t_consts])

    def __len__(self) -> int:
        return len(self._vectors)
