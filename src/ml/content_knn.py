from typing import Optional

import numpy as np
import pandas as pd
import scipy.sparse as sp
from sklearn.feature_extraction.text import TfidfVectorizer

from .cosine_knn import CosineKNN


class ContentKNN(CosineKNN):
    """
    Recommender over richer content features than genre, for the titles TMDB details were fetched for (the library,
    including removed titles still in watch history). See `CosineKNN` for ranking and ties.

    Feature blocks, each TF-IDF weighted and L2-normalised on its own:
        genre        IMDB genres (TMDB's when IMDB has none for the title)
        keywords     TMDB keywords, plus the movie's franchise ("collection") and original language when not English
                     (the library is mostly English, so "language: en" would say nothing)
        people       director / writers (movies) or creators (series) and the top-billed cast, by TMDB person ID
        description  words of the TMDB overview (English stop words removed)

    The blocks are combined with `weights`, so the similarity of two titles is the weighted sum of their per-block
    cosine similarities (re-weighted over the blocks a title has, if one is empty).

    TF-IDF matters here: a keyword or actor shared by two titles says more the rarer it is ("workplace comedy" vs
    "based on novel or book"). Tokens found in only one title are dropped, as they can never match anything.
    """

    # chosen by leave-one-out on real watch history (`python -m ml.evaluate --grid`, Oct 2026: 542 titles, 168 held
    # out). Genre carries almost no signal once keywords are in - every top-10 weighting had genre at 0 - but a little
    # keeps titles with few keywords sensible and lets explanations mention genres. Re-check as history grows.
    DEFAULT_WEIGHTS = {"genre": 0.05, "keywords": 0.4, "people": 0.35, "description": 0.2}
    # blocks shown in explanations, in this order (description words are too noisy to read as a reason; genres go
    # last as they're shared by so many titles)
    EXPLAIN_BLOCKS = (("keywords", "people"), ("genre",))

    def __init__(self, content: pd.DataFrame, weights: Optional[dict[str, float]] = None):
        """
        Args:
            content (DataFrame): output of `PreProcess.get_title_content()` - one row per title with OUTPUT_COLS,
                `genres`, `keywords`, `collection`, `original_language`, `people` (list of {id, name, role}), `overview`
            weights (dict, optional): block -> weight - DEFAULT: DEFAULT_WEIGHTS. A block can be switched off with 0.
        """
        self.weights = {**self.DEFAULT_WEIGHTS, **(weights or {})}
        if unknown := set(self.weights) - set(self.DEFAULT_WEIGHTS):
            raise ValueError(f"Unknown feature blocks: {unknown}")
        if sum(self.weights.values()) <= 0:
            raise ValueError("At least one block needs a positive weight")

        content = content.reset_index(drop=True)
        self._person_names = {
            str(p["id"]): p["name"] for people in content["people"] for p in people
        }
        tokens = {
            "genre": content["genres"].map(list),
            "keywords": [
                list(kw) + ([f"collection: {col}"] if isinstance(col, str) and col else [])
                + ([f"language: {lang}"] if isinstance(lang, str) and lang and lang != "en" else [])
                for kw, col, lang in zip(content["keywords"], content["collection"], content["original_language"])
            ],
            # a person counts once per title, whatever their role(s)
            "people": content["people"].map(lambda people: sorted({str(p["id"]) for p in people})),
        }

        self._blocks: dict[str, sp.csr_matrix] = {}
        self._features: dict[str, np.ndarray] = {}
        for name, docs in tokens.items():
            self._blocks[name], self._features[name] = self._tfidf(list(docs), analyzer=lambda toks: toks)
        self._blocks["description"], self._features["description"] = self._tfidf(
            content["overview"].fillna("").tolist(), stop_words="english", sublinear_tf=True
        )

        X = sp.hstack([np.sqrt(self.weights[b]) * self._blocks[b] for b in self.DEFAULT_WEIGHTS], format="csr")
        has_features = X.getnnz(axis=1) > 0
        self._blocks = {b: m[has_features] for b, m in self._blocks.items()}
        self._genre_lists = content.loc[has_features, "genres"].map(list).tolist()
        self._init_index(content.loc[has_features, self.OUTPUT_COLS].reset_index(drop=True), X[has_features])

    @staticmethod
    def _tfidf(docs: list, **kwargs) -> tuple[sp.csr_matrix, np.ndarray]:
        """TF-IDF matrix (rows L2-normalised, all-zero for empty docs) and its feature names"""
        vec = TfidfVectorizer(min_df=2, binary="analyzer" in kwargs, **kwargs)
        try:
            return sp.csr_matrix(vec.fit_transform(docs), dtype=np.float32), vec.get_feature_names_out()
        except ValueError:      # nothing appears in 2+ titles (tiny or empty corpus)
            return sp.csr_matrix((len(docs), 0), dtype=np.float32), np.array([], dtype=object)

    def genres_of(self, t_const: str) -> list[str]:
        """Genres of a title in the model"""
        return self._genre_lists[int(self._row_of[t_const])]

    def _shared(self, seed: int, rows: np.ndarray, top: int = 5) -> list[list[str]]:
        """Shared keywords and people by how much each adds to the similarity, then shared genres"""
        out = []
        for j in rows:
            labels = []
            for group in self.EXPLAIN_BLOCKS:
                found = []
                for b in group:
                    if self.weights[b] <= 0:
                        continue
                    a, c = self._blocks[b][seed], self._blocks[b][j]
                    for f in np.intersect1d(a.indices, c.indices):
                        found.append((-self.weights[b] * a[0, f] * c[0, f], self._label(b, self._features[b][f])))
                labels += [label for _, label in sorted(found)]
            out.append(labels[:top])
        return out

    def _label(self, block: str, feature: str) -> str:
        return self._person_names.get(feature, feature) if block == "people" else feature
