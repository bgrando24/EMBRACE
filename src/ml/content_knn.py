from typing import Optional

import numpy as np
import pandas as pd
import scipy.sparse as sp
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import normalize

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
        tmdb_recs    TMDB's "people who liked this also liked" list for the title, plus the title itself: two titles are
                     similar when TMDB recommends one alongside the other, or recommends the same titles for both -
                     a collaborative signal from TMDB's users, including titles not on our server
        movielens    the MovieLens users who liked the movie (rated 4+, see `PreProcess.get_movielens_likers()`):
                     two movies are similar when the same people liked both - collaborative filtering from 200k
                     MovieLens users. Movies up to Oct 2023 only; series and newer movies have an empty block

    The blocks are combined with `weights`, so the similarity of two titles is the weighted sum of their per-block
    cosine similarities (re-weighted over the blocks a title has, if one is empty).

    TF-IDF matters here: a keyword or actor shared by two titles says more the rarer it is ("workplace comedy" vs
    "based on novel or book"). Tokens found in only one title are dropped, as they can never match anything.
    """

    # chosen with `python -m ml.evaluate` on real watch history (Oct 2026, 566 titles, 178 held out):
    # - content blocks (--grid): genre carries almost no signal once keywords are in - every top-10 weighting had genre
    #   at 0 - but a little keeps titles with few keywords sensible and lets explanations mention genres
    # - movielens at 0.3 (the content blocks scaled to the remaining 0.7): hit@10 0.112 -> 0.135, MRR 0.059 -> 0.070,
    #   and next-watch MRR 0.182 -> 0.202; MRR rose steadily from 0.1 to 0.5 in both evaluations
    # - tmdb_recs: no measurable gain at 0.1-0.3 in either evaluation, so off (the data is still fetched)
    # Re-check as history grows.
    DEFAULT_WEIGHTS = {
        "genre": 0.035, "keywords": 0.28, "people": 0.245, "description": 0.14, "tmdb_recs": 0.0, "movielens": 0.3,
    }
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
            "tmdb_recs": [
                sorted(set(recs) | {f"{media}:{tmdb_id}"}) if isinstance(recs, list) else []
                for recs, media, tmdb_id in zip(
                    content.get("recommendations", pd.Series([None] * len(content))),
                    content.get("media_type", pd.Series([None] * len(content))),
                    content.get("tmdb_id", pd.Series([None] * len(content))),
                )
            ],
        }

        self._blocks: dict[str, sp.csr_matrix] = {}
        self._features: dict[str, np.ndarray] = {}
        for name, docs in tokens.items():
            self._blocks[name], self._features[name] = self._tfidf(list(docs), analyzer=lambda toks: toks)
        self._blocks["description"], self._features["description"] = self._tfidf(
            content["overview"].fillna("").tolist(), stop_words="english", sublinear_tf=True
        )
        self._blocks["movielens"], self._features["movielens"] = self._id_tfidf(
            content["movielens_likers"] if "movielens_likers" in content else pd.Series([None] * len(content))
        )

        X = sp.hstack([np.sqrt(self.weights[b]) * self._blocks[b] for b in self.DEFAULT_WEIGHTS], format="csr")
        X.eliminate_zeros()     # a 0-weighted block leaves explicit zeros, which must not count as features
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

    @staticmethod
    def _id_tfidf(id_arrays: pd.Series) -> tuple[sp.csr_matrix, np.ndarray]:
        """
        TF-IDF over integer ID arrays (e.g. MovieLens user IDs), built as a sparse matrix directly - a TfidfVectorizer
        over millions of string tokens is much slower. Same treatment as `_tfidf()`: binary, IDF-weighted (a user who
        likes everything counts for less), IDs in only one title dropped, rows L2-normalised.
        """
        arrays = [np.asarray(a, dtype=np.int64) if a is not None and not (isinstance(a, float)) else np.array([], dtype=np.int64)
                  for a in id_arrays]
        lengths = np.array([len(a) for a in arrays])
        if lengths.sum() == 0:
            return sp.csr_matrix((len(arrays), 0), dtype=np.float32), np.array([], dtype=object)
        ids, cols = np.unique(np.concatenate(arrays), return_inverse=True)
        rows = np.repeat(np.arange(len(arrays)), lengths)
        m = sp.csr_matrix((np.ones(len(cols), dtype=np.float32), (rows, cols)), shape=(len(arrays), len(ids)))
        m.data[:] = 1.0     # binary, even if an ID was listed twice
        df = np.asarray((m > 0).sum(axis=0)).ravel()
        keep = df >= 2
        m, df, ids = m[:, keep], df[keep], ids[keep]
        idf = np.log((1 + len(arrays)) / (1 + df)) + 1      # sklearn's smoothed IDF
        m = normalize(m @ sp.diags(idf.astype(np.float32)), norm="l2")
        return sp.csr_matrix(m, dtype=np.float32), ids.astype(str)

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
