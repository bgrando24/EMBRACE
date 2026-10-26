import difflib
import functools
import json
import re
from typing import Callable, Iterable, Optional

import numpy as np
import pandas as pd
import scipy.sparse as sp
from sklearn.feature_extraction.text import TfidfVectorizer

from ml import ContentKNN, Recommender, UserProfiles
from ml.picks import BACK_CATALOGUE, FRESH

# IMDB title types shown to the LLM as "show"; everything else (movie, tvMovie, ...) is a "movie"
SHOW_TYPES = {"tvSeries", "tvMiniSeries"}
# characters of the TMDB overview included in list results (title_info has all of it)
ABOUT_CHARS = 220
# how many keywords / cast members to show per title
LIST_KEYWORDS = 6
INFO_CAST = 6
# words in a search query that ask for a genre (IMDB's genre names), so that "something funny" ranks comedies first
# rather than whatever mentions the query's other words most
GENRE_WORDS = {
    "Comedy": ["comedy", "comedies", "funny", "hilarious", "comedic", "humour", "humor", "humorous", "sitcom",
               "laugh", "laughs", "light hearted", "lighthearted", "rom com", "romcom", "romantic comedy"],
    "Horror": ["horror", "scary", "spooky", "creepy", "terrifying", "frightening"],
    "Romance": ["romance", "romantic", "rom com", "romcom", "love story"],
    "Sci-Fi": ["sci fi", "scifi", "science fiction"],
    "Animation": ["animation", "animated", "cartoon", "anime"],
    "Documentary": ["documentary", "documentaries", "doco", "docuseries"],
    "Thriller": ["thriller", "thrillers", "suspense", "suspenseful"],
    "Mystery": ["mystery", "mysteries", "whodunit", "whodunnit"],
    "Crime": ["crime", "gangster", "gangsters", "mafia", "heist"],
    "Action": ["action"],
    "Adventure": ["adventure"],
    "Fantasy": ["fantasy"],
    "Family": ["family friendly", "for kids", "kids"],
    "Drama": ["drama", "dramas"],
    "War": ["war"],
    "Western": ["western", "westerns"],
    "Musical": ["musical", "musicals"],
    "Sport": ["sport", "sports"],
    "Biography": ["biopic", "biography"],
    "History": ["historical", "history", "period piece"],
}
# where a recommended title may be: anywhere (the default: what's worth watching, whether the server has it or not),
# or only on the server / only not on it
AVAILABILITY = ["anywhere", "on_server", "not_on_server"]
# tools whose results leave out titles already suggested in the conversation (see `call`)
EXCLUDING_TOOLS = {"search_library", "similar_titles", "my_recommendations"}


def _norm(name: str) -> str:
    """Title as typed by people: case, punctuation and a leading article don't matter"""
    s = re.sub(r"[^\w\s]", " ", str(name).casefold())
    return re.sub(r"^(the|a|an)\s+", "", " ".join(s.split()))


def _list(value) -> list:
    """A genres / keywords / people cell as a list (they can be missing)"""
    return list(value) if isinstance(value, (list, tuple, np.ndarray)) else []


def tmdb_lookup(tmdb) -> Callable[[str], Optional[dict]]:
    """
    `LibraryTools`' lookup for titles the server never had: TMDB's best match for a name, with its details (see
    `TMDBConnector.fetch_title_details`, plus `year` and `kind`), or None. A match whose name isn't close to the one
    asked for is ignored, so a description passed as a title doesn't become some random film. Answers are cached.

    Args:
        tmdb (TMDBConnector): the connector to look titles up with
    """
    @functools.lru_cache(maxsize=256)
    def lookup(name: str) -> Optional[dict]:
        hit = tmdb.search_title(name)
        details = hit and tmdb.fetch_title_details(hit["media_type"], hit["tmdb_id"])
        if not details or difflib.SequenceMatcher(None, _norm(name), _norm(details["title"])).ratio() < 0.6:
            return None
        return {**details, "year": hit["year"], "kind": "show" if hit["media_type"] == "tv" else "movie"}
    return lambda title: lookup(re.sub(r"\(\d{4}\)\s*$", "", title).strip().casefold())


class LibraryTools:
    """
    What the chat-bot can look up, as tools for an LLM (see `chat.ChatSession`): searching by description, similar
    titles, details of one title, and the user's own picks and watch history. Titles are the library's and, when
    loaded (`Recommender.load(discovery=True)`), thousands beyond it, each marked with whether the server has it.

    Everything is built once from a `Recommender` and shared by all users; each call says which user it's for. Results
    are small JSON documents, since they go into the model's context.
    """

    def __init__(self, rec: Recommender, library: Optional[set[str]] = None, runtimes: Optional[pd.Series] = None,
                 lookup: Optional[Callable[[str], Optional[dict]]] = None):
        """
        Args:
            rec (Recommender): needs `content` (TMDB details), i.e. the content model
            library (set, optional): IMDB IDs on the server - DEFAULT: `rec.library`
            runtimes (Series, optional): `PreProcess.get_title_runtimes()` - minutes per title (episodes: per
                episode), shown in results and used by search's `max_minutes`
            lookup (callable, optional): details of a title the server never had, by name (`tmdb_lookup`), so
                "something like X" and questions about X work for any X - DEFAULT: only the server's titles
        """
        if rec.content is None:
            raise ValueError("The chat tools need TMDB title details (the content model)")
        self.rec = rec
        self.lookup = lookup
        self.library = set(library if library is not None else rec.library)
        if rec.discovery is not None and len(rec.discovery):
            # a model over the library and the titles beyond it, so they can be similar to each other; the daily
            # picks keep the library-only `rec.knn`
            content = pd.concat([rec.content, rec.discovery], ignore_index=True)
            self.knn = ContentKNN(content)
            self.profiles = UserProfiles(self.knn, rec.profiles.history)
        else:
            content, self.knn, self.profiles = rec.content, rec.knn, rec.profiles
        c = content.drop_duplicates("t_const").set_index("t_const")
        year = pd.to_numeric(c["start_year"], errors="coerce").astype("Int64")
        self.titles = c.assign(
            kind=np.where(c["type"].isin(SHOW_TYPES), "show", "movie"),
            year=year,
            display=[f"{n} ({y})" if pd.notna(y) else str(n) for n, y in zip(c["primary_name"], year)],
            on_server=c.index.isin(self.library),
            norm=c["primary_name"].map(_norm),
            minutes=(runtimes if runtimes is not None else pd.Series(dtype=float)).reindex(c.index).astype("Int64"),
        )
        self._ids = self.titles.index.to_numpy()
        self._genres = self.titles["genres"].map(lambda g: set(_list(g))).to_numpy()
        known = set().union(*self._genres) if len(self._genres) else set()
        self._genre_words = {
            g: re.compile(r"\b(" + "|".join(map(re.escape, words)) + r")\b")
            for g, words in GENRE_WORDS.items() if g in known
        }

        # text search over everything said about a title; bigrams so "small town" or "serial killer" match as phrases
        docs = [self._doc(r.primary_name, r.genres, r.keywords, r.people, r.overview) for r in self.titles.itertuples()]
        self._vectorizer = TfidfVectorizer(stop_words="english", ngram_range=(1, 2), sublinear_tf=True)
        self._X = sp.csr_matrix(self._vectorizer.fit_transform(docs))

        self._tools: dict[str, tuple[Callable, dict]] = {
            "search_library": (self.search_library, {
                "description": "Find titles matching a description: themes, setting, tone, plot "
                               "elements, genre, actors or directors. Write the query as descriptive keywords, e.g. "
                               "'small town canada comedy hockey' rather than a question. Whenever the user names a "
                               "title they liked (on the server or not), also pass it as `like`: results then match "
                               "both the description and that title.",
                "properties": {
                    "query": {"type": "string", "description": "descriptive keywords"},
                    "like": {"type": "string", "description": "a title the user liked, to find titles similar to it"},
                    "kind": {"type": "string", "enum": ["movie", "show", "any"], "description": "default any"},
                    "availability": {"type": "string", "enum": AVAILABILITY,
                                     "description": "default anywhere: on the server or not"},
                    "max_minutes": {"type": "integer", "description": "only titles at most this long (shows: per episode)"},
                    "include_watched": {"type": "boolean", "description": "default false"},
                    "limit": {"type": "integer", "description": "default 6"},
                },
                "required": ["query"],
            }),
            "similar_titles": (self.similar_titles, {
                "description": "Titles most similar to a given title (by keywords, cast and crew, "
                               "plot, genre, and what other viewers liked). The given title doesn't have to be on "
                               "the server.",
                "properties": {
                    "title": {"type": "string", "description": "the title's name, optionally with its year"},
                    "kind": {"type": "string", "enum": ["movie", "show", "any"], "description": "default any"},
                    "availability": {"type": "string", "enum": AVAILABILITY,
                                     "description": "default anywhere: on the server or not"},
                    "include_watched": {"type": "boolean", "description": "default false"},
                    "limit": {"type": "integer", "description": "default 6"},
                },
                "required": ["title"],
            }),
            "title_info": (self.title_info, {
                "description": "Details of one title, on the server or not: plot, genres, keywords, cast and crew, "
                               "whether it's on the server, and whether the user has watched it.",
                "properties": {"title": {"type": "string", "description": "the title's name, optionally with its year"}},
                "required": ["title"],
            }),
            "my_recommendations": (self.my_recommendations, {
                "description": "The user's personal recommendations from their watch history: 'fresh' = new on the "
                               "server, 'back_catalogue' = older titles. Use when they ask what to watch without "
                               "saying what they're in the mood for.",
                "properties": {
                    "list": {"type": "string", "enum": [FRESH, BACK_CATALOGUE], "description": "default fresh"},
                    "limit": {"type": "integer", "description": "default 6"},
                },
                "required": [],
            }),
            "my_watch_history": (self.my_watch_history, {
                "description": "What the user has watched: the titles they watched most, or most recently.",
                "properties": {
                    "order": {"type": "string", "enum": ["most_watched", "recent"], "description": "default most_watched"},
                    "limit": {"type": "integer", "description": "default 15"},
                },
                "required": [],
            }),
        }

    # ---- for the LLM ----

    def schemas(self) -> list[dict]:
        """Tool definitions in Ollama's (OpenAI-style) format"""
        return [
            {"type": "function", "function": {
                "name": name, "description": spec["description"],
                "parameters": {"type": "object", "properties": spec["properties"], "required": spec["required"]},
            }}
            for name, (_, spec) in self._tools.items()
        ]

    def call(self, name: str, arguments: dict, user_id: Optional[str], exclude: Iterable[str] = ()) -> str:
        """
        Runs a tool for a user; errors come back as a JSON `error` the model can read and recover from.
        `exclude`: titles (as results name them, "Name (year)") to leave out of search, similar titles and
        recommendations - the ones already suggested, so a follow-up ("something else?") gets new titles.
        """
        if name not in self._tools:
            return json.dumps({"error": f"unknown tool {name}"})
        fn, spec = self._tools[name]
        args = {k: v for k, v in (arguments or {}).items() if k in spec["properties"]}
        if name in EXCLUDING_TOOLS:
            args["exclude"] = frozenset(exclude)
        try:
            return json.dumps(fn(user_id=user_id, **args), ensure_ascii=False, default=str)
        except Exception as e:      # incl. bad argument types: the model can retry, the conversation goes on
            return json.dumps({"error": str(e) if isinstance(e, ValueError) else f"{type(e).__name__}: {e}"})

    # ---- tools ----

    def search_library(self, query: str = "", user_id: Optional[str] = None, like: Optional[str] = None,
                       kind: str = "any", availability: str = "anywhere", max_minutes: Optional[int] = None,
                       include_watched: bool = False, limit: int = 6, exclude: frozenset = frozenset()) -> dict:
        """
        Ranks titles by how well they match `query`: the text match, averaged with the share of the genres the query
        asks for that a title has (GENRE_WORDS: "funny" asks for Comedy). With `like`, that's averaged with the
        similarity to the `like` title (`_like`). Each part is scaled to its best match, so a small model gets all
        of it from one call rather than having to merge tool results itself.
        `max_minutes` leaves out titles that are longer, or whose length isn't known.
        """
        mask = self._mask(user_id, kind, include_watched, exclude, availability)
        if max_minutes:
            mask &= (self.titles["minutes"] <= int(max_minutes)).fillna(False).to_numpy(dtype=bool)
        ids, out = self._ids[mask], {}
        q = self._vectorizer.transform([query or ""])
        text = self._scaled(pd.Series(np.asarray((self._X[np.flatnonzero(mask)] @ q.T).todense()).ravel(), index=ids))
        matches = [text] if text.max() > 0 else []
        wanted = {g for g, words in self._genre_words.items() if words.search(_norm(query or ""))}
        if wanted:
            matches.append(pd.Series([len(wanted & g) / len(wanted) for g in self._genres[mask]], index=ids))
            out["genres_asked_for"] = sorted(wanted)
        parts = [sum(matches) / len(matches)] if matches else []
        shared = None
        if like:
            similarity, out["like"], shared = self._like(like, mask)
            if similarity is not None:
                parts.append(similarity)
        if exclude:
            out["left_out"] = "titles already suggested in this conversation"
        score = sum(parts) / len(parts) if parts else pd.Series(dtype=float)
        top = score[score > 0].sort_values(ascending=False).head(self._limit(limit))
        if top.empty:
            return {**out, "results": [], "note": "nothing matched; try other words (synonyms, setting, genre)"}
        taste = self._taste_fit(user_id)
        results = [self._summary(t, user_id, taste) for t in top.index]
        if shared is not None:
            for result, in_common in zip(results, shared(top.index)):
                if in_common:
                    result["in_common"] = in_common[:5]
        return {**out, "results": results}

    def similar_titles(self, title: str, user_id: Optional[str] = None, kind: str = "any",
                       availability: str = "anywhere", include_watched: bool = False, limit: int = 6,
                       exclude: frozenset = frozenset()) -> dict:
        try:
            t_const, others = self._resolve(title)
        except ValueError:
            # not a title we have details of: compared by its looked-up details instead, if it can be looked up
            if self._lookup(title) is None:
                raise
            out = self.search_library(user_id=user_id, like=title, kind=kind, availability=availability,
                                      include_watched=include_watched, limit=limit, exclude=exclude)
            return {"similar_to": out.pop("like"), **out}
        knn = self.knn
        if t_const not in knn:
            return {"error": f"{self.titles.loc[t_const, 'primary_name']} has no features to compare"}
        allowed = self.titles.index[self._mask(user_id, kind, include_watched, exclude, availability)]
        recs = knn.recommend(t_const, n=self._limit(limit), allowed=allowed)
        taste = self._taste_fit(user_id)
        results = [{**self._summary(t, user_id, taste), "in_common": shared[:5]}
                   for t, shared in zip(recs["t_const"], recs[knn.SHARED_COL])]
        return {"similar_to": self._name(t_const), **({"other_matches": others} if others else {}), "results": results}

    def title_info(self, title: str, user_id: Optional[str] = None) -> dict:
        try:
            t_const, others = self._resolve(title)
        except ValueError:
            if (d := self._lookup(title)) is None:
                raise
            people = self._people(d["people"], cast=INFO_CAST)
            return {
                "title": f"{d['title']} ({d['year']})" if d["year"] else d["title"], "type": d["kind"],
                "on_server": False, "note": "not on the server: they can request it in Jellyseerr",
                "genres": d["genres"], "keywords": d["keywords"][:15],
                "crew": [f"{p['name']} ({p['role']})" for p in people if p["role"] != "cast"],
                "cast": [p["name"] for p in people if p["role"] == "cast"],
                "about": d["overview"],
            }
        r = self.titles.loc[t_const]
        people = self._people(r["people"], cast=INFO_CAST)
        out = {
            "title": self._name(t_const), "type": r["kind"], "on_server": bool(r["on_server"]),
            "genres": list(r["genres"] or []), "keywords": list(r["keywords"] or [])[:15],
            "crew": [f"{p['name']} ({p['role']})" for p in people if p["role"] != "cast"],
            "cast": [p["name"] for p in people if p["role"] == "cast"],
            "about": r["overview"],
        }
        if not r["on_server"]:
            out["note"] = "not on the server: they can request it in Jellyseerr"
        if pd.notna(r["avg_rating"]):
            out["imdb_rating"] = float(r["avg_rating"])
        out.update(self._length(t_const))
        if user_id is not None:
            out["you_watched"] = self._watched_note(user_id, t_const)
            if (fit := self._taste_fit(user_id)) is not None and t_const in fit.index:
                out["taste_fit"] = int(fit[t_const])
        if others:
            out["other_matches"] = others
        return out

    def my_recommendations(self, user_id: Optional[str] = None, list: str = FRESH, limit: int = 6,
                           exclude: frozenset = frozenset()) -> dict:
        if user_id is None:
            return {"error": "no user"}
        try:
            picks = self.rec.picks(user_id, n=self._limit(limit) + len(exclude))
        except (KeyError, ValueError):
            return {"error": "not enough watch history for personal recommendations yet; use search_library"}
        picks = picks[(picks["list"] == list) & ~picks["t_const"].map(self.titles["display"]).isin(exclude)]
        picks = picks.head(self._limit(limit))
        if picks.empty:
            return {"results": [], "note": "nothing new on the server recently" if list == FRESH else "no results"}
        taste = self._taste_fit(user_id)
        results = []
        for r in picks.itertuples():
            item = {**self._summary(r.t_const, user_id, taste), "because_you_watched": r.because_you_watched}
            if pd.notna(r.added_days_ago):
                item["added_days_ago"] = int(r.added_days_ago)
            if pd.notna(r.you_requested) and bool(r.you_requested):
                item["you_requested_it"] = True
            results.append(item)
        return {"list": list, "results": results}

    def my_watch_history(self, user_id: Optional[str] = None, order: str = "most_watched", limit: int = 15) -> dict:
        h = self._history(user_id)
        if h.empty:
            return {"results": [], "note": "no watch history yet"}
        if order == "recent":
            h = h.sort_values("last_watched_timestamp", ascending=False, na_position="last")
        else:
            h = h.sort_values("total_seconds_watched", ascending=False)
        results = [
            {"title": self._name(r.imdb_id) if r.imdb_id in self.titles.index else r.unit_name,
             "type": "show" if r.unit_type == "Series" else "movie", **self._watch_stats(r)}
            for r in h.head(self._limit(limit, cap=30)).itertuples()
        ]
        return {"order": order, "results": results}

    # ---- helpers ----

    @staticmethod
    def _limit(limit, cap: int = 15) -> int:
        try:
            return max(1, min(int(limit), cap))
        except (TypeError, ValueError):
            return 6

    @staticmethod
    def _people(people, cast: int) -> list[dict]:
        """Crew first (each person once, all their roles), then the first `cast` cast members"""
        people = list(people) if isinstance(people, (list, np.ndarray)) else []
        crew: dict[str, dict] = {}
        for p in people:
            if p["role"] != "cast":
                crew.setdefault(p["name"], {"name": p["name"], "roles": []})["roles"].append(p["role"])
        out = [{"name": c["name"], "role": "/".join(c["roles"])} for c in crew.values()]
        return out + [p for p in people if p["role"] == "cast"][:cast]

    def _doc(self, name, genres, keywords, people, overview) -> str:
        """What text search matches a title on: name, genres, keywords (twice: the most telling), main people, plot"""
        keywords = " ".join(_list(keywords))
        return " ".join([str(name or ""), " ".join(_list(genres)), keywords, keywords,
                         " ".join(p["name"] for p in self._people(people, cast=5)), str(overview or "")])

    @staticmethod
    def _scaled(score: pd.Series) -> pd.Series:
        return score / score.max() if len(score) and score.max() > 0 else score

    def _like(self, like: str, mask: np.ndarray) -> tuple[Optional[pd.Series], str, Optional[Callable]]:
        """
        For search's `like`: how similar each allowed title is to it (scaled to the best match; None if it can't be
        compared), what to call it in the result, and a function giving what each of some titles has in common with
        it. A title we have details of is compared by `ContentKNN` (and left out of the similarities); any other
        by the text match to its looked-up details, without its name (so "The Boys" isn't like "Trailer Park Boys").
        """
        ids = self._ids[mask]
        try:
            seed, _ = self._resolve(like)
        except ValueError as e:
            if (d := self._lookup(like)) is None:
                return None, str(e), None
            q = self._vectorizer.transform([self._doc("", d["genres"], d["keywords"], d["people"], d["overview"])])
            similarity = pd.Series(np.asarray((self._X[np.flatnonzero(mask)] @ q.T).todense()).ravel(), index=ids)
            theirs = {w.casefold() for w in [*d["genres"], *d["keywords"]]}
            def shared(titles):
                return [[w for w in [*_list(self.titles.at[t, "keywords"]), *_list(self.titles.at[t, "genres"])]
                         if w.casefold() in theirs] for t in titles]
            name = f"{d['title']} ({d['year']})" if d["year"] else d["title"]
            return self._scaled(similarity), f"{name} - compared by its genres, keywords and plot", shared
        knn = self.knn
        if seed not in knn:
            return None, f"{self._name(seed)} has no features to compare", None
        candidates = [t for t in ids if t in knn and t != seed]
        similarity = pd.Series(knn.similarity_between([seed], candidates)[0] if candidates else [], index=candidates, dtype=float)
        def shared(titles):
            in_knn = [t for t in titles if t in knn]
            found = dict(zip(in_knn, knn.shared_between(seed, in_knn))) if in_knn else {}
            return [found.get(t, []) for t in titles]
        # -inf for the seed itself, so a good text match can't put it in its own results
        return self._scaled(similarity).reindex(ids, fill_value=0).where(ids != seed, -np.inf), self._name(seed), shared

    def _lookup(self, title: str) -> Optional[dict]:
        """A title the server never had, looked up (see `__init__`'s lookup); None if it can't be"""
        if self.lookup is None:
            return None
        try:
            return self.lookup(title)
        except Exception:       # e.g. TMDB unreachable: answered as if it were unknown
            return None

    def _name(self, t_const: str) -> str:
        return self.titles.at[t_const, "display"]

    def _resolve(self, title: str) -> tuple[str, list[str]]:
        """
        The best match for a title as a person would type it (any case, with or without "The", optionally with a year
        in brackets), preferring titles on the server and then popular ones. Returns it and any other close matches.

        Raises:
            ValueError: nothing close enough
        """
        year = re.search(r"\((\d{4})\)\s*$", title.strip())
        name = _norm(re.sub(r"\(\d{4}\)\s*$", "", title)) if year else _norm(title)
        t = self.titles
        hits = t[t["norm"] == name]
        if hits.empty and len(name) >= 4:
            hits = t[t["norm"].str.startswith(name) | t["norm"].str.contains(rf"\b{re.escape(name)}\b", regex=True)]
        if hits.empty:
            close = difflib.get_close_matches(name, t["norm"].unique().tolist(), n=3, cutoff=0.8)
            hits = t[t["norm"].isin(close)]
        if hits.empty:
            raise ValueError(f"no title called '{title}' found; describe it to search_library instead")
        hits = hits.assign(
            year_match=(hits["year"] == int(year.group(1))).fillna(False) if year else False,
            votes=hits["num_votes"].fillna(0),
        ).sort_values(["year_match", "on_server", "votes"], ascending=False)
        return hits.index[0], [self._name(o) for o in hits.index[1:4]]

    def _mask(self, user_id: Optional[str], kind: str, include_watched: bool, exclude: Iterable[str] = (),
              availability: str = "anywhere") -> np.ndarray:
        on_server = self.titles["on_server"].to_numpy()
        mask = on_server.copy() if availability == "on_server" else ~on_server if availability == "not_on_server" \
            else np.ones(len(self.titles), dtype=bool)
        if kind in ("movie", "show"):
            mask &= (self.titles["kind"] == kind).to_numpy()
        if not include_watched and user_id is not None:
            mask &= ~self.titles.index.isin(self._seen(user_id))
        if exclude:
            mask &= ~self.titles["display"].isin(list(exclude)).to_numpy()
        return mask

    def _seen(self, user_id: str) -> list[str]:
        try:
            return self.profiles.seen_by(user_id)
        except KeyError:
            return []

    def _history(self, user_id: Optional[str]) -> pd.DataFrame:
        h = self.profiles.history
        return h[h["user_id"] == user_id] if user_id is not None else h.iloc[0:0]

    def _taste_fit(self, user_id: Optional[str]) -> Optional[pd.Series]:
        """Percentile (0-100) of each title's similarity to the user's taste, among all titles"""
        if user_id is None:
            return None
        try:
            similarity = self.profiles.taste_similarity(user_id)
        except (KeyError, ValueError):
            return None
        return (similarity.rank(pct=True) * 100).round(0)

    def _length(self, t_const: str) -> dict:
        minutes = self.titles.loc[t_const, "minutes"]
        if pd.isna(minutes):
            return {}
        return {"episode_minutes" if self.titles.loc[t_const, "kind"] == "show" else "minutes": int(minutes)}

    def _watch_stats(self, r) -> dict:
        out = {"hours_watched": round(float(r.total_seconds_watched or 0) / 3600, 1)}
        if r.unit_type == "Series" and pd.notna(r.items_watched):
            out["episodes_watched"] = int(r.items_watched)
        if pd.notna(r.last_watched_timestamp):
            out["last_watched"] = str(r.last_watched_timestamp)[:10]
        return out

    def _watched_note(self, user_id: str, t_const: str):
        h = self._history(user_id)
        h = h[h["imdb_id"] == t_const]
        return self._watch_stats(next(h.itertuples())) if len(h) else False

    def _summary(self, t_const: str, user_id: Optional[str], taste: Optional[pd.Series]) -> dict:
        r = self.titles.loc[t_const]
        about = str(r["overview"] or "")
        out = {
            "title": self._name(t_const), "type": r["kind"], "on_server": bool(r["on_server"]),
            "genres": list(r["genres"] or [])[:3], "keywords": list(r["keywords"] or [])[:LIST_KEYWORDS],
            "about": about if len(about) <= ABOUT_CHARS else about[:ABOUT_CHARS].rsplit(" ", 1)[0] + "...",
            **self._length(t_const),
        }
        if pd.notna(r["avg_rating"]):
            out["imdb_rating"] = float(r["avg_rating"])
        if user_id is not None and t_const in self._seen(user_id):
            out["you_watched"] = True
        if taste is not None and t_const in taste.index:
            out["taste_fit"] = int(taste[t_const])
        return out
