"""
Style tags: how a title feels to watch - mood, pacing, humour, intensity - which its genres and plot keywords don't
say ("a crime drama" can be a cosy whodunnit or a harrowing slow-burn). An LLM picks them from a fixed vocabulary,
from the title's description and what it knows about the title, so they can be compared like any other feature.

Made by the chat LLM's host (scripts/llm/tag_titles.py), not by the nightly refresh, and cached in
data/cache/title_style.parquet with a hash of the text each title was tagged from (and of the vocabulary), so a title
is tagged again only when either changes.
"""
import hashlib
import json
from pathlib import Path
from typing import Callable, Optional

import pandas as pd

# facet -> (most tags per title, options). Plain words, so the tags read well in chat answers and explanations
FACETS: dict[str, tuple[int, tuple[str, ...]]] = {
    "mood": (3, (
        "dark", "bleak", "gritty", "tense", "eerie", "disturbing", "melancholic", "bittersweet", "emotional",
        "heartwarming", "uplifting", "feel-good", "cozy", "lighthearted", "playful", "whimsical", "quirky", "romantic",
        "nostalgic", "mysterious", "thought-provoking", "inspiring", "exciting", "epic", "chaotic", "cynical",
    )),
    "pacing": (1, ("slow-burn", "measured", "brisk", "frenetic")),
    "humour": (2, ("none", "gentle", "dry", "witty", "satirical", "absurd", "slapstick", "cringe", "crude", "dark",
                   "parody")),
    "intensity": (1, ("gentle", "moderate", "intense", "harrowing")),
    "attention": (1, ("easy watch", "engaging", "demanding")),
    "world": (1, ("realistic", "heightened", "fantastical", "surreal")),
    "story": (2, ("character-driven", "plot-driven", "ensemble", "episodic", "serialized", "anthology", "spectacle",
                  "dialogue-heavy", "twisty")),
    "audience": (1, ("kids", "family", "teens", "adults")),
}

# `feel` comes first: describing the experience in a sentence before picking tags gave somewhat more fitting tags than
# picking straight away, where qwen3:8b leaned on the first options listed (Oct 2026)
SCHEMA = {
    "type": "object",
    "properties": {
        "feel": {"type": "string"},
        **{f: {"type": "array", "items": {"type": "string", "enum": list(options)}, "maxItems": most}
           for f, (most, options) in FACETS.items()},
    },
    "required": ["feel", *FACETS],
}

SYSTEM = (
    "You tag films and TV series by how they feel to watch, for a recommender: the mood, pace, humour and intensity "
    "of the experience, not the plot or genre. Use what you know about the title as well as the details given.\n\n"
    "First write `feel`: one sentence on what watching it is like, as a friend would put it to someone choosing what "
    "to watch tonight. Then pick the tags that fit that, most fitting first, up to the number allowed for each facet. "
    "Only pick a tag when it's central to the experience: most titles aren't melancholic, bittersweet or slow-burn.\n"
    "Pacing: slow-burn = builds deliberately slowly; measured = unhurried; brisk = moves quickly; frenetic = "
    "relentless. Humour: none = no real comedy.\n\nFacets (most tags) and options:\n"
    + "\n".join(f"- {f} ({most}): {', '.join(options)}" for f, (most, options) in FACETS.items())
)

DEFAULT_PATH = "data/cache/title_style.parquet"
# changing the vocabulary or the prompt invalidates every cached title
VERSION = hashlib.sha1((SYSTEM + json.dumps(SCHEMA, sort_keys=True)).encode()).hexdigest()[:8]


def messages(text: str) -> list[dict]:
    """The chat messages asking for one title's tags (answer with `SCHEMA` as the reply format)"""
    return [{"role": "system", "content": SYSTEM}, {"role": "user", "content": text}]


def clean(answer: dict) -> dict:
    """
    `feel` (the sentence), then per facet only known options, the first few, no duplicates (a model can ignore the
    schema's limits)
    """
    feel = answer.get("feel")
    out: dict = {"feel": feel.strip() if isinstance(feel, str) else ""}
    for f, (most, options) in FACETS.items():
        picked = answer.get(f) or []
        picked = [picked] if isinstance(picked, str) else picked
        out[f] = list(dict.fromkeys(t for t in picked if t in options))[:most]
    return out


class StyleTags:
    """Style tags per title (see module docstring), by t_const"""

    def __init__(self, path: str = DEFAULT_PATH):
        self.path = Path(path)
        self._hash: dict[str, str] = {}
        self._tags: dict[str, dict] = {}
        if self.path.exists():
            df = pd.read_parquet(self.path)
            for t, h, tags in zip(df["t_const"], df["doc_hash"], df["tags"]):
                self._hash[t], self._tags[t] = h, json.loads(tags)

    @staticmethod
    def _digest(text: str) -> str:
        return hashlib.sha1(f"{VERSION}\n{text}".encode()).hexdigest()[:16]

    def missing(self, texts: pd.Series) -> pd.Series:
        """The texts not tagged yet, or tagged from a different text or vocabulary"""
        return texts[[self._hash.get(t) != self._digest(x) for t, x in texts.items()]]

    def update(self, texts: pd.Series, ask: Callable[[str], dict], save_every: int = 25,
               log: Callable[[str], None] = print) -> int:
        """
        Tags what's missing (`missing()`), one title per `ask(text)` (the LLM's answer as a dict), saving every
        `save_every` titles so an interrupted run keeps its progress. A title whose answer isn't valid JSON is skipped
        (and asked again next time); anything else `ask` raises, such as the host going away, stops the run.

        Returns:
            int: titles tagged
        """
        todo = self.missing(texts)
        done = 0
        for i, (t, text) in enumerate(todo.items(), 1):
            try:
                tags = clean(ask(text))
            except json.JSONDecodeError as e:
                log(f"[WARN] {t}: the answer wasn't JSON ({e}), skipped")
                continue
            self._hash[t], self._tags[t] = self._digest(text), tags
            done += 1
            if i % save_every == 0 or i == len(todo):
                self.save()
                log(f"[INFO] tagged {i}/{len(todo)} titles")
        return done

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        pd.DataFrame({"t_const": list(self._tags), "doc_hash": [self._hash[t] for t in self._tags],
                      "tags": [json.dumps(v) for v in self._tags.values()]}).to_parquet(tmp, index=False)
        tmp.replace(self.path)

    def get(self, t_const: str) -> Optional[dict]:
        """`clean()` answer for a title, or None if untagged"""
        return self._tags.get(t_const)

    def tokens(self, t_consts) -> list[list[str]]:
        """'facet: tag' tokens per title, e.g. ['mood: dark', 'pacing: slow-burn'] (empty if untagged)"""
        return [[f"{f}: {tag}" for f in FACETS for tag in self._tags.get(t, {}).get(f, [])] for t in t_consts]

    def attach(self, content: pd.DataFrame) -> pd.DataFrame:
        """`content` (`PreProcess.get_title_content()`) with a `style` column of `tokens()`, for `ContentKNN`"""
        return content.assign(style=self.tokens(content["t_const"]))

    def __len__(self) -> int:
        return len(self._tags)
