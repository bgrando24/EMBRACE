import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from chat import LLMClient
from ml import ContentKNN
from ml.style_tags import SCHEMA, StyleTags, clean, messages
from test_chat import models
from test_content_knn import CONTENT
from test_tmdb_content import response

DARK = {"feel": "A grim, tense ride.", "mood": ["dark", "tense"], "pacing": ["slow-burn"], "humour": ["none"]}
LIGHT = {"feel": "Silly fun.", "mood": ["playful"], "pacing": ["brisk"], "humour": ["slapstick"]}


class TestClean(unittest.TestCase):

    def test_only_known_options_the_first_few_without_repeats(self):
        tags = clean({"feel": " Fun. ", "mood": ["dark", "dark", "made-up", "tense", "cozy", "epic"],
                      "pacing": "brisk", "story": None})
        self.assertEqual(tags["feel"], "Fun.")
        self.assertEqual(tags["mood"], ["dark", "tense", "cozy"])
        self.assertEqual(tags["pacing"], ["brisk"])
        self.assertEqual(tags["story"], [])

    def test_the_schema_asks_for_the_sentence_first(self):
        self.assertEqual(next(iter(SCHEMA["properties"])), "feel")
        self.assertIn("pacing (1): slow-burn", messages("Heat (1995).")[0]["content"])


class TestStyleTags(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = str(Path(self.dir.name) / "style.parquet")
        self.asked = []

    def tearDown(self):
        self.dir.cleanup()

    def ask(self, text):
        self.asked.append(text)
        if "broken" in text:
            raise json.JSONDecodeError("bad", text, 0)
        return DARK if "grim" in text else LIGHT

    def test_tags_only_whats_new_or_changed_and_keeps_it(self):
        store = StyleTags(self.path)
        texts = pd.Series({"tt1": "a grim heist", "tt2": "a silly comedy"})
        self.assertEqual(store.update(texts, self.ask, log=lambda _: None), 2)
        self.assertEqual(store.update(texts, self.ask, log=lambda _: None), 0)
        again = StyleTags(self.path)                                          # read back from the cache
        self.assertEqual(again.get("tt1")["feel"], "A grim, tense ride.")
        self.assertEqual(again.update(texts.replace("a silly comedy", "a silly romcom"), self.ask, log=lambda _: None), 1)
        self.assertEqual(self.asked[-1], "a silly romcom")

    def test_an_answer_that_isnt_json_is_skipped_and_asked_again_next_time(self):
        store = StyleTags(self.path)
        texts = pd.Series({"tt1": "broken", "tt2": "a grim one"})
        self.assertEqual(store.update(texts, self.ask, log=lambda _: None), 1)
        self.assertEqual(store.missing(texts).index.tolist(), ["tt1"])

    def test_tokens_are_facet_and_tag_without_the_sentence(self):
        store = StyleTags(self.path)
        store.update(pd.Series({"tt1": "grim"}), self.ask, log=lambda _: None)
        self.assertEqual(store.tokens(["tt1", "tt9"]),
                         [["mood: dark", "mood: tense", "pacing: slow-burn", "humour: none"], []])


class TestStyleBlock(unittest.TestCase):

    def test_titles_that_feel_alike_are_similar_on_style_alone(self):
        store = StyleTags(str(Path(tempfile.mkdtemp()) / "style.parquet"))
        texts = pd.Series({"tt1": "grim", "tt3": "grim", "tt5": "silly", "tt6": "silly"})
        store.update(texts, lambda t: DARK if t == "grim" else LIGHT, log=lambda _: None)
        content = store.attach(CONTENT)
        self.assertEqual(content.loc[content["t_const"] == "tt8", "style"].iloc[0], [])
        knn = ContentKNN(content, {b: float(b == "style") for b in ContentKNN.DEFAULT_WEIGHTS})
        self.assertEqual(knn.recommend("tt1", n=1)["t_const"].tolist(), ["tt3"])
        # off by default: the same neighbours as without tags
        self.assertEqual(ContentKNN(content).recommend("tt1", n=3)["t_const"].tolist(),
                         ContentKNN(CONTENT).recommend("tt1", n=3)["t_const"].tolist())


class TestStructuredChat(unittest.TestCase):

    def test_a_reply_format_is_sent_and_answered_at_temperature_0(self):
        llm = LLMClient(url="http://server:8080", embed_model="Embed-0.6B")
        with mock.patch("chat.llm.requests.get", return_value=response(200, models())), \
             mock.patch("chat.llm.requests.post", return_value=response(200, {"choices": [{"message": {"content": "{}"}}]})) as post:
            llm.chat(messages("x"), format=SCHEMA)
        body = post.call_args.kwargs["json"]
        self.assertEqual(body["response_format"]["json_schema"]["schema"], SCHEMA)
        self.assertEqual(body["temperature"], 0)


if __name__ == "__main__":
    unittest.main()
