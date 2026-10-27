import tempfile
import unittest
from unittest import mock

import numpy as np
import pandas as pd

from chat import LLMUnavailable, OllamaClient, TitleEmbeddings
from chat.embeddings import QUERY_INSTRUCTION, title_text
from test_chat import make_tools, titles
from test_tmdb_content import response

# a stand-in embedding model: one dimension per idea, so "caper" means the same as "heist" without sharing a word
IDEAS = {"heist": ("heist", "bank", "robbery", "caper"), "funny": ("funny", "sitcom", "laugh"),
         "family": ("family",), "detective": ("detective", "sleuth")}


def fake_embed(texts):
    calls.append(list(texts))
    rows = [[sum(t.lower().count(w) for w in words) for words in IDEAS.values()] for t in texts]
    e = np.asarray(rows, dtype=np.float32) + 1e-3
    return e / np.linalg.norm(e, axis=1, keepdims=True)


calls: list = []


class TestTitleText(unittest.TestCase):

    def test_says_what_the_title_is(self):
        people = [{"id": 1, "name": "Dir", "role": "director"}, {"id": 2, "name": "Star", "role": "cast"}]
        text = title_text("Heat", 1995, "movie", ["Crime"], ["heist"], people, "A crew plans a job.")
        self.assertEqual(text, "Heat (1995). Movie. Genres: Crime. Keywords: heist. Directed by Dir. Starring Star. "
                               "A crew plans a job.")
        self.assertEqual(title_text("S", None, "show", [], None, None, None), "S. TV series.")


class TestTitleEmbeddings(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        calls.clear()

    def tearDown(self):
        self.dir.cleanup()

    def test_embeds_only_whats_new_or_changed_and_keeps_it(self):
        store = TitleEmbeddings("m:1", self.dir.name)
        texts = pd.Series({"tt1": "a heist", "tt2": "a funny family"})
        self.assertEqual(store.update(texts, fake_embed), 2)
        self.assertEqual(store.update(texts, fake_embed), 0)                     # nothing new
        again = TitleEmbeddings("m:1", self.dir.name)                            # read back from the cache
        self.assertEqual(again.update(texts.replace("a heist", "a bank robbery"), fake_embed), 1)
        self.assertEqual(calls[-1], ["a bank robbery"])
        m = again.matrix(pd.Index(["tt2", "tt9"]))
        self.assertAlmostEqual(float(np.linalg.norm(m[0])), 1.0, places=5)
        self.assertFalse(m[1].any())                                             # not embedded: zeros
        self.assertIsNone(TitleEmbeddings("other", self.dir.name).matrix(pd.Index(["tt1"])))


class TestSearchByMeaning(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.tools = make_tools()
        store = TitleEmbeddings("m", self.dir.name)
        store.update(TitleEmbeddings.texts(self.tools.titles), fake_embed)
        self.store = store

    def tearDown(self):
        self.dir.cleanup()

    def test_a_query_with_no_words_in_common_finds_titles_by_meaning(self):
        self.assertEqual(titles(self.tools.call("search_library", {"query": "caper"}, "u1")), [])
        self.tools.use_embeddings(self.store, fake_embed)
        calls.clear()
        self.assertEqual(titles(self.tools.call("search_library", {"query": "caper"}, "u1"))[0], "Heist Two (2000)")
        self.assertEqual(calls, [[QUERY_INSTRUCTION + "caper"]])

    def test_falls_back_to_words_when_the_host_is_gone(self):
        def gone(texts):
            raise LLMUnavailable("off")
        self.tools.use_embeddings(self.store, gone)
        self.assertEqual(titles(self.tools.call("search_library", {"query": "bank heist crew"}, "u1"))[0], "Heist Two (2000)")


class TestOllamaEmbed(unittest.TestCase):

    def test_batches_and_normalises(self):
        llm = OllamaClient(hosts="http://up:1", model="m")
        llm.host = "http://up:1"
        bodies = []

        def post(url, json, timeout):
            bodies.append(json)
            return response(200, {"embeddings": [[3.0, 4.0]] * len(json["input"])})
        with mock.patch("chat.llm.EMBED_BATCH", 2), mock.patch("chat.llm.requests.post", side_effect=post):
            e = llm.embed(["a", "b", "c"])
        self.assertEqual([b["input"] for b in bodies], [["a", "b"], ["c"]])
        self.assertEqual(bodies[0]["model"], llm.embed_model)
        self.assertEqual(bodies[0]["options"]["num_gpu"], 0)                    # the GPU stays the chat model's
        np.testing.assert_allclose(e, [[0.6, 0.8]] * 3, rtol=1e-6)

    def test_model_not_pulled_is_unavailable(self):
        llm = OllamaClient(hosts="http://up:1", model="m")
        with mock.patch("chat.llm.requests.get", return_value=response(200, {})), \
             mock.patch("chat.llm.requests.post", return_value=response(404, {"error": "not found"})):
            with self.assertRaisesRegex(LLMUnavailable, "isn't pulled"):
                llm.embed(["a"])


if __name__ == "__main__":
    unittest.main()
