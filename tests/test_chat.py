import json
import unittest
from unittest import mock

import pandas as pd
import requests

from chat import ChatSession, LibraryTools, LLMUnavailable, OllamaClient
from chat.session import MAX_TOOL_ROUNDS
from ml import ContentKNN, Freshness, Recommender, Requests, UserProfiles
from test_content_knn import CONTENT
from test_freshness import ADDITIONS
from test_tmdb_content import response
from test_picks import watched

# tt5 is a show, tt8 (no features) and tt6 aren't on the server
CHAT_CONTENT = CONTENT.assign(type=CONTENT["type"].where(CONTENT["t_const"] != "tt5", "tvSeries"))
LIBRARY = {"tt1", "tt2", "tt3", "tt4", "tt5", "tt7", "tt8"}


def make_tools() -> LibraryTools:
    knn = ContentKNN(CHAT_CONTENT)
    history = pd.DataFrame([
        {**watched("tt1"), "last_watched_timestamp": "2026-01-02 20:00:00"},
        {**watched("tt3", hours=1.0), "last_watched_timestamp": "2026-03-05 20:00:00"},
    ])
    rec = Recommender(knn=knn, profiles=UserProfiles(knn, history), library=LIBRARY, freshness=Freshness(ADDITIONS),
                      requests=Requests(pd.DataFrame(columns=["user_id", "t_const", "requested_at"])), content=CHAT_CONTENT)
    return LibraryTools(rec)


def titles(result: str) -> list[str]:
    return [r["title"] for r in json.loads(result)["results"]]


class TestLibraryTools(unittest.TestCase):

    def setUp(self):
        self.tools = make_tools()

    def test_search_finds_by_description_and_skips_watched_and_off_server_titles(self):
        self.assertEqual(titles(self.tools.call("search_library", {"query": "bank heist crew"}, "u1"))[0], "Heist Two (2000)")
        found = titles(self.tools.call("search_library", {"query": "family detective bank", "limit": 15}, "u1"))
        self.assertNotIn("Heist One (2000)", found)        # watched
        self.assertNotIn("Korean Drama (2000)", found)     # not on the server
        with_watched = titles(self.tools.call("search_library", {"query": "bank", "include_watched": True}, "u1"))
        self.assertIn("Heist One (2000)", with_watched)

    def test_search_like_a_title_blends_in_similarity_to_it(self):
        out = json.loads(self.tools.call("search_library", {"query": "funny", "like": "Heist One", "include_watched": True}, "u1"))
        self.assertEqual(out["like"], "Heist One (2000)")
        found = [r["title"] for r in out["results"]]
        self.assertNotIn("Heist One (2000)", found)                       # never the title itself
        self.assertEqual(found[0], "Heist Two (2000)")                    # not funny, but very like Heist One
        self.assertIn("heist", out["results"][0]["in_common"])
        # an unknown `like` is reported, and the text search still runs
        out = json.loads(self.tools.call("search_library", {"query": "funny", "like": "Nope Nothing"}, "u1"))
        self.assertIn("isn't a title", out["like"])
        self.assertTrue(out["results"])

    def test_kind_filter(self):
        self.assertEqual(titles(self.tools.call("search_library", {"query": "funny", "kind": "show"}, "u1")), ["Sitcom (2000)"])

    def test_titles_resolve_as_people_type_them(self):
        self.assertEqual(json.loads(self.tools.call("title_info", {"title": "the HEIST one!"}, "u1"))["title"], "Heist One (2000)")
        info = json.loads(self.tools.call("title_info", {"title": "Heist On"}, "u1"))        # close enough
        self.assertEqual(info["title"], "Heist One (2000)")
        self.assertEqual(info["you_watched"]["hours_watched"], 2.0)
        self.assertEqual(info["crew"], ["Dir (director)"])
        self.assertIn("error", json.loads(self.tools.call("title_info", {"title": "Trailer Park Boys"}, "u1")))

    def test_similar_titles_are_on_the_server_and_say_why(self):
        out = json.loads(self.tools.call("similar_titles", {"title": "Heist One", "include_watched": True}, "u1"))
        self.assertEqual(out["similar_to"], "Heist One (2000)")
        self.assertEqual(out["results"][0]["title"], "Heist Two (2000)")
        self.assertIn("heist", out["results"][0]["in_common"])
        self.assertNotIn("Korean Drama (2000)", [r["title"] for r in out["results"]])

    def test_history_orders(self):
        recent = titles(self.tools.call("my_watch_history", {"order": "recent"}, "u1"))
        self.assertEqual(recent, ["Popular Crime (2000)", "Heist One (2000)"])
        self.assertEqual(titles(self.tools.call("my_watch_history", {}, "u1"))[0], "Heist One (2000)")
        self.assertEqual(titles(self.tools.call("my_watch_history", {}, "nobody")), [])

    def test_recommendations_need_history(self):
        # 4 unwatched titles: 2 fresh, so the (never overlapping) back catalogue has the rest
        self.assertEqual(len(titles(self.tools.call("my_recommendations", {"list": "back_catalogue", "limit": 2}, "u1"))), 2)
        self.assertIn("error", json.loads(self.tools.call("my_recommendations", {}, "nobody")))

    def test_bad_calls_come_back_as_errors(self):
        self.assertIn("error", json.loads(self.tools.call("no_such_tool", {}, "u1")))
        # arguments a tool doesn't take are dropped, rather than failing the call
        self.assertTrue(titles(self.tools.call("search_library", {"query": "bank", "colour": "red"}, "u1")))
        self.assertEqual({s["function"]["name"] for s in self.tools.schemas()},
                         {"search_library", "similar_titles", "title_info", "my_recommendations", "my_watch_history"})


class FakeLLM:
    """Replies from a script; records what it was sent"""

    def __init__(self, replies):
        self.replies, self.sent = list(replies), []

    def chat(self, messages, tools=None):
        self.sent.append((messages, tools))
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply


def tool_call(name, **args):
    return {"role": "assistant", "content": "", "tool_calls": [{"function": {"name": name, "arguments": args}}]}


class TestChatSession(unittest.TestCase):

    def test_tool_results_go_back_to_the_model_and_the_conversation_is_kept(self):
        llm = FakeLLM([tool_call("search_library", query="bank heist"), {"content": "<think>hm</think>Try **Heist Two**"},
                       {"content": "It's a heist movie."}])
        session = ChatSession(llm, make_tools(), "u1", "Sam")
        self.assertEqual(session.ask("a heist movie?"), "Try **Heist Two**")
        sent = llm.sent[1][0]
        self.assertEqual(sent[-1]["role"], "tool")
        self.assertIn("Heist Two", sent[-1]["content"])
        self.assertIn("Sam", sent[0]["content"])
        session.ask("what's it about?")
        self.assertEqual([m["content"] for m in llm.sent[2][0] if m["role"] == "user"], ["a heist movie?", "what's it about?"])

    def test_after_max_rounds_the_model_must_answer(self):
        llm = FakeLLM([tool_call("search_library", query="x")] * MAX_TOOL_ROUNDS + [{"content": "done"}])
        self.assertEqual(ChatSession(llm, make_tools(), "u1", "Sam").ask("?"), "done")
        self.assertIsNone(llm.sent[-1][1])
        self.assertTrue(all(tools for _, tools in llm.sent[:-1]))

    def test_llm_down_keeps_the_conversation(self):
        session = ChatSession(FakeLLM([LLMUnavailable("off")]), make_tools(), "u1", "Sam")
        with self.assertRaises(LLMUnavailable):
            session.ask("hello")
        self.assertEqual(session.exchanges, [])


class TestOllamaClient(unittest.TestCase):

    def test_first_host_that_answers_is_used_and_remembered(self):
        def get(url, timeout):
            if url.startswith("http://down"):
                raise requests.ConnectionError()
            return response(200, {"version": "1"})
        llm = OllamaClient(hosts="http://down:1, http://up:2/", model="m")
        with mock.patch("chat.llm.requests.get", side_effect=get) as g, \
             mock.patch("chat.llm.requests.post", return_value=response(200, {"message": {"content": "hi"}})) as p:
            self.assertEqual(llm.chat([{"role": "user", "content": "hi"}])["content"], "hi")
            llm.chat([])
        self.assertEqual(llm.host, "http://up:2")
        self.assertEqual(g.call_count, 2)       # found once, then remembered
        self.assertEqual(p.call_args.args[0], "http://up:2/api/chat")
        self.assertEqual(p.call_args.kwargs["json"]["options"]["num_ctx"], llm.num_ctx)

    def test_no_host_or_no_model_is_unavailable(self):
        llm = OllamaClient(hosts="http://down:1", model="m")
        with mock.patch("chat.llm.requests.get", side_effect=requests.ConnectionError()):
            with self.assertRaises(LLMUnavailable):
                llm.chat([])
        with mock.patch("chat.llm.requests.get", return_value=response(200, {})), \
             mock.patch("chat.llm.requests.post", return_value=response(404, {"error": "model 'm' not found"})):
            with self.assertRaisesRegex(LLMUnavailable, "isn't pulled"):
                llm.chat([])


if __name__ == "__main__":
    unittest.main()
