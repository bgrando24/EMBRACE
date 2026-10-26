import json
import re
from datetime import date
from typing import Callable, Optional

from .llm import OllamaClient
from .tools import LibraryTools

# exchanges (a question, the tool calls it caused, and the answer) kept as context for follow-up questions
MAX_EXCHANGES = 6
# tool-call rounds per question before the model must answer with what it has
MAX_TOOL_ROUNDS = 5

SYSTEM_PROMPT = """You are EMBRACE, the movie and TV recommendation assistant for a private Emby media server. \
You're chatting with {user_name} on Discord. Today is {today}.

How to work:
- Look things up with your tools before recommending. Only recommend titles your tools returned: never make up \
titles, years, plots or people, and describe a title only from what the tools said about it (about, keywords, genres).
- Everything you recommend should be on the server, and something they haven't watched unless they ask otherwise.
- For "something like X": call search_library with like="X" (X doesn't have to be on the server) and a query of \
keywords for what they liked about X (its tone, genre, setting, themes).
- For a mood or description ("dark, slow-burn crime show"), use search_library with descriptive keywords, including \
the genre ("funny" or "comedy", "scary", "thriller"); if the results are thin or off, search again with different words.
- For "what should I watch?" with nothing else to go on, use my_recommendations.
- When they refine the request ("darker", "funnier", "shorter"), search again with keywords for the new request. \
Searches leave out titles you already suggested, so suggest new ones; never repeat earlier picks unless they ask.
- taste_fit (0-100) is how well a title matches their viewing history compared with the rest of the library; \
use it to choose between titles that fit the request equally well.
- If a title isn't on the server, say so: they can request it in Jellyseerr.

How to answer:
- Choose the 3-5 results that best fit what they asked for, and leave out weak matches: never just list \
everything a tool returned, and never list a title twice.
- Short and friendly, for a chat app: each pick as "**Title (year)** - one line on why it fits what they asked \
for". No tables, no headings.
- If they say something is wrong or they've seen it, take it on board in your next answer."""


class ChatSession:
    """
    One person's conversation with the chat-bot: their messages, the model's tool calls (`LibraryTools`) and answers.
    Keeps the last MAX_EXCHANGES exchanges for follow-ups ("something shorter?", "seen it"), and the titles it has
    suggested, which searches then leave out: a small model repeats its earlier picks otherwise, however it's told.
    """

    def __init__(self, llm: OllamaClient, tools: LibraryTools, user_id: Optional[str], user_name: str,
                 log: Callable[[str], None] = lambda _: None):
        """
        Args:
            llm (OllamaClient): the model
            tools (LibraryTools): what it can look up
            user_id (str, optional): canonical Emby user ID the personal tools use (None: no personal data)
            user_name (str): what the model calls them
            log (callable, optional): receives one line per tool call, for debugging
        """
        self.llm, self.tools, self.user_id, self.user_name, self.log = llm, tools, user_id, user_name, log
        self.exchanges: list[list[dict]] = []
        self.suggested: set[str] = set()

    def reset(self) -> None:
        self.exchanges, self.suggested = [], set()

    def ask(self, text: str) -> str:
        """
        The model's answer to a message, after any tool calls it makes.

        Raises:
            LLMUnavailable: the LLM isn't running (the conversation so far is kept)
        """
        system = {"role": "system", "content": SYSTEM_PROMPT.format(user_name=self.user_name, today=date.today().isoformat())}
        exchange = [{"role": "user", "content": text}]
        schemas = self.tools.schemas()
        for round_ in range(MAX_TOOL_ROUNDS + 1):
            history = [m for e in self.exchanges[-MAX_EXCHANGES:] for m in e]
            # out of rounds: no tools offered, so the model has to answer with what it found
            reply = self.llm.chat([system, *history, *exchange], tools=schemas if round_ < MAX_TOOL_ROUNDS else None)
            calls = reply.get("tool_calls") or []
            exchange.append({"role": "assistant", "content": reply.get("content") or "", **({"tool_calls": calls} if calls else {})})
            if not calls:
                break
            for call in calls:
                name, args = call["function"]["name"], call["function"].get("arguments") or {}
                if isinstance(args, str):       # some models send the arguments JSON-encoded
                    args = json.loads(args or "{}")
                result = self.tools.call(name, args, self.user_id, exclude=self.suggested)
                self.log(f"{name}({json.dumps(args, ensure_ascii=False)}) -> {len(result)} chars")
                exchange.append({"role": "tool", "tool_name": name, "content": result})
        self.exchanges.append(exchange)
        answer = self.clean(exchange[-1]["content"])
        self.suggested |= self.picks_in(answer, exchange)
        return answer

    @staticmethod
    def clean(text: str) -> str:
        """The answer without any reasoning a thinking model left in it"""
        return re.sub(r"<think>.*?</think>", "", text or "", flags=re.S).strip() or "(no answer - try rephrasing?)"

    @staticmethod
    def picks_in(answer: str, exchange: list[dict]) -> set[str]:
        """
        The titles from this exchange's tool results (not watch history) that the answer suggests: named in bold, as
        the prompt asks for picks, or anywhere if nothing is bold
        """
        found = set()
        for m in exchange:
            if m["role"] == "tool" and m["tool_name"] != "my_watch_history":
                try:
                    found |= {r["title"] for r in json.loads(m["content"]).get("results", []) if "title" in r}
                except (ValueError, AttributeError):
                    pass
        where = " | ".join(re.findall(r"\*\*(.+?)\*\*", answer)) or answer
        return {t for t in found
                if re.search(rf"(?<!\w){re.escape(re.sub(r' \(\d{4}\)$', '', t))}(?!\w)", where, re.I)}
