"""
EMBRACE on Discord: DM the bot (or @mention it in a server channel) to chat with the recommender. Each Discord user
is linked to their Emby account (EMBRACE_DISCORD_USERS), so it knows their watch history; anyone not linked is told
how to get linked. The LLM is a llama.cpp server on another machine that's also used for other things
(EMBRACE_LLM_URL), so it may be off or busy: the bot then says the AI side isn't available and carries on.
Runs as a service on the EMBRACE host (scripts/systemd/embrace-chat.service); see docs/modules/chat.md.

usage (from project root): PYTHONPATH=src python scripts/chat/discord_bot.py
"""
import asyncio
import os
import re
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Optional

import discord
from dotenv import load_dotenv

from chat import ChatSession, LibraryTools, LLMClient, LLMUnavailable, TitleEmbeddings, tmdb_lookup
from connectors import EmbyConnector, TMDBConnector
from ml import PreProcess, Recommender
from utils import Notifications

ROOT_DIR = Path(__file__).resolve().parents[2]
os.chdir(ROOT_DIR)  # SQLiteConnector resolves "sqlite_db/" relative to the working directory
load_dotenv(ROOT_DIR / ".env")

# a conversation idle for this long starts afresh (a new question is usually a new topic)
IDLE_RESET_SECONDS = 2 * 3600
# how often to try embedding new titles while the LLM server is away, so search by meaning catches up without a message
EMBED_RETRY_SECONDS = 15 * 60
HELP = (
    "Ask me for something to watch, e.g. *\"a funny show like Letterkenny\"*, *\"a tense thriller under 2 hours\"* "
    "or just *\"what should I watch tonight?\"*. I know what's on the server, a lot that isn't (you can request "
    "it), and what you've watched.\n"
    "`reset` starts a new conversation, `status` shows whether my AI side is available."
)
OFFLINE = (
    "My AI side isn't available right now (its server is off or busy), so I can't chat at the moment. "
    "Your daily picks still arrive as usual - try me again a bit later."
)


def log(msg: str) -> None:
    print(msg, flush=True)


def parse_user_links(raw: Optional[str]) -> dict[int, str]:
    """EMBRACE_DISCORD_USERS ("discord_id:emby_name,...") -> {discord user ID: Emby user name}"""
    links = {}
    for pair in (raw or "").split(","):
        if ":" in pair:
            discord_id, name = pair.split(":", 1)
            if discord_id.strip().isdigit() and name.strip():
                links[int(discord_id.strip())] = name.strip()
    return links


class Brain:
    """
    The recommender data and the chat tools, reloaded when the nightly refresh has updated the database, and the
    title embeddings for search by meaning. Those are made by the LLM server, so `embed_missing()` runs in the
    background whenever something isn't embedded yet, and waits for the server when it's away; until then search
    matches words only.
    """

    def __init__(self, llm: LLMClient):
        self.db_path = ROOT_DIR / "sqlite_db" / (os.getenv("SQLITE_DB_NAME") or "EMBRACE_SQLITE_DB.db")
        self.tools: Optional[LibraryTools] = None
        self.loaded_mtime = 0.0
        self.loaded_at = 0.0
        self.emby_ids: dict[str, str] = {}
        # titles the server never had ("something like X") are looked up on TMDB; without a token, they're unknown
        token = os.getenv("TMDB_READ_ACCESS_TOKEN")
        self.lookup = tmdb_lookup(TMDBConnector(token)) if token else None
        self.llm = llm
        self.embeddings = TitleEmbeddings(llm.embed_model)
        self.embeddings_done = False        # every title of the loaded tools is embedded and in use
        self.embeddings_waiting = False     # the last try found the LLM server away (logged once, not every retry)
        self._embedding = threading.Lock()

    def stale(self) -> bool:
        try:
            return self.tools is None or self.db_path.stat().st_mtime > self.loaded_mtime + 60
        except OSError:
            return self.tools is None

    def load(self) -> None:
        mtime = self.db_path.stat().st_mtime if self.db_path.exists() else 0.0
        pp = PreProcess()
        rec = Recommender.load(pp, log=log, discovery=True)
        self.tools = LibraryTools(rec, runtimes=pp.get_title_runtimes(), lookup=self.lookup)
        self.embeddings_done = False
        # what's cached already works now, before anything new is embedded; if an embedding run is busy, it's left to
        # the next one (never make a message wait minutes for it)
        if self._embedding.acquire(blocking=False):
            try:
                if len(self.embeddings):
                    self.tools.use_embeddings(self.embeddings, self.llm.embed)
            finally:
                self._embedding.release()
        try:
            self.emby_ids = {name.casefold(): uid for name, uid in EmbyConnector().get_all_emby_users().items()}
        except Exception as e:     # Emby down: fall back to the names in watch history
            log(f"[WARN] Emby users unavailable ({type(e).__name__}: {e})")
        self.loaded_mtime, self.loaded_at = mtime, time.time()
        log(f"[INFO] loaded {len(self.tools.titles)} titles ({len(self.tools.library)} on the server)")

    def embed_missing(self) -> None:
        """Embeds the titles that aren't yet (all ~12k the first time), then searches with them. Needs the server."""
        if not self._embedding.acquire(blocking=False):
            return      # already running
        try:
            tools = self.tools
            texts = TitleEmbeddings.texts(tools.titles)
            todo = len(self.embeddings.missing(texts))
            if todo:
                log(f"[INFO] embedding {todo} titles for search by meaning ({self.embeddings.model})")
                self.embeddings.update(texts, self.llm.embed, log=log)
            tools.use_embeddings(self.embeddings, self.llm.embed)
            self.embeddings_done = tools is self.tools
            self.embeddings_waiting = False
        except LLMUnavailable as e:
            if not self.embeddings_waiting:
                log(f"[INFO] title embeddings wait for the LLM server ({e}); retrying every {EMBED_RETRY_SECONDS // 60} min")
            self.embeddings_waiting = True
        except Exception as e:
            log(f"[WARN] embedding titles failed ({type(e).__name__}: {e}); search matches words until it works")
        finally:
            self._embedding.release()

    def user_id(self, emby_name: str) -> Optional[str]:
        """Canonical Emby user ID for a name: watch history's (aliases resolved) first, then Emby's current users"""
        users = self.tools.rec.profiles.users()
        match = users[users["user_name"].str.casefold() == emby_name.casefold()]
        return match["user_id"].iloc[0] if len(match) else self.emby_ids.get(emby_name.casefold())


class EmbraceBot(discord.Client):

    def __init__(self, links: dict[int, str]):
        # the default intents are enough: DMs and messages that mention the bot carry their text without the
        # privileged message-content intent
        super().__init__(intents=discord.Intents.default())
        self.links = links
        self.llm = LLMClient()
        self.brain = Brain(self.llm)
        self.background: set[asyncio.Task] = set()
        self.sessions: dict[tuple[int, int], tuple[ChatSession, float]] = {}
        self.llm_lock = asyncio.Lock()      # the server answers one request at a time: so do we

    async def setup_hook(self) -> None:
        await asyncio.to_thread(self.brain.load)
        self.embed_in_background()
        self.background.add(asyncio.create_task(self.retry_embeddings()))

    async def retry_embeddings(self) -> None:
        """Search by meaning catches up after the LLM server was away, even if nobody's chatting"""
        while True:
            await asyncio.sleep(EMBED_RETRY_SECONDS)
            self.embed_in_background()

    def embed_in_background(self) -> None:
        """Start `Brain.embed_missing()` unless everything's embedded; it returns within seconds if the server's away"""
        if not self.brain.embeddings_done:
            task = asyncio.create_task(asyncio.to_thread(self.brain.embed_missing))
            self.background.add(task)       # keep a reference until it's done
            task.add_done_callback(self.background.discard)

    async def on_ready(self) -> None:
        log(f"[INFO] logged in as {self.user} - {len(self.links)} linked users, LLM server {self.llm.url}")

    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return
        is_dm = isinstance(message.channel, discord.DMChannel)
        if not is_dm and self.user not in message.mentions:
            return
        text = re.sub(r"<@!?\d+>", "", message.content).strip()
        command = text.casefold().strip("!/ .")

        emby_name = self.links.get(message.author.id)
        if emby_name is None:
            log(f"[INFO] unlinked Discord user {message.author} ({message.author.id})")
            await message.reply("I don't know who you are on Emby yet. Ask the server admin to link your Discord "
                                f"ID `{message.author.id}` to your Emby name (EMBRACE_DISCORD_USERS).")
            return
        if not text or command == "help":
            await message.reply(HELP)
            return
        if command == "status":
            await message.reply(await asyncio.to_thread(self.status))
            return

        key = (message.channel.id, message.author.id)
        if command in ("reset", "new chat", "start over"):
            self.sessions.pop(key, None)
            await message.reply("Fresh start - what are you in the mood for?")
            return

        async with message.channel.typing():
            try:
                async with self.llm_lock:
                    if self.brain.stale():
                        await asyncio.to_thread(self.brain.load)
                    session = self.session(key, emby_name)
                    answer = await asyncio.to_thread(session.ask, text)
            except LLMUnavailable as e:
                log(f"[INFO] {e}")
                answer = OFFLINE
            except Exception as e:
                traceback.print_exc()
                answer = f"Something went wrong on my end ({type(e).__name__}) - the details are in my log."
        for part in Notifications.split_message(answer.split("\n")):    # Discord's 2000-character limit
            await message.reply(part, mention_author=False)
        self.embed_in_background()      # new data may have been loaded: embed what's new

    def session(self, key: tuple[int, int], emby_name: str) -> ChatSession:
        """The conversation in this channel with this person: continued unless idle, always on the latest tools"""
        found = self.sessions.get(key)
        if found and time.time() - found[1] < IDLE_RESET_SECONDS:
            session = found[0]
            session.tools = self.brain.tools
        else:
            session = ChatSession(self.llm, self.brain.tools, self.brain.user_id(emby_name), emby_name, log=log)
        self.sessions[key] = (session, time.time())
        return session

    def status(self) -> str:
        try:
            llm = f"AI: **available** ({self.llm.chat_model()})"
        except LLMUnavailable:
            llm = "AI: **not available right now** - its server is off or busy. Daily picks aren't affected."
        age = (time.time() - self.brain.loaded_at) / 3600
        search = ("by meaning and words" if self.brain.embeddings_done else
                  f"by words only for now ({len(self.brain.embeddings)} titles embedded)")
        return f"{llm}\nLibrary data loaded {age:.1f} h ago. Search: {search}."


def main():
    token = os.getenv("DISCORD_BOT_TOKEN")
    if not token:
        log("[ERROR] DISCORD_BOT_TOKEN isn't set (see docs/modules/chat.md)")
        sys.exit(1)
    links = parse_user_links(os.getenv("EMBRACE_DISCORD_USERS"))
    if not links:
        log("[WARN] EMBRACE_DISCORD_USERS is empty: the bot will only tell people their Discord ID")
    EmbraceBot(links).run(token)


if __name__ == "__main__":
    main()
