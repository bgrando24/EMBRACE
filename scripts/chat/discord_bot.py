"""
EMBRACE on Discord: DM the bot (or @mention it in a server channel) to chat with the recommender. Each Discord user
is linked to their Emby account (EMBRACE_DISCORD_USERS), so it knows their watch history; anyone not linked is told
how to get linked. The LLM runs elsewhere and only when started (scripts/llm/llm.sh); when it's off, the bot says so.
Runs as a service on the EMBRACE host (scripts/systemd/embrace-chat.service); see docs/modules/chat.md.

usage (from project root): PYTHONPATH=src python scripts/chat/discord_bot.py
"""
import asyncio
import os
import re
import sys
import time
import traceback
from pathlib import Path
from typing import Optional

import discord
from dotenv import load_dotenv

from chat import ChatSession, LibraryTools, LLMUnavailable, OllamaClient
from connectors import EmbyConnector
from ml import PreProcess, Recommender
from utils import Notifications

ROOT_DIR = Path(__file__).resolve().parents[2]
os.chdir(ROOT_DIR)  # SQLiteConnector resolves "sqlite_db/" relative to the working directory
load_dotenv(ROOT_DIR / ".env")

# a conversation idle for this long starts afresh (a new question is usually a new topic)
IDLE_RESET_SECONDS = 2 * 3600
HELP = (
    "Ask me for something to watch, e.g. *\"a funny show like Letterkenny\"*, *\"a tense thriller under 2 hours\"* "
    "or just *\"what should I watch tonight?\"*. I know what's on the server and what you've watched.\n"
    "`reset` starts a new conversation, `status` shows whether my LLM is running."
)
OFFLINE = (
    "My LLM isn't running at the moment, so I can't chat. Start it with `scripts/llm/llm.sh start` on one of its "
    "machines (or `scripts/llm/llm.sh start <ssh host>` from anywhere), then ask again."
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
    """The recommender data and the chat tools, reloaded when the nightly refresh has updated the database"""

    def __init__(self):
        self.db_path = ROOT_DIR / "sqlite_db" / (os.getenv("SQLITE_DB_NAME") or "EMBRACE_SQLITE_DB.db")
        self.tools: Optional[LibraryTools] = None
        self.loaded_mtime = 0.0
        self.loaded_at = 0.0
        self.emby_ids: dict[str, str] = {}

    def stale(self) -> bool:
        try:
            return self.tools is None or self.db_path.stat().st_mtime > self.loaded_mtime + 60
        except OSError:
            return self.tools is None

    def load(self) -> None:
        mtime = self.db_path.stat().st_mtime if self.db_path.exists() else 0.0
        rec = Recommender.load(PreProcess(), log=log)
        self.tools = LibraryTools(rec)
        try:
            self.emby_ids = {name.casefold(): uid for name, uid in EmbyConnector().get_all_emby_users().items()}
        except Exception as e:     # Emby down: fall back to the names in watch history
            log(f"[WARN] Emby users unavailable ({type(e).__name__}: {e})")
        self.loaded_mtime, self.loaded_at = mtime, time.time()
        log(f"[INFO] loaded {len(self.tools.titles)} titles ({len(self.tools.library)} on the server)")

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
        self.brain = Brain()
        self.llm = OllamaClient()
        self.sessions: dict[tuple[int, int], tuple[ChatSession, float]] = {}
        self.llm_lock = asyncio.Lock()      # one GPU: questions are answered one at a time

    async def setup_hook(self) -> None:
        await asyncio.to_thread(self.brain.load)

    async def on_ready(self) -> None:
        log(f"[INFO] logged in as {self.user} - {len(self.links)} linked users, LLM hosts {self.llm.hosts}")

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
        host = self.llm.find_host()
        llm = f"LLM: **running** ({self.llm.model} at {host})" if host else "LLM: **off** - " + OFFLINE
        age = (time.time() - self.brain.loaded_at) / 3600
        return f"{llm}\nLibrary data loaded {age:.1f} h ago."


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
