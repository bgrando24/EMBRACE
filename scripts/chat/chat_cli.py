"""
Chat with EMBRACE in the terminal, as an Emby user (the same model and tools as the Discord bot).
Needs the LLM server (EMBRACE_LLM_URL); without it, says so and stops.

usage (from project root): PYTHONPATH=src python scripts/chat/chat_cli.py --user NAME [--ask "question" ...] [--tools]
"""
import argparse
import os
import time
from pathlib import Path

from dotenv import load_dotenv

from chat import ChatSession, LibraryTools, LLMClient, LLMUnavailable, TitleEmbeddings, tmdb_lookup
from connectors import TMDBConnector
from ml import PreProcess, Recommender

ROOT_DIR = Path(__file__).resolve().parents[2]
os.chdir(ROOT_DIR)  # SQLiteConnector resolves "sqlite_db/" relative to the working directory
load_dotenv(ROOT_DIR / ".env")


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--user", required=True, help="Emby user name whose history the personal tools use")
    p.add_argument("--ask", action="append", help="ask this (repeatable, as one conversation) and exit")
    p.add_argument("--tools", action="store_true", help="print each tool call")
    args = p.parse_args()

    pp = PreProcess()
    rec = Recommender.load(pp, log=lambda m: print(f"[INFO] {m}"), discovery=True)
    users = rec.profiles.users()
    match = users[users["user_name"].str.casefold() == args.user.casefold()]
    user_id = match["user_id"].iloc[0] if len(match) else None
    if user_id is None:
        print(f"[WARN] no watch history for {args.user}: personal tools will say so")
    llm = LLMClient()
    token = os.getenv("TMDB_READ_ACCESS_TOKEN")
    tools = LibraryTools(rec, runtimes=pp.get_title_runtimes(), lookup=tmdb_lookup(TMDBConnector(token)) if token else None)
    embeddings = TitleEmbeddings(llm.embed_model)
    try:        # search by meaning: embeds what isn't yet (all ~12k titles the first time)
        embeddings.update(TitleEmbeddings.texts(tools.titles), llm.embed, log=lambda m: print(m))
        tools.use_embeddings(embeddings, llm.embed)
    except LLMUnavailable as e:
        print(f"[INFO] search matches words only: {e}")
    session = ChatSession(llm, tools, user_id, args.user, log=print if args.tools else lambda _: None)

    questions = iter(args.ask) if args.ask else None
    try:
        print(f"[INFO] {llm.chat_model()} at {llm.url} - 'reset' starts over, Ctrl-D quits")
    except LLMUnavailable as e:
        print(f"[INFO] {e} - questions will fail until it's back")
    while True:
        try:
            text = next(questions) if questions else input("\nyou> ")
        except (StopIteration, EOFError):
            break
        if questions:
            print(f"\nyou> {text}")
        if text.strip().casefold() == "reset":
            session.reset()
            continue
        started = time.time()
        try:
            answer = session.ask(text)
        except LLMUnavailable as e:
            print(f"[ERROR] {e}")
            continue
        print(f"\nembrace> {answer}\n[{time.time() - started:.1f}s]")


if __name__ == "__main__":
    main()
