"""
Chat with EMBRACE in the terminal, as an Emby user (the same model and tools as the Discord bot).
The LLM must be running: scripts/llm/llm.sh start.

usage (from project root): PYTHONPATH=src python scripts/chat/chat_cli.py --user NAME [--ask "question" ...] [--tools]
"""
import argparse
import os
import time
from pathlib import Path

from dotenv import load_dotenv

from chat import ChatSession, LibraryTools, LLMUnavailable, OllamaClient
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

    rec = Recommender.load(PreProcess(), log=lambda m: print(f"[INFO] {m}"))
    users = rec.profiles.users()
    match = users[users["user_name"].str.casefold() == args.user.casefold()]
    user_id = match["user_id"].iloc[0] if len(match) else None
    if user_id is None:
        print(f"[WARN] no watch history for {args.user}: personal tools will say so")
    llm = OllamaClient()
    session = ChatSession(llm, LibraryTools(rec), user_id, args.user, log=print if args.tools else lambda _: None)

    questions = iter(args.ask) if args.ask else None
    print(f"[INFO] {llm.model} via {llm.find_host() or 'no host yet'} - 'reset' starts over, Ctrl-D quits")
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
