"""
Style-tag titles with the chat LLM (see src/ml/style_tags.py): a few seconds per title. Resumable: what's tagged is
cached in data/cache/title_style.parquet, and only new or changed titles are asked.
Needs the LLM server (EMBRACE_LLM_URL); stops, keeping its progress, if it goes away.

usage (from project root): PYTHONPATH=src python scripts/llm/tag_titles.py [--all] [--url URL] [--think] [--limit N]
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from chat import LibraryTools, LLMClient, LLMUnavailable, TitleEmbeddings
from ml import PreProcess, Recommender
from ml.style_tags import SCHEMA, StyleTags, messages

ROOT_DIR = Path(__file__).resolve().parents[2]
os.chdir(ROOT_DIR)  # SQLiteConnector resolves "sqlite_db/" relative to the working directory
load_dotenv(ROOT_DIR / ".env")


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--all", action="store_true", help="also the titles beyond the library (~11k: hours)")
    p.add_argument("--url", help="the LLM server (DEFAULT: EMBRACE_LLM_URL)")
    p.add_argument("--think", action="store_true", help="let the model think first: better tags, ~8x slower")
    p.add_argument("--limit", type=int, help="tag at most this many titles")
    args = p.parse_args()

    pp = PreProcess()
    rec = Recommender.load(pp, log=lambda m: print(f"[INFO] {m}"), discovery=args.all)
    texts = TitleEmbeddings.texts(LibraryTools(rec).titles)     # the same description search by meaning embeds
    if not args.all:
        texts = texts[texts.index.isin(rec.content["t_const"])]
    store = StyleTags()
    todo = store.missing(texts)
    if args.limit:
        todo = todo.head(args.limit)
    llm = LLMClient(url=args.url, think=args.think)
    try:
        print(f"[INFO] {len(todo)} of {len(texts)} titles to tag with {llm.chat_model()} at {llm.url}", flush=True)
        started = time.time()
        n = store.update(todo, lambda text: json.loads(llm.chat(messages(text), format=SCHEMA)["content"]))
    except LLMUnavailable as e:
        print(f"[WARN] {e} - stopped; what's tagged is kept, run again to carry on ({len(store)} cached)")
        sys.exit(1)
    if n:
        print(f"[INFO] tagged {n} titles in {(time.time() - started) / 60:.0f} min ({len(store)} cached)")


if __name__ == "__main__":
    main()
