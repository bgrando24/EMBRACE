# Chat-bot (`src/chat`)

EMBRACE's conversational interface: ask for something to watch in plain language ("a funny show like Letterkenny",
"a tense thriller under 2 hours", "what should I watch tonight?") and get a few picks chosen with **your** watch
history in mind: from the server, and beyond it (about 12,000 well-known titles it doesn't have, to request in
Jellyseerr). It runs as a Discord bot, with a local LLM: a [llama.cpp](https://github.com/ggml-org/llama.cpp) server
on another machine, which is also used for other things, so it may be off or busy at times.

```
Discord ──► discord_bot.py (EMBRACE host, always on) ──► LLMClient ──► llama-server (a GPU machine, when it's on)
                     │                                        ▲
                     └──► ChatSession ──► LibraryTools ───────┘ tool calls: search, similar titles, details,
                                              │                            your picks, your history
                                              └──► Recommender (ml) + SQLite library, TMDB details, watch history
```

## How it answers

The LLM never answers from its own memory of films: it is given **tools** over the titles EMBRACE has details of,
and calls them before answering (the system prompt in `chat/session.py` insists on it). That keeps answers to real
titles described from real data, and lets a small local model give grounded answers.

Those titles are the library's (on the server, or once were) and the titles beyond it that the daily refresh fetches
TMDB details for: everything TMDB recommends for a library title, and IMDB titles with 25,000+ votes (see
[SQLite: titles beyond the library](../databases/sqlite.md#titles-beyond-the-library)). Search and similar titles
cover both by default; every result says whether it's `on_server`, and the answer marks picks that aren't, so you can
request them. Asking for "something on Emby" or "to watch tonight" limits it to the server (`availability`). The
tools use a `ContentKNN` and taste profiles over all of these titles (`Recommender.wide`). The daily picks' two
playlists keep the library-only model, since playlists can only hold what the server has; their titles to request
use the wide one.

| Tool | What it does |
| --- | --- |
| `search_library` | Search by meaning and by words (see [Search by meaning](#search-by-meaning)), over each title's name, genres, TMDB keywords, cast/crew and plot, for moods and descriptions. The word part is TF-IDF over words and two-word phrases. Words that ask for a genre ("funny", "comedy", "scary", "thriller", "sci-fi"...: `GENRE_WORDS`) also count: the share of those genres a title has is averaged with the text match, so "funny small town show" gives comedies, not every show that mentions a small town. With `like` (a title the user liked), that's averaged with the similarity to the `like` title, each scaled to its best match: one call does "like X, but ...", which a small model handles much better than combining two tool results itself. Last, the IMDb rating nudges the order (`QUALITY_WEIGHT`, 20%; the rating weighted by its vote count), so that of thousands of equally good matches the well-rated go first, not obscure ones. Words like "movie" or "show" are left out of the text match: `kind` says that. |
| `similar_titles` | `ContentKNN` neighbours of a title (keywords, people, plot, genre, MovieLens), with what they have in common. |
| `title_info` | One title's plot, genres, keywords, crew and cast, whether it's on the server, and whether you've watched it. |
| `my_recommendations` | Your daily picks (`Recommender.picks()`): `fresh` or `back_catalogue`, on the server; or `to_request`, titles not on the server worth requesting (`Recommender.to_request()`). |
| `my_watch_history` | What you watched most, or most recently. |

Results include each title's length from Emby (`PreProcess.get_title_runtimes()`: a movie's runtime, or a show's
median episode), and `search_library` takes `max_minutes` ("under 2 hours"), which also leaves out titles whose
length isn't known, so the limit is a promise.

Every result marks titles you've watched, and (when you have enough history) a `taste_fit` from 0 to 100: how close
the title is to your taste profile, as a percentile of all the titles. Watch history only covers the server, so a
title beyond it may be one you've seen elsewhere: say so, and the bot takes it on board. Search and similar titles leave out what you've
watched unless asked. Titles are matched the way people type them (any case, with or without "The", small typos,
an optional year in brackets).

**Any other title.** "Something like X" shouldn't depend on EMBRACE having X's details. When it doesn't, X is looked
up on TMDB (`TMDBConnector.search_title`, `chat.tmdb_lookup`; needs
`TMDB_READ_ACCESS_TOKEN`, as the nightly refresh does): `like` and `similar_titles` then compare it by the text match
to its genres, keywords, people and plot (not its name, so "The Boys" isn't like "Trailer Park Boys"), and
`title_info` describes it and suggests requesting it. A TMDB match whose name isn't close to the one asked for is
ignored, so a description passed as a title doesn't turn into some other film.

A conversation keeps its last 6 exchanges for follow-ups ("something shorter?", "I've seen that one"), and starts
afresh after 2 hours idle or when you send `reset`. It also remembers the titles it has suggested (those in bold in
its answers), and search, similar titles and recommendations leave them out: told "those aren't funny enough", a
small model otherwise searches with nearly the same words and offers the same titles, whatever its prompt says. A question may take up to 5 rounds of tool calls.

## The LLM: a shared llama.cpp server

EMBRACE doesn't run an LLM of its own: it uses a [llama.cpp](https://github.com/ggml-org/llama.cpp) server
(`llama-server`) that's already running on a GPU machine for other things, through its OpenAI-compatible API
(`chat.LLMClient`, at `EMBRACE_LLM_URL`). One server means one model in GPU memory, instead of two competing for it.

The server runs in router mode: it lists several models, loads one on the first request (and unloads it after a
while idle), and holds at most `--models-max` at once. So EMBRACE:
- **uses whichever chat model is loaded already**, and only asks for its own (`EMBRACE_LLM_MODEL`) when none is, so
  it never swaps out the model someone else is using;
- **needs room for the embedding model beside it**: `--models-max 2`, with the embedding model's preset set to run
  on the CPU (see [Search by meaning](#search-by-meaning));
- **switches thinking off** per request (`chat_template_kwargs`), as answers are much slower with it.

The server must listen on the network (`--host 0.0.0.0`). Its API has no authentication: if the machine has a
firewall, allow only the EMBRACE host to reach the port.

### When it's off or busy

The machine and its server are used for other things, so EMBRACE treats the LLM as optional:
- **Everything that isn't chat carries on**: the nightly refresh, daily picks and playlists never use it.
- **Chat says so and waits.** Any failure to get an answer (the machine off, the server stopped, no reply within
  3 minutes because it's busy with something else, a model failing to load) is one error, `LLMUnavailable`. The bot
  then replies that its AI side isn't available right now, keeps the conversation, and tries again on the next
  message. `status` says whether it's available.
- **Search by meaning catches up by itself**: titles not embedded yet wait for the server, retried every 15 minutes,
  and search matches words only until then.
- **Batch jobs stop and resume**: `scripts/llm/tag_titles.py` keeps what it has tagged, and carries on from there
  the next time it's run.

The server answers one request at a time (`--parallel 1`), so a question can wait behind someone else's request.

### Search by meaning

Word matching can't tell that "mind-bending sci-fi" means Interstellar, or that "cozy feel-good show" means Schitt's
Creek and not a horror film called *Us*. So search also compares meanings, with text embeddings
(`EMBRACE_EMBED_MODEL`, default `Qwen3-Embedding-0.6B-Q8_0`) from the same server as the chat model:
- **What's embedded:** each title's text (`chat.embeddings.title_text()`): name, year, movie or series, genres,
  keywords, director or creator, the top 4 cast, and the plot.
- **Where it's kept:** `data/cache/title_embeddings_<model>.parquet` (`TitleEmbeddings`), stored with a hash of
  that text, so a title is only embedded again when its details change.
- **Queries:** each one is embedded at question time, with the instruction Qwen3-Embedding expects in front.
- **Ranking:** a query's match is 75% meaning and 25% words (`SEMANTIC_WEIGHT`). Words still catch names and exact
  phrases. Genre words and `like` then count as before.

Made by the LLM server, not the nightly refresh, so the refresh never depends on it being on.
- **First run:** after loading its data, the bot embeds whatever isn't yet, in the background, and again after each
  answer and every 15 minutes until it's all done. All ~12k titles take a while on the CPU; after that, only new or
  changed titles (seconds).
- **Until then,** or if the server stops mid-conversation, search matches words only. `status` says which.

Why this model, on the CPU:
- **The GPU stays the chat model's.** On an 8 GB GPU shared with a desktop, the embedding model and a chat model
  didn't fit together, and part of the chat model went to the CPU, making answers several times slower. The
  embedding model's preset in the server's `--models-preset` file keeps it off the GPU:

    ```ini
    [Qwen3-Embedding-0.6B-Q8_0]
    embeddings = true
    pooling = last
    n-gpu-layers = 0
    c = 8192
    batch-size = 2048
    ubatch-size = 2048
    parallel = 4
    threads = 4
    ```

- **Size:** the 0.6B version embeds a query in well under a second on the CPU. The 4B and 8B versions gave no better
  picks in testing and would be much slower there.
- **Quality:** on ten typical queries, it already beat word search on every vague one, e.g. "epic fantasy with
  dragons and politics" went from Lord of the Rings films to House of the Dragon and Game of Thrones.

Not for the daily picks: blending embeddings into the picks' model was tested at every size and weight, and never
beat it ([ML: tried and not adopted](ml.md#evaluation-mlevaluate)).

## The Discord bot: `scripts/chat/discord_bot.py`

Talk to it in a DM, or @mention it in a channel it can read. Besides questions, it understands `help`, `reset`
and `status` (whether its AI side is available, and how old its library data is). It reloads the library data on
the first message after the nightly refresh. Questions are answered one at a time, as the LLM server answers one
request at a time.

Each Discord user must be linked to their Emby user name in `EMBRACE_DISCORD_USERS`; anyone else is told their
Discord ID and asked to get linked, and nothing about the library or anyone's history is shown to them.

### Setup

1. **Create the bot**: [Discord developer portal](https://discord.com/developers/applications) -> New Application
   -> Bot -> Reset Token, and copy the token into `.env` as `DISCORD_BOT_TOKEN`. No privileged intents are needed
   (DMs and mentions carry their text without "Message Content").
2. **Invite it** to your server: OAuth2 -> URL Generator, scope `bot`, permissions *Send Messages*, *Read Message
   History*; open the URL. (To only DM it, it still needs to share a server with you.)
3. **Link users**: DM the bot; it replies with your Discord ID. Add `EMBRACE_DISCORD_USERS=<discord id>:<emby
   name>,...` to `.env`.
4. **Point it at the LLM**: `EMBRACE_LLM_URL=http://<gpu machine>:8080` (see
   [The LLM](#the-llm-a-shared-llamacpp-server)), and `EMBRACE_LLM_MODEL` to the chat model to load when none is.
5. **Run it as a service** on the EMBRACE host:

    ```bash
    cp scripts/systemd/embrace-chat.service ~/.config/systemd/user/
    systemctl --user daemon-reload
    systemctl --user enable --now embrace-chat.service
    journalctl --user -u embrace-chat -f     # tool calls are logged here
    ```

    The bot itself needs no GPU: about 1.2 GB of memory with ~12k titles (Oct 2026), loading in ~15 s; the LLM runs elsewhere.

## In the terminal: `scripts/chat/chat_cli.py`

The same chat without Discord, for trying prompts and models:

```bash
PYTHONPATH=src python scripts/chat/chat_cli.py --user <emby name> --tools
PYTHONPATH=src python scripts/chat/chat_cli.py --user <emby name> --ask "a heist movie" --ask "something lighter"
```

`--tools` prints each tool call. It uses the same `EMBRACE_LLM_URL` as the bot, and embeds any titles that aren't
yet before the first question, so the first run takes longer.

## Limits and next steps

- **Search by meaning is only as good as each title's text** (TMDB's keywords and plot). Richer per-title tags,
  such as tone, pacing and style of humour (README goal 2), are the planned next step.
- **Titles beyond the server are the well-known ones** (TMDB's recommendations for the library, and 25,000+ IMDB
  votes); anything else is only reachable by name (looked up on TMDB). A Jellyseerr "request this" button is on the
  roadmap.
- The `taste_fit` and tool results are only as good as the watch history: with little history, answers are driven
  by the request alone.
