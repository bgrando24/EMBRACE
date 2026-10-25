# Chat-bot (`src/chat`)

EMBRACE's conversational interface: ask for something to watch in plain language ("a funny show like Letterkenny",
"a tense thriller under 2 hours", "what should I watch tonight?") and get a few picks from **your** server, chosen
with **your** watch history in mind. It runs as a Discord bot, with a local LLM (via [Ollama](https://ollama.com))
that is only running when you start it.

```
Discord ──► discord_bot.py (EMBRACE host, always on) ──► OllamaClient ──► Ollama (a GPU machine / Mac, on demand)
                     │                                        ▲
                     └──► ChatSession ──► LibraryTools ───────┘ tool calls: search, similar titles, details,
                                              │                            your picks, your history
                                              └──► Recommender (ml) + SQLite library, TMDB details, watch history
```

## How it answers

The LLM never answers from its own memory of films: it is given **tools** over the library and calls them before
answering (the system prompt in `chat/session.py` insists on it). That keeps answers to titles that are actually on
the server, and lets a small local model give grounded answers.

| Tool | What it does |
| --- | --- |
| `search_library` | Text search (TF-IDF, words and two-word phrases) over each title's name, genres, TMDB keywords, cast/crew and plot. For moods and descriptions. With `like` (a title the user liked), results are ranked by the average of the text match and the `ContentKNN` similarity to that title, each scaled to its best match: one call does "like X, but ...", which a small model handles much better than combining two tool results itself. |
| `similar_titles` | `ContentKNN` neighbours of a title the server has or had (keywords, people, plot, genre, MovieLens), with what they have in common. |
| `title_info` | One title's plot, genres, keywords, crew and cast, whether it's on the server, and whether you've watched it. |
| `my_recommendations` | Your daily picks (`Recommender.picks()`): `fresh` or `back_catalogue`. |
| `my_watch_history` | What you watched most, or most recently. |

Every result marks titles you've watched, and (when you have enough history) a `taste_fit` from 0 to 100: how close
the title is to your taste profile, as a percentile of the library. Search and similar titles leave out what you've
watched unless asked. Titles are matched the way people type them (any case, with or without "The", small typos,
an optional year in brackets).

A conversation keeps its last 6 exchanges for follow-ups ("something shorter?", "I've seen that one"), and starts
afresh after 2 hours idle or when you send `reset`. A question may take up to 5 rounds of tool calls.

## The LLM: `scripts/llm/llm.sh`

The LLM runs on whichever machine you start it on, so it only uses a GPU (or a laptop's memory) while you want it:

```bash
scripts/llm/llm.sh start              # on this machine
scripts/llm/llm.sh start <ssh host>   # on another machine, over ssh (no repo needed there)
scripts/llm/llm.sh status [HOST]
scripts/llm/llm.sh stop [HOST]
```

`start` runs a separate Ollama server for EMBRACE on port **11435**, listening on the local network, pulls the
model on first use, and loads it. Being separate, it leaves alone any Ollama you already run (the Mac app, or a
system service on 11434), and it runs as you, so no sudo is needed (with a snap install, it runs the snap's binary
directly, since the snap's launcher forces its own address). Models are stored in `~/.ollama/models`, shared with
your own Ollama on the same machine. While running, the model leaves memory after 15 minutes without a message, and
loads again (a few seconds) on the next one.

The bot tries each URL in `EMBRACE_LLM_HOSTS` in order and uses the first that answers, so list every machine you
might start it on. When none is running, it replies with how to start one.

> **Network access.** Ollama's API has no authentication: anyone on your network can use a running EMBRACE LLM. If
> the machine has a firewall, allow only the EMBRACE host, e.g. `sudo ufw allow from <EMBRACE host IP> to any port
> 11435 proto tcp`. After `stop`, nothing listens on the port.

### Model

Default `qwen3:8b`: it calls tools reliably and fits an 8 GB GPU with an 8k-token context (about 5.2 GB of weights).
Set `EMBRACE_LLM_MODEL` to try others: the model must support tools in Ollama (see the "tools" tag on
ollama.com/library), and the same name must be set for `llm.sh` (which pulls it) and for the bot. Thinking mode is
off: on test questions it made answers about 4x slower (about 37 s vs 8 s on an M4 Pro) without choosing better
titles or tools.

## The Discord bot: `scripts/chat/discord_bot.py`

Talk to it in a DM, or @mention it in a channel it can read. Besides questions, it understands `help`, `reset`
and `status` (whether the LLM is running, and how old its library data is). It reloads the library data on the
first message after the nightly refresh. Questions are answered one at a time (one GPU).

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
4. **Point it at the LLM**: `EMBRACE_LLM_HOSTS=http://<gpu machine>:11435,http://<mac>:11435` (machine addresses
   on your network).
5. **Run it as a service** on the EMBRACE host:

    ```bash
    cp scripts/systemd/embrace-chat.service ~/.config/systemd/user/
    systemctl --user daemon-reload
    systemctl --user enable --now embrace-chat.service
    journalctl --user -u embrace-chat -f     # tool calls are logged here
    ```

    The bot itself is light (the recommender data, about 0.5-1 GB of memory); the LLM runs elsewhere.

## In the terminal: `scripts/chat/chat_cli.py`

The same chat without Discord, for trying prompts and models:

```bash
PYTHONPATH=src python scripts/chat/chat_cli.py --user <emby name> --tools
PYTHONPATH=src python scripts/chat/chat_cli.py --user <emby name> --ask "a heist movie" --ask "something lighter"
```

`--tools` prints each tool call. Set `EMBRACE_LLM_HOSTS` to the machine running the LLM (default
`http://localhost:11435`).

## Limits and next steps

- **Search is word-based.** "Canadian" doesn't match the keyword "canada"; the model is told to search with several
  descriptive keywords and retry, which mostly covers it. Embedding search (semantic matching, computed by the same
  Ollama) is the planned improvement, together with richer per-title tags (README goal 2).
- **Only titles on the server are recommended.** A Jellyseerr "request this" button for titles that aren't is on
  the roadmap.
- The `taste_fit` and tool results are only as good as the watch history: with little history, answers are driven
  by the request alone.
