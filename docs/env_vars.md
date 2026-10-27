# Environment Variables

Two `.env` files are required: one at the repository root for the Python services, and another under `scripts/mysql/` that configures the optional IMDB MySQL stack. Use the examples as a baseline and fill in the values that match your deployment.

## Root `.env`

| Name | Required | Description |
| --- | --- | --- |
| `BASE_DOMAIN` | Yes | Base URL for your Emby server, including the `/emby` suffix (for example `https://192.168.1.5:8096/emby`). The Emby connector concatenates this with the REST paths it calls. |
| `EMBY_API_KEY` | Yes | API key created from the Emby dashboard. Required for all playback history and metadata calls. |
| `TMDB_READ_ACCESS_TOKEN` | Yes for TMDB sync | TMDB “read access token” used by the TMDB connector: genre IDs, and the title details (keywords, cast/crew, descriptions) the refresh fetches for `ContentKNN`. |
| `JELLYSEERR_URL` | No | Base URL of Jellyseerr, e.g. `https://requests.example.com`. With `JELLYSEERR_API_KEY`, the refresh stores media requests. |
| `JELLYSEERR_API_KEY` | No | Jellyseerr API key (Jellyseerr: Settings -> General -> API Key). |
| `DISCORD_WEBHOOK_URL` | No | Discord webhook for the refresh status and the nightly picks preview. Without it, notifications are skipped. |
| `EMBRACE_PICKS_EXCLUDE_USERS` | No | Comma-separated Emby user names that get no recommendation playlists, and are left out of the picks preview. |
| `DISCORD_BOT_TOKEN` | For the chat-bot | Token of EMBRACE's Discord bot (see [Chat-bot](modules/chat.md#setup)). |
| `EMBRACE_DISCORD_USERS` | For the chat-bot | Who's who: comma-separated `<discord user id>:<emby user name>` pairs. Only linked people can chat. |
| `EMBRACE_LLM_URL` | For the chat-bot | The llama.cpp server (`llama-server`) EMBRACE's LLM features use, e.g. `http://<gpu machine>:8080`. Default `http://localhost:8080`. |
| `EMBRACE_LLM_MODEL` | No | Chat model to load when the server has none loaded (whichever is loaded is used, so someone else's isn't swapped out). Must support tool calling. Default: the first model the server lists. |
| `EMBRACE_EMBED_MODEL` | No | The server's embedding model, for search by meaning. Default `Qwen3-Embedding-0.6B-Q8_0`. |
| `ENVIRONMENT` | Optional | Controls connector debug logging. Use `dev`, `staging`, or `prod`; defaults to `dev` when unset. |
| `SQLITE_DB_NAME` | Optional | Overrides the default SQLite file name (`EMBRACE_SQLITE_DB.db`). The database is stored under `sqlite_db/` and is created automatically if missing. |
| `SQLITE_KEEP_BACKUPS` | Optional | Number of dated database backups the refresh keeps in `sqlite_db/backups/` (one is made per run). Default `30`. `sqlite_db/archive/` is never pruned. |
| `IMDB_DB_PATH` | Optional | Path to a custom IMDB dataset build. Future preprocessing steps can use this to point at an alternative dataset without reconfiguring the MySQL connector. Keep this unset unless you have a non-default ingest pipeline. |

## `scripts/mysql/.env`

Populate this file when you plan to run the dockerised MySQL stack that powers the IMDB ingest scripts.

| Name | Required | Description |
| --- | --- | --- |
| `MYSQL_DATABASE` | Yes | Logical database name; all helper scripts assume `imdb` unless you change it. |
| `MYSQL_ROOT_PASSWORD` | Yes | Root password passed to the container and used by helper scripts when granting permissions. |
| `MYSQL_USER` | Yes | Application user created for the dataset loading scripts (defaults to `embrace`). |
| `MYSQL_PASSWORD` | Yes | Password for `MYSQL_USER`. |
| `MYSQL_HOST` | Yes | Hostname or IP address where the MySQL container is reachable (often `127.0.0.1` when using docker-compose). |
| `MYSQL_PORT` | Yes | Port exposed by the container, typically `3306`. |

### Additional docker-compose variable

`docker-compose.yml` expects an `IMDB_DATA_DIR` environment variable when the stack is started. Point it to a directory that contains the raw IMDB TSV dumps (e.g. `export IMDB_DATA_DIR=/path/to/imdb-tsv`). The path is mounted read-only inside the container at `/import` so the loader scripts can stream TSVs into staging tables.

## Tips

- Keep `.env` files out of source control—`git status` should remain clean after you populate them.
- Restart any running services after updating credentials so connectors pick up the latest values.
