# MySQL + IMDB Dataset Guide

The optional machine-learning workflow enriches recommendations with IMDB metadata (genres, ratings, cast). Rather than embedding the TSV dumps directly in the repository, EMBRACE relies on a local MySQL instance that you control. The helper scripts under `scripts/mysql/` orchestrate the full lifecycle: provisioning the container, creating tables, and loading TSV data.

## When you need MySQL

You need this stack to build the genre recommender's data (`PreProcess.imdb_get_encoded_genres()`, cached under `data/cache/` afterwards) and for the episode -> series fallback in `PreProcess.map_emby_items_to_imdb()`. The Emby/SQLite side of EMBRACE works without it.

## Directory layout

```
scripts/mysql/
├── docker-compose.yml
├── .env.example
├── README.md
├── download_imdb.sh
├── imdb_create-schema.py
├── imdb_load-from-tsv.py
├── imdb_reset.py
└── init_scripts/
    └── 01_grant_file_priv.sh
```

- `docker-compose.yml` runs the official `mysql:9.4.0` image, with the database in a named volume, credentials from `.env`, and the raw TSV directory mounted read-only at `/import` inside the container. The port is published on `127.0.0.1` only unless `MYSQL_BIND_ADDRESS` is set.
- `init_scripts/` runs on first boot (empty data volume) and grants the `FILE` privilege the loader needs.
- `download_imdb.sh` fetches and decompresses the TSVs and records a `MANIFEST.tsv` (download time, size, sha256).
- `imdb_create-schema.py` and `imdb_load-from-tsv.py` are the two primary scripts for first-time setup.
- `imdb_reset.py` truncates all tables so you can perform a clean reload without dropping the database. It is destructive: read it first.

## Where it runs

The database runs on **optiplex01** (`/opt/stacks/embrace-mysql`, visible in Dockge), with the TSVs in `~/data/imdb`. The port is only published on optiplex01's localhost, so from another machine use an SSH tunnel and point `scripts/mysql/.env` at it:

```sh
ssh -fN -L 3306:127.0.0.1:3306 optiplex01      # MYSQL_HOST=127.0.0.1, MYSQL_PORT=3306 in scripts/mysql/.env
```

## Prerequisites

1. **Set up credentials** by copying both `.env` templates and filling in strong passwords and `IMDB_DATA_DIR` (absolute path to the TSV directory):

   ```sh
   cp .env.example .env
   cp scripts/mysql/.env.example scripts/mysql/.env
   ```

2. **Download the IMDB TSV dumps** (about 7 GB uncompressed):

   ```sh
   scripts/mysql/download_imdb.sh
   ```

## Bootstrapping the database

1. **Start MySQL**

   ```sh
   cd scripts/mysql
   docker compose up -d
   ```

   The compose file enables `--secure-file-priv=/import` and `--local-infile=ON`, allowing the loader scripts to stream TSVs directly into staging tables. On first boot the init script grants `FILE` to `MYSQL_USER`.

2. **Create schemas** (from the project root, using the project venv)

   ```sh
   PYTHONPATH=src python scripts/mysql/imdb_create-schema.py
   ```

   The script connects via `MySQLConnector`, ensures the database exists, and creates the target tables (`titles`, `genres`, `directors`, `writers`, `episodes`, `roles`, `ratings`, `persons`) along with staging tables used during data loads.

3. **Load TSV data**

   ```sh
   PYTHONPATH=src python scripts/mysql/imdb_load-from-tsv.py
   ```

   Each block wraps a table load in a timing context, bulk imports the TSV into a staging table (where required), and then inserts the normalized rows into the destination table. The `roles` table (~95M rows) dominates; expect tens of minutes. Run it detached (`tmux`, or `systemd-run --user`) so a dropped SSH session doesn't kill it.

4. **Verify row counts** with exact counts - `information_schema.TABLES.TABLE_ROWS` is only an InnoDB estimate:

   ```sh
   docker compose exec db sh -c 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "SELECT COUNT(*) FROM titles; SELECT COUNT(*) FROM roles;"'
   ```

5. **Rebuild the genre cache** used by the recommender: `PreProcess().imdb_get_encoded_genres(refresh=True)`.

6. **Shut down** (optional)

   ```sh
   docker compose down
   ```

## Maintenance workflow

- **Refresh data**: rerun `imdb_load-from-tsv.py` after downloading new IMDB dumps. Use `imdb_reset.py` beforehand if you want to truncate tables instead of appending.
- **Cache management**: call `PreProcess().imdb_get_encoded_genres(refresh=True)` after a reload to rebuild `data/cache/imdb_genres_<types>.parquet`.
- **Connection reuse**: the Python code instantiates `MySQLConnector` with `scripts/mysql/.env` to discover credentials, so keep that file updated if hosts or ports change.

With these pieces in place, `PreProcess.imdb_get_encoded_genres()` can connect to MySQL, fetch the latest title/genre associations, and generate the multi-hot genre vectors used by `GenreKNN` (demo: `src/ml/main.py`).
