# MySQL + IMDB Dataset Guide

The optional machine-learning workflow enriches recommendations with IMDB metadata (genres, ratings, cast). Rather than embedding the TSV dumps directly in the repository, EMBRACE relies on a local MySQL instance that you control. The helper scripts under `scripts/mysql/` orchestrate the full lifecycle: provisioning the container, creating tables, and loading TSV data.

## When you need MySQL

You only need this stack if you plan to run `PreProcess.imdb_get_encoded_genres()` or any downstream tasks that require one-hot encoded IMDB genres. The preprocessing step queries MySQL for `titles` and `genres`, aggregates them, and writes a cached Parquet/PKL file under `data/cache/` for reuse.

## Directory layout

```
scripts/mysql/
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── README.md
├── imdb_create-schema.py
├── imdb_load-from-tsv.py
├── imdb_reset.py
└── init_scripts/
    └── set_usr_perms.sh
```

- `Dockerfile` builds a thin wrapper around the official MySQL image.
- `docker-compose.yml` wires storage, the `.env` credentials, and a bind mount that exposes the raw TSV dumps at `/import` inside the container.
- `imdb_create-schema.py` and `imdb_load-from-tsv.py` are the two primary scripts for first-time setup.
- `imdb_reset.py` truncates all tables so you can perform a clean reload without dropping the database.

## Prerequisites

1. **Download the IMDB TSV dumps** from [https://datasets.imdbws.com/](https://datasets.imdbws.com/). Extract them somewhere on your host machine.
2. **Set up credentials** by copying both `.env` templates and filling in strong passwords:

   ```sh
   cp .env.example .env
   cp scripts/mysql/.env.example scripts/mysql/.env
   ```

3. **Export `IMDB_DATA_DIR`** before starting docker-compose so the TSV directory mounts at `/import` inside the container:

   ```sh
   export IMDB_DATA_DIR=/absolute/path/to/imdb-tsv
   ```

## Bootstrapping the database

1. **Build and start MySQL**

   ```sh
   cd scripts/mysql
   docker compose up -d --build
   ```

   The compose file enables `--secure-file-priv=/import` and `--local-infile=ON`, allowing the loader scripts to stream TSVs directly into staging tables.

2. **Grant file permissions** (first run only)

   ```sh
   ./init_scripts/set_usr_perms.sh
   ```

   This wrapper runs `GRANT FILE` for the application user using the root credentials you supplied.

3. **Create schemas**

   ```sh
   python3 imdb_create-schema.py
   ```

   The script connects via `MySQLConnector`, ensures the database exists, and creates the target tables (`titles`, `genres`, `directors`, `writers`, `episodes`, `roles`, `ratings`, `persons`) along with staging tables used during data loads.

4. **Load TSV data**

   ```sh
   python3 imdb_load-from-tsv.py
   ```

   Each block wraps a table load in a timing context, bulk imports the TSV into a staging table (where required), and then inserts the normalized rows into the destination table. Expect this step to take several minutes depending on disk speed.

5. **Verify row counts**

   After the loader finishes, you can run quick sanity checks:

   ```sh
   docker compose exec db mysql -u${MYSQL_USER} -p${MYSQL_PASSWORD} ${MYSQL_DATABASE} \
     -e "SELECT TABLE_NAME, TABLE_ROWS FROM information_schema.tables WHERE TABLE_SCHEMA='${MYSQL_DATABASE}' ORDER BY TABLE_ROWS DESC;"
   ```

6. **Shut down** (optional)

   ```sh
   docker compose down
   ```

## Maintenance workflow

- **Refresh data**: rerun `imdb_load-from-tsv.py` after downloading new IMDB dumps. Use `imdb_reset.py` beforehand if you want to truncate tables instead of appending.
- **Cache management**: delete or refresh `data/cache/imdb_genres_ohe.parquet` if you need to rebuild the encoded dataset after a reload.
- **Connection reuse**: the Python code instantiates `MySQLConnector` with `scripts/mysql/.env` to discover credentials, so keep that file updated if hosts or ports change.

With these pieces in place, `PreProcess.imdb_get_encoded_genres()` can connect to MySQL, fetch the latest title/genre associations, and generate the sparse one-hot vectors used by the k-NN example in `src/main.py`.
