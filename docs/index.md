# EMBRACE Documentation

EMBRACE is a local-first recommendation and analytics engine built around your Emby library. The project ingests playback history and rich metadata so downstream models can reason about what each household actually watches.

## Supported runtimes and tooling

-   **Python**: The project requires Python **3.11 or newer** (see `pyproject.toml`). Development has primarily targeted CPython 3.11/3.12, so you do not need to upgrade to 3.13 unless you already run it.
-   **Optional services**:
    -   **SQLite** is bundled and created on demand under `sqlite_db/`.
    -   **MySQL** (via Docker) backs the optional IMDB ingest pipeline that powers the ML preprocessing step.
    -   **MkDocs** generates the static documentation in this directory (`mkdocs serve` for live preview).

## Quickstart (local Python environment)

1. **Clone the repository**

    ```
    git clone https://github.com/bgrando24/EMBRACE.git
    cd EMBRACE
    ```

2. **Create a virtual environment and install dependencies**

    ```
    python3 -m venv .venv
    source .venv/bin/activate        # Windows: .venv\Scripts\activate.bat
    pip install -r requirements.txt
    pip install -e .
    ```

3. **Configure environment variables**

    Populate both `.env` files so the connectors can authenticate:

    ```
    cp .env.example .env
    cp scripts/mysql/.env.example scripts/mysql/.env   # optional MySQL pipeline
    ```

    The [environment variables guide](env_vars.md) documents every setting and when it is required.

4. **Run the application entry point**

    ```
    python3 src/main.py
    ```

    The default script demonstrates how to build genre embeddings from the IMDB dataset once the supporting databases are populated.

## Docker usage

A simple Dockerfile is provided for packaging the core application. Build and run it with:

```
docker build -t embrace .
docker run --env-file .env embrace
```

For the optional IMDB + MySQL toolchain, see the dedicated [MySQL database guide](databases/mysql.md) for docker-compose instructions.

## Where to go next

-   [Architecture overview](architecture/overview.md) explains the major components and how the connectors interact.
-   [Environment variables](env_vars.md) clarifies which credentials are required for Emby, TMDB, and MySQL.
-   [Data ingestion workflows](workflows/data_ingestion.md) walks through library metadata ingestion, watch-history processing, and TMDB synchronisation.
-   [SQLite schema reference](databases/sqlite.md) documents the operational data store created by `SQLiteConnector`.
-   [MySQL + IMDB reference](databases/mysql.md) covers the optional dataset that powers the ML preprocessing step.
