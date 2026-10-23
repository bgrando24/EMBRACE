# IMDB MySQL database

Full guide: `docs/databases/mysql.md`. Quick version, from the project root:

```sh
cp scripts/mysql/.env.example scripts/mysql/.env && nano scripts/mysql/.env   # passwords, MYSQL_HOST, IMDB_DATA_DIR
scripts/mysql/download_imdb.sh                                                # TSVs + MANIFEST.tsv into IMDB_DATA_DIR
(cd scripts/mysql && docker compose up -d)
PYTHONPATH=src venv/bin/python scripts/mysql/imdb_create-schema.py
PYTHONPATH=src venv/bin/python scripts/mysql/imdb_load-from-tsv.py           # long: run detached, see below
```

The port is published on 127.0.0.1 only. From another machine: `ssh -fN -L 3306:127.0.0.1:3306 <db host>`.

## Running long loads over SSH

A dropped SSH session kills a foreground load. Either use `tmux`:

```sh
tmux new -s imdb            # open new tmux session called 'imdb'
python3 script_name.py      # run the python script INSIDE tmux
Ctrl + b   then   d         # this detaches from the tmux session WITHOUT killing it
tmux attach -t imdb         # reconnect to tmux session
```

or a transient systemd unit, which also keeps the log:

```sh
systemd-run --user --unit embrace-imdb-load --working-directory="$PWD" -E PYTHONPATH="$PWD/src" \
  "$PWD/venv/bin/python" scripts/mysql/imdb_load-from-tsv.py
journalctl --user -u embrace-imdb-load -f
```

## Handy SQL snippets

Estimated row counts (InnoDB estimates - use `COUNT(*)` for exact numbers):

```SQL
SELECT
    TABLE_NAME AS `Table`,
    TABLE_ROWS AS `Rows`
FROM
    information_schema.tables
WHERE
    TABLE_SCHEMA = 'imdb'
ORDER BY
    TABLE_ROWS DESC;
```
