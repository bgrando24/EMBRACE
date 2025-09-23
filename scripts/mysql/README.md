# Build and run the mySQL docker container

Don't forget to create the `.env` file first
```sh
cd scripts/mysql
cp .env.example .env
nano .env
# change the environment variables as necessary
docker compose up -d --build
docker compose logs -f db   # optional
docker compose down 
```

## Using `tmux` for data load over SSH

* Running TSV data loads over SSH can sometimes run into issues with SSH session breaking while waiting for script to finish
* recommend using `tmux` on the host machine to help prevent broken SSH killing the script process too

```sh
tmux new -s imdb            # open new tmux session called 'imdb'
python3 script_name.py      # run the python script INSIDE tmux
Ctrl + b   then   d         # this detaches from the tmux session WITHOUT killing it
tmux ls                     # if you forget the tmux session name
tmux attach -t imdb_load    # reconnect to tmux session
```



## Handy SQL snippets

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