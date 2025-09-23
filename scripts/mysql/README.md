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
tmux attach -t imdb         # reconnect to tmux session
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


While that's running, let me give you some context about this database. 

So, the whole reason for me loading in the imdb data into a dedicated database is I am working on building a movie/tv show recommendation service, as a personal hobby project. The idea for an initial MVP is to build a k-NN model using the genres of a given media item, then provide recommendations based on an input media item (movie or tv show) by finding its vector, and then finding the nearest neighbours.

I wanted a large set of movies and tv shows, as well as a rich collection of genre tags for each media item, which thankfully the IMDB dataset has up to 3 genre tags for a given title. The idea was I was going to 