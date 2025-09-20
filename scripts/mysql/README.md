# Build and run the mySQL docker container

Don't forget to create the `.env` file first
```sh
cd scripts/mysql
cp .env.example .env
# change the environment variables as necessary
docker compose up -d --build
docker compose logs -f db   # optional
docker compose down 
```