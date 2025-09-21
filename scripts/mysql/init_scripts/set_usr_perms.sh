# load the .env values into this shell so $MYSQL_ROOT_PASSWORD expands
set -a
source .env
set +a

docker compose exec db mysql -uroot -p"$MYSQL_ROOT_PASSWORD" \
  -e "GRANT FILE ON *.* TO 'embrace'@'%'; FLUSH PRIVILEGES;"
