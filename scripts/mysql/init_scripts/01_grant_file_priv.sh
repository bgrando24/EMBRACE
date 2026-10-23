# Runs inside the MySQL container on first boot (empty data volume), after MYSQL_USER has been created.
# The official entrypoint sources non-executable *.sh files here, which makes docker_process_sql available.
# FILE is needed for the loader's server-side LOAD DATA INFILE from /import.
docker_process_sql --database=mysql <<<"GRANT FILE ON *.* TO '${MYSQL_USER}'@'%'; FLUSH PRIVILEGES;"
