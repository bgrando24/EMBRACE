# syntax=docker/dockerfile:1

FROM mysql:9.4.0

EXPOSE 3306

# healthcheck reads env at runtime (from compose env_file)
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=10 \
  CMD sh -c 'mysqladmin ping -h 127.0.0.1 -uroot -p"$MYSQL_ROOT_PASSWORD" || exit 1'