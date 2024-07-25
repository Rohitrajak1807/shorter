FROM postgres:16.3-bookworm
COPY init.sql /docker-entrypoint-initdb.d
