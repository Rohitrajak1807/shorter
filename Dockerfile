FROM postgres:18.1-trixie
COPY init.sql /docker-entrypoint-initdb.d
