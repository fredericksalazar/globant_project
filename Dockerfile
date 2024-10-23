FROM postgres:14

COPY scripts/globant_database.sql /docker-entrypoint-initdb.d/
