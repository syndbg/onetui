#!/bin/sh
# Sourced by the official image during initialization; keys stay in the disposable tmpfs.
set -eu
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -subj /CN=onetui-fixture-ca -addext basicConstraints=critical,CA:TRUE \
    -keyout "$PGDATA/onetui-ca.key" -out "$PGDATA/onetui-ca.crt" 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj /CN=localhost \
    -keyout "$PGDATA/onetui-server.key" -out "$PGDATA/onetui-server.csr" 2>/dev/null
openssl x509 -req -days 2 -in "$PGDATA/onetui-server.csr" \
    -CA "$PGDATA/onetui-ca.crt" -CAkey "$PGDATA/onetui-ca.key" -CAcreateserial \
    -extfile /onetui-server.ext -out "$PGDATA/onetui-server.crt" 2>/dev/null
chmod 600 "$PGDATA/onetui-server.key" "$PGDATA/onetui-ca.key"
# Allow only the disposable replica role to stream WAL from the fixture network.
printf '\nhost replication onetui_replication all scram-sha-256\n' >> "$PGDATA/pg_hba.conf"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
ALTER SYSTEM SET ssl = on;
ALTER SYSTEM SET ssl_cert_file = 'onetui-server.crt';
ALTER SYSTEM SET ssl_key_file = 'onetui-server.key';
SQL
