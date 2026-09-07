#!/bin/sh
# Sourced by the official image during initialization; keys stay in the disposable tmpfs.
set -eu
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -subj /CN=bpearl-fixture-ca -addext basicConstraints=critical,CA:TRUE \
    -keyout "$PGDATA/bpearl-ca.key" -out "$PGDATA/bpearl-ca.crt" 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj /CN=localhost \
    -keyout "$PGDATA/bpearl-server.key" -out "$PGDATA/bpearl-server.csr" 2>/dev/null
openssl x509 -req -days 2 -in "$PGDATA/bpearl-server.csr" \
    -CA "$PGDATA/bpearl-ca.crt" -CAkey "$PGDATA/bpearl-ca.key" -CAcreateserial \
    -extfile /bpearl-server.ext -out "$PGDATA/bpearl-server.crt" 2>/dev/null
chmod 600 "$PGDATA/bpearl-server.key" "$PGDATA/bpearl-ca.key"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
ALTER SYSTEM SET ssl = on;
ALTER SYSTEM SET ssl_cert_file = 'bpearl-server.crt';
ALTER SYSTEM SET ssl_key_file = 'bpearl-server.key';
SQL
