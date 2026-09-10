#!/bin/sh
set -eu
umask 077
# Fixture keys live only in tmpfs and never enter the host trust store.
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -subj /CN=onetui-nats-fixture-ca -addext basicConstraints=critical,CA:TRUE \
    -keyout /tls/ca.key -out /tls/ca.crt 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj /CN=localhost \
    -keyout /tls/server.key -out /tls/server.csr 2>/dev/null
openssl x509 -req -days 2 -in /tls/server.csr \
    -CA /tls/ca.crt -CAkey /tls/ca.key -CAcreateserial \
    -extfile /server.ext -out /tls/server.crt 2>/dev/null
if [ "${ONETUI_NATS_TLS:-false}" = true ]; then
    exec nats-server -c /nats.conf --tls --tlscert /tls/server.crt --tlskey /tls/server.key
fi
exec nats-server -c /nats.conf
