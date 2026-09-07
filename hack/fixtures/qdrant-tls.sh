#!/bin/sh
set -eu
umask 077
# Independent fixture CA: never installed in a host trust store; private keys stay in tmpfs.
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -subj /CN=bpearl-qdrant-fixture-ca -addext basicConstraints=critical,CA:TRUE \
    -keyout /qdrant/tls/ca.key -out /qdrant/tls/ca.crt 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj /CN=localhost \
    -keyout /qdrant/tls/server.key -out /qdrant/tls/server.csr 2>/dev/null
openssl x509 -req -days 2 -in /qdrant/tls/server.csr \
    -CA /qdrant/tls/ca.crt -CAkey /qdrant/tls/ca.key -CAcreateserial \
    -extfile /bpearl-server.ext -out /qdrant/tls/server.crt 2>/dev/null
exec /qdrant/entrypoint.sh
