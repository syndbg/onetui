#!/bin/sh
set -eu
umask 077
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -subj /CN=onetui-rabbitmq-fixture-ca -addext basicConstraints=critical,CA:TRUE \
    -keyout /tls/ca.key -out /tls/ca.crt 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj /CN=localhost \
    -keyout /tls/server.key -out /tls/server.csr 2>/dev/null
openssl x509 -req -days 2 -in /tls/server.csr \
    -CA /tls/ca.crt -CAkey /tls/ca.key -CAcreateserial \
    -extfile /server.ext -out /tls/server.crt 2>/dev/null
chown -R rabbitmq:rabbitmq /tls
exec docker-entrypoint.sh rabbitmq-server
