#!/bin/sh
set -eu
umask 077
# Fixture-only trust; no host trust-store changes or persistent private keys.
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -subj /CN=localhost -addext subjectAltName=DNS:localhost \
    -keyout /tmp/onetui-kafka-tls/server.key \
    -out /tmp/onetui-kafka-tls/ca.crt 2>/dev/null
openssl pkcs12 -export -name localhost \
    -inkey /tmp/onetui-kafka-tls/server.key -in /tmp/onetui-kafka-tls/ca.crt \
    -out /tmp/onetui-kafka-tls/server.p12 -passout pass:fixture-keystore-only
exec /etc/kafka/docker/run
