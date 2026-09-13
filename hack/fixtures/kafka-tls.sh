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
if [ -f /tmp/onetui-kafka-tls/trust.p12 ]; then
    keytool -delete -alias fixture-ca -keystore /tmp/onetui-kafka-tls/trust.p12 \
        -storepass fixture-keystore-only
fi
keytool -importcert -noprompt -alias fixture-ca -storetype PKCS12 \
    -file /tmp/onetui-kafka-tls/ca.crt -keystore /tmp/onetui-kafka-tls/trust.p12 \
    -storepass fixture-keystore-only
for identity in reader denied; do
    openssl req -new -newkey rsa:2048 -nodes -subj "/CN=fixture-$identity" \
        -keyout "/tmp/onetui-kafka-tls/$identity.key" \
        -out "/tmp/onetui-kafka-tls/$identity.csr" 2>/dev/null
    openssl x509 -req -days 2 -in "/tmp/onetui-kafka-tls/$identity.csr" \
        -CA /tmp/onetui-kafka-tls/ca.crt -CAkey /tmp/onetui-kafka-tls/server.key \
        -CAcreateserial -out "/tmp/onetui-kafka-tls/$identity.crt" 2>/dev/null
done
openssl pkey -in /tmp/onetui-kafka-tls/reader.key -aes-256-cbc \
    -passout pass:fixture-client-key-only -out /tmp/onetui-kafka-tls/reader-encrypted.key
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=untrusted \
    -keyout /tmp/onetui-kafka-tls/untrusted.key \
    -out /tmp/onetui-kafka-tls/untrusted.crt 2>/dev/null
# OpenSSL's generated RSA key uses exponent 65537 (base64url AQAB).
modulus=$(openssl rsa -in /tmp/onetui-kafka-tls/server.key -noout -modulus \
    | cut -d= -f2 | xxd -r -p | openssl base64 -A | tr '+/' '-_' | tr -d '=')
printf '{"keys":[{"kty":"RSA","kid":"onetui-fixture","use":"sig","alg":"RS256","n":"%s","e":"AQAB"}]}\n' "$modulus" \
    > /tmp/onetui-kafka-tls/jwks.json
exec /etc/kafka/docker/run
