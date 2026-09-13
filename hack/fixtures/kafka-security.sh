#!/bin/sh
set -eu
# Fixed in-container endpoint and disposable identities only.
for mechanism in SCRAM-SHA-256 SCRAM-SHA-512; do
    /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \
        --alter --entity-type users --entity-name fixture-reader \
        --add-config "$mechanism=[password=fixture-reader-only]"
done
/opt/kafka/bin/kafka-acls.sh --bootstrap-server localhost:9092 \
    --add --allow-principal User:fixture-reader --allow-principal User:CN=fixture-reader --operation Read --operation Describe \
    --topic demo_ --resource-pattern-type prefixed
/opt/kafka/bin/kafka-acls.sh --bootstrap-server localhost:9092 \
    --add --allow-principal User:fixture-reader --allow-principal User:CN=fixture-reader --operation Describe \
    --group onetui- --resource-pattern-type prefixed
