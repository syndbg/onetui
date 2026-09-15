#!/bin/sh
set -eu
# Fixed loopback target and disposable administrator credentials only.
put() {
    curl --fail-with-body --silent --show-error --max-time 10 \
        -u fixture-admin:fixture-admin-only -H 'Content-Type: application/json' \
        -X PUT --data "$2" "http://127.0.0.1:15672/api/$1"
}
put users/fixture-reader '{"password":"fixture-reader-only","tags":"monitoring"}'
put users/fixture-denied '{"password":"fixture-denied-only","tags":""}'
put 'vhosts/demo%20%2F%20%D0%A1%D0%BE%D1%84%D0%B8%D1%8F' '{"description":"Unicode and slash fixture"}'
put permissions/%2F/fixture-reader '{"configure":"^$","write":"^$","read":"^$"}'
put permissions/demo%20%2F%20%D0%A1%D0%BE%D1%84%D0%B8%D1%8F/fixture-reader '{"configure":"^$","write":"^$","read":"^$"}'
put exchanges/%2F/demo_events '{"type":"topic","durable":true,"auto_delete":false,"internal":false,"arguments":{}}'
put queues/demo%20%2F%20%D0%A1%D0%BE%D1%84%D0%B8%D1%8F/special '{"durable":true,"arguments":{}}'
put policies/%2F/demo_ttl '{"pattern":"^demo_records","definition":{"message-ttl":3600000},"priority":0,"apply-to":"queues"}'
i=0
while [ "$i" -lt 120 ]; do
    name=$(printf 'demo_records_%03d' "$i")
    put "queues/%2F/$name" '{"durable":true,"auto_delete":false,"arguments":{}}'
    curl --fail-with-body --silent --show-error --max-time 10 \
        -u fixture-admin:fixture-admin-only -H 'Content-Type: application/json' \
        -X POST --data '{"routing_key":"demo.#","arguments":{}}' \
        "http://127.0.0.1:15672/api/bindings/%2F/e/demo_events/q/$name"
    i=$((i + 1))
done
put queues/%2F/demo_quorum '{"durable":true,"arguments":{"x-queue-type":"quorum"}}'
put queues/%2F/demo_stream '{"durable":true,"arguments":{"x-queue-type":"stream","x-max-length-bytes":8388608}}'
printf 'RabbitMQ: 120 paged queues, quorum/stream queues, bindings, policy and Unicode vhost\n'
