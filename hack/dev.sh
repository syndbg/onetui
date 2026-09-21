#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd -- "$repo_root"
compose=(docker compose --project-name onetui-fixtures --env-file /dev/null -f "$repo_root/hack/compose.yaml")

# Never inherit real database credentials for fixture commands.
export ONETUI_POSTGRES_URL='postgresql://onetui_reader:fixture-reader-only@127.0.0.1:15432/onetui_fixture?sslmode=disable'
export ONETUI_POSTGRES_REPLICA_URL='postgresql://onetui_reader:fixture-reader-only@127.0.0.1:15433/onetui_fixture?sslmode=disable'
export ONETUI_QDRANT_API_KEY='fixture-reader-only'
export ONETUI_NATS_USERNAME='fixture-reader'
export ONETUI_NATS_PASSWORD='fixture-reader-only'
export ONETUI_RABBITMQ_USERNAME='fixture-reader'
export ONETUI_RABBITMQ_PASSWORD='fixture-reader-only'
export ONETUI_DYNAMODB_ACCESS_KEY='onetuiFixtureOnly'
export ONETUI_DYNAMODB_SECRET_KEY='fixture-secret-only'

check_connection() {
    ./target/debug/onetui --check --config target/demo-onetui.toml --connection "$1" --timeout 2
}

wait_for_connection() {
    local alias=$1 deadline=$((SECONDS + 60))
    until check_connection "$alias" >/dev/null 2>&1; do
        if (( SECONDS >= deadline )); then
            check_connection "$alias"
            return 1
        fi
        sleep 1
    done
    printf 'Ready: %s\n' "$alias"
}

start_fixtures() {
    local attempt status=0
    for attempt in 1 2 3; do
        if "${compose[@]}" up -d --wait --wait-timeout 60 "$@"; then
            return
        else
            status=$?
        fi
        if (( attempt == 3 )); then
            return "$status"
        fi
        printf 'Fixture startup failed; retrying (%s/3).\n' "$attempt" >&2
        sleep $((attempt * 5))
    done
}

seed_postgres() {
    "${compose[@]}" exec -T postgres psql -U onetui_fixture_admin -d onetui_fixture -v ON_ERROR_STOP=1 < hack/fixtures/postgres-demo.sql
}

seed_qdrant() {
    cargo run -p onetui-qdrant --example seed_demo --locked
}

seed_kafka() {
    "${compose[@]}" exec -T kafka sh /onetui-kafka-security.sh
    cargo run -p onetui-kafka --example seed_demo --locked
    cargo run -p onetui-kafka --example seed_redpanda --locked
}

seed_nats() {
    cargo run -p onetui-nats --example seed_nats --locked
}

seed_dynamodb() {
    cargo run -p onetui-dynamodb --example seed_dynamodb --locked
}

seed_rabbitmq() {
    sh hack/fixtures/rabbitmq-seed.sh
}

seed() {
    seed_postgres
    seed_qdrant
    seed_kafka
    seed_nats
    seed_dynamodb
    seed_rabbitmq
}

up() {
    start_fixtures
    wait_for_connection local_pg
    wait_for_connection local_pg_replica
    wait_for_connection local_qdrant
    wait_for_connection local_kafka
    wait_for_connection local_redpanda
    wait_for_connection local_nats
    wait_for_connection local_nats_system
    wait_for_connection local_dynamodb
    seed
    wait_for_connection local_rabbitmq
}

up_test_suite() {
    local suite=$1
    local services=()

    case "$suite" in
        postgres) services=(postgres postgres-replica) ;;
        qdrant) services=(qdrant qdrant-peer qdrant-tls) ;;
        kafka) services=(kafka redpanda) ;;
        nats) services=(nats nats-tls nats-secure nats-jwt nats-system nats-system-peer redpanda) ;;
        dynamodb) services=(dynamodb) ;;
        rabbitmq) services=(rabbitmq rabbitmq-traffic) ;;
        tui) services=(postgres qdrant) ;;
    esac

    start_fixtures "${services[@]}"
    case "$suite" in
        postgres)
            wait_for_connection local_pg
            wait_for_connection local_pg_replica
            seed_postgres
            ;;
        qdrant)
            wait_for_connection local_qdrant
            seed_qdrant
            ;;
        kafka)
            wait_for_connection local_kafka
            wait_for_connection local_redpanda
            seed_kafka
            ;;
        nats)
            wait_for_connection local_redpanda
            wait_for_connection local_nats
            wait_for_connection local_nats_system
            seed_nats
            ;;
        dynamodb)
            wait_for_connection local_dynamodb
            seed_dynamodb
            ;;
        rabbitmq)
            seed_rabbitmq
            wait_for_connection local_rabbitmq
            ;;
        tui)
            wait_for_connection local_pg
            wait_for_connection local_qdrant
            seed_postgres
            seed_qdrant
            ;;
    esac
}

cleanup() {
    local status=$?
    trap - EXIT
    if (( status != 0 )); then
        "${compose[@]}" logs --no-color --tail 100 || true
    fi
    "${compose[@]}" down --timeout 10 || status=1
    exit "$status"
}

stop_traffic() {
    local status=$?
    trap - EXIT INT TERM
    # Use the job table even if a signal arrives before a new PID is recorded.
    local pids
    pids=$(jobs -pr)
    if [[ -n $pids ]]; then
        kill -TERM $pids 2>/dev/null || true
    fi
    wait 2>/dev/null || true
    exit "$status"
}

traffic() {
    check_connection local_kafka
    check_connection local_nats
    check_connection local_redpanda
    cargo build -p onetui-kafka --example produce_demo --locked
    cargo build -p onetui-nats --example produce_nats --locked
    traffic_pids=()
    trap stop_traffic EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    ./target/debug/examples/produce_demo &
    traffic_pids+=("$!")
    ./target/debug/examples/produce_nats &
    traffic_pids+=("$!")
    ./target/debug/examples/seed_redpanda --traffic &
    traffic_pids+=("$!")
    # macOS ships Bash 3.2, without wait -n. Notice any producer exiting.
    while true; do
        for pid in "${traffic_pids[@]}"; do
            if ! kill -0 "$pid" 2>/dev/null; then
                local status=0
                wait "$pid" || status=$?
                printf 'Traffic producer exited (status %s); stopping all.\n' "$status" >&2
                return "$status"
            fi
        done
        sleep 0.2
    done
}

command=${1:-}
test_suite=${2:-all}

if [[ "$command" == test ]]; then
    case "$test_suite" in
        all|postgres|qdrant|kafka|nats|dynamodb|rabbitmq|tui) ;;
        *) printf 'Unknown integration test suite: %s\n' "$test_suite" >&2; exit 2 ;;
    esac
fi

case "$command" in
    up|check|test|run|seed|traffic|traffic-kafka|traffic-nats)
        if [[ ! -x target/debug/onetui ]]; then
            printf 'Build first with make build.\n' >&2
            exit 1
        fi
        if [[ "$command" != run ]]; then
            cargo run -p onetui-kafka --example seed_redpanda --locked -- --prepare
            if [[ "$command" != test || "$test_suite" == all || "$test_suite" == nats ]]; then
                cargo run -p onetui-nats --example seed_nats --locked -- --prepare
            fi
        fi
        ;;
    down|logs) ;;
    *) printf 'Usage: bash hack/dev.sh {up|check|test|run|seed|traffic|traffic-kafka|traffic-nats|down|logs}\n' >&2; exit 2 ;;
esac

if [[ "$command" == run ]]; then
    if [[ ! -f target/demo-onetui.toml ]]; then
        printf 'Prepare demos with make dev-up (or make dev-seed for running fixtures).\n' >&2
        exit 1
    fi
    exec ./target/debug/onetui --config target/demo-onetui.toml
fi

# The test clients use localhost; never create/delete fixtures on a remote Docker context.
if [[ -n ${DOCKER_CONTEXT:-} || -z ${DOCKER_HOST:-} ]]; then
    docker_host=$(docker context inspect --format '{{.Endpoints.docker.Host}}')
else
    docker_host=$DOCKER_HOST
fi
if [[ "$docker_host" != unix://* ]]; then
    printf 'Use a local Unix-socket Docker context for these disposable fixtures.\n' >&2
    exit 1
fi
docker info >/dev/null
"${compose[@]}" version >/dev/null
case "$command" in
    up) up ;;
    seed) seed ;;
    traffic) traffic ;;
    traffic-kafka)
        check_connection local_kafka
        exec cargo run -p onetui-kafka --example produce_demo --locked
        ;;
    traffic-nats)
        check_connection local_nats
        exec cargo run -p onetui-nats --example produce_nats --locked
        ;;
    check) check_connection local_pg; check_connection local_pg_replica; check_connection local_qdrant; check_connection local_kafka; check_connection local_redpanda; check_connection local_nats; check_connection local_nats_system; check_connection local_dynamodb; check_connection local_rabbitmq ;;
    down) "${compose[@]}" down --timeout 10 ;;
    logs) "${compose[@]}" logs --no-color --tail 100 ;;
    test)
        if [[ -n $("${compose[@]}" ps --all --quiet) ]]; then
            printf 'Existing onetui-fixtures containers found; refusing to reset them. Run make dev-down first.\n' >&2
            exit 1
        fi
        trap cleanup EXIT
        trap 'exit 130' INT
        trap 'exit 143' TERM
        if [[ "$test_suite" == all ]]; then
            up
            cargo build -p onetui-kafka --example produce_demo --locked
            cargo build -p onetui-nats --example produce_nats --locked
            cargo test --workspace --locked --test fixtures -- --ignored --test-threads=1
        else
            up_test_suite "$test_suite"
            case "$test_suite" in
                kafka) cargo build -p onetui-kafka --example produce_demo --locked ;;
                nats) cargo build -p onetui-nats --example produce_nats --locked ;;
            esac
            cargo test -p "onetui-$test_suite" --locked --test fixtures -- --ignored --test-threads=1
        fi
        ;;
esac
