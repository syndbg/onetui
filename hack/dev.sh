#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd -- "$repo_root"
compose=(docker compose --project-name onetui-fixtures --env-file /dev/null -f "$repo_root/hack/compose.yaml")

# Never inherit real database credentials for fixture commands.
export ONETUI_POSTGRES_URL='postgresql://onetui_reader:fixture-reader-only@127.0.0.1:15432/onetui_fixture?sslmode=disable'
export ONETUI_QDRANT_API_KEY='fixture-reader-only'

check_connection() {
    ./target/debug/onetui --check --config hack/connections.toml --connection "$1" --timeout 2
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

up() {
    "${compose[@]}" up -d --wait --wait-timeout 60
    wait_for_connection local_pg
    wait_for_connection local_qdrant
    seed
}

seed() {
    "${compose[@]}" exec -T postgres psql -U onetui_fixture_admin -d onetui_fixture -v ON_ERROR_STOP=1 < hack/fixtures/postgres-demo.sql
    cargo run -p onetui-qdrant --example seed_demo --locked
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

case "${1:-}" in
    up|check|test|run|seed)
        if [[ ! -x target/debug/onetui ]]; then
            printf 'Build first with make build.\n' >&2
            exit 1
        fi
        ;;
    down|logs) ;;
    *) printf 'Usage: bash hack/dev.sh {up|check|test|run|seed|down|logs}\n' >&2; exit 2 ;;
esac

if [[ "$1" == run ]]; then
    exec ./target/debug/onetui --config hack/connections.toml --connection local_pg
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
case "$1" in
    up) up ;;
    seed) seed ;;
    check) check_connection local_pg; check_connection local_qdrant ;;
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
        up
        cargo test --workspace --locked --test fixtures -- --ignored --test-threads=1
        ;;
esac
