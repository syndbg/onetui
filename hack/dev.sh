#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd -- "$repo_root"
compose=(docker compose --project-name bpearl-fixtures --env-file /dev/null -f "$repo_root/hack/compose.yaml")

# Never inherit real database credentials for fixture commands.
export BPEARL_POSTGRES_URL='postgresql://bpearl_reader:fixture-reader-only@127.0.0.1:15432/bpearl_fixture?sslmode=disable'
export BPEARL_QDRANT_API_KEY='fixture-reader-only'

check_connection() {
    ./target/debug/bpearl --check --config hack/connections.toml --connection "$1" --timeout 2
}

up() {
    "${compose[@]}" up -d --wait --wait-timeout 60
    # Qdrant's image has no healthcheck utility. Probe the same authenticated RPC as the CLI.
    local alias deadline
    for alias in local_pg local_qdrant; do
        deadline=$((SECONDS + 60))
        until check_connection "$alias" >/dev/null 2>&1; do
            if (( SECONDS >= deadline )); then
                check_connection "$alias"
                return 1
            fi
            sleep 1
        done
        printf 'Ready: %s\n' "$alias"
    done
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
    up|check|test)
        if [[ ! -x target/debug/bpearl ]]; then
            printf 'Build first with make build.\n' >&2
            exit 1
        fi
        ;;
    down|logs) ;;
    *) printf 'Usage: bash hack/dev.sh {up|check|test|down|logs}\n' >&2; exit 2 ;;
esac

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
    check) check_connection local_pg; check_connection local_qdrant ;;
    down) "${compose[@]}" down --timeout 10 ;;
    logs) "${compose[@]}" logs --no-color --tail 100 ;;
    test)
        if [[ -n $("${compose[@]}" ps --all --quiet) ]]; then
            printf 'Existing bpearl-fixtures containers found; refusing to reset them. Run make dev-down first.\n' >&2
            exit 1
        fi
        trap cleanup EXIT
        trap 'exit 130' INT
        trap 'exit 143' TERM
        up
        cargo test --locked --test fixtures -- --ignored --test-threads=1
        ;;
esac
