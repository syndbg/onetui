#!/bin/sh
set -eu
if [ -e "$PGDATA/PG_VERSION" ]; then
    printf 'Replica fixture requires an empty data directory.\n' >&2
    exit 1
fi
mkdir -p "$PGDATA"
chown postgres:postgres "$PGDATA"
chmod 700 "$PGDATA"
attempt=0
until pg_isready -h postgres -U onetui_replication; do
    attempt=$((attempt + 1))
    [ "$attempt" -lt 60 ] || exit 1
    sleep 1
done
gosu postgres pg_basebackup \
    --dbname="host=postgres user=onetui_replication password=fixture-replica-only application_name=onetui-fixture-standby" \
    --pgdata="$PGDATA" --write-recovery-conf --wal-method=stream
exec gosu postgres postgres -D "$PGDATA"
