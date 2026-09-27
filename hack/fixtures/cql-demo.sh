#!/bin/sh
# Print the CQL demo schema and data for one fixture: scylla or cassandra. Roles exist
# only on ScyllaDB, which runs with password authentication. Statements are idempotent,
# so reseeding a running fixture is safe.
set -eu

cat <<'CQL'
CREATE KEYSPACE IF NOT EXISTS onetui_demo
    WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
CREATE KEYSPACE IF NOT EXISTS onetui_writable
    WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
CREATE TYPE IF NOT EXISTS onetui_demo.address (city text, zip text);
CREATE TABLE IF NOT EXISTS onetui_demo.events (
    bucket int, seq int, id uuid, at timestamp, day date, payload blob,
    amount decimal, big varint, tags set<text>, attrs map<text, int>, note text,
    PRIMARY KEY (bucket, seq));
CREATE TABLE IF NOT EXISTS onetui_demo.profiles (
    id int PRIMARY KEY, home frozen<address>, pair tuple<int, text>);
CREATE TABLE IF NOT EXISTS onetui_demo."CaseSensitive" (id int PRIMARY KEY, "Value" text);
CREATE TABLE IF NOT EXISTS onetui_writable.notes (id int PRIMARY KEY, body text);
INSERT INTO onetui_demo.profiles (id, home, pair) VALUES (1, {city: 'Sofia', zip: null}, (7, 'seven'));
INSERT INTO onetui_demo."CaseSensitive" (id, "Value") VALUES (1, 'quoted');
CQL

# 250 rows in one partition: two full pages of 100 and a partial third.
seq=0
while [ "$seq" -lt 250 ]; do
    printf "INSERT INTO onetui_demo.events (bucket, seq, id, at, day, payload, amount, big, tags, attrs, note) VALUES (0, %d, 00000000-0000-0000-0000-%012d, %d, '2000-02-29', 0x00ff, 12.34, 1000000000000000000000, {'a', 'b'}, {'k': %d}, 'row %d');\n" \
        "$seq" "$seq" "$((1700000000000 + seq))" "$seq" "$seq"
    seq=$((seq + 1))
done
# A null cell, distinct from an empty one.
echo "INSERT INTO onetui_demo.events (bucket, seq, note) VALUES (1, 0, null);"

if [ "${1:-}" = scylla ]; then
    cat <<'CQL'
CREATE ROLE IF NOT EXISTS fixture_reader WITH PASSWORD = 'fixture-reader-only' AND LOGIN = true;
GRANT SELECT ON ALL KEYSPACES TO fixture_reader;
CREATE ROLE IF NOT EXISTS fixture_writer WITH PASSWORD = 'fixture-writer-only' AND LOGIN = true;
GRANT SELECT ON ALL KEYSPACES TO fixture_writer;
GRANT MODIFY ON KEYSPACE onetui_writable TO fixture_writer;
GRANT CREATE ON KEYSPACE onetui_writable TO fixture_writer;
GRANT DROP ON KEYSPACE onetui_writable TO fixture_writer;
CQL
fi
