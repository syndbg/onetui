CREATE ROLE onetui_reader LOGIN PASSWORD 'fixture-reader-only';
ALTER ROLE onetui_reader SET default_transaction_read_only = on;
CREATE TYPE order_state AS ENUM ('pending', 'paid');
CREATE DOMAIN positive_amount AS numeric CHECK (VALUE >= 0);
CREATE TABLE sample_rows (
    ordinal integer,
    note text,
    amount numeric(30, 10),
    bytes bytea,
    tags text[],
    state order_state,
    price positive_amount
);
INSERT INTO sample_rows VALUES
    (1, NULL, 12345678901234567890.1234567890, decode('00ff', 'hex'), ARRAY['a', 'b'], 'paid', 12.34),
    (2, '', 0, ''::bytea, ARRAY[]::text[], 'pending', NULL),
    (3, 'NULL', NULL, NULL, NULL, 'paid', 0),
    (4, E'Unicode: София 🌊\ncontrol: \x1b[31m', 1, NULL, ARRAY['quote"'], 'pending', 1);
CREATE VIEW sample_view AS SELECT ordinal, note FROM sample_rows;
CREATE TABLE keyed_rows (id bigint PRIMARY KEY, note text);
INSERT INTO keyed_rows VALUES (9007199254740993, 'first'), (9007199254740994, 'second'), (9007199254740995, 'third');
CREATE TABLE composite_rows (
    tenant text COLLATE "C",
    id bigint,
    PRIMARY KEY (tenant, id)
);
INSERT INTO composite_rows VALUES ('a', 1), ('a', 2), ('b''; DELETE FROM keyed_rows; --', 1), ('b''; DELETE FROM keyed_rows; --', 2);
CREATE TABLE changing_rows (id bigint PRIMARY KEY);
INSERT INTO changing_rows VALUES (1), (2), (3), (4);
CREATE SCHEMA empty_schema;
GRANT USAGE ON SCHEMA empty_schema TO onetui_reader;
CREATE TABLE restricted_rows (id integer);
CREATE TABLE "quoted'; -- relation" ("odd""column" text);
INSERT INTO "quoted'; -- relation" VALUES ('quoted value');
CREATE TABLE browse_composite (
    tenant text COLLATE "C",
    id bigint,
    c0 text,
    PRIMARY KEY (tenant, id)
);
INSERT INTO browse_composite SELECT E'a''; --\nСофия', i, 'value' FROM generate_series(1, 205) AS i;
INSERT INTO browse_composite SELECT 'b', i, 'value' FROM generate_series(1, 205) AS i;
CREATE TABLE browse_bigint (id bigint PRIMARY KEY);
INSERT INTO browse_bigint SELECT 9007199254740992 + i FROM generate_series(1, 205) AS i;
CREATE VIEW browse_offset AS SELECT id FROM browse_bigint;
CREATE TABLE browse_zero ();
CREATE TABLE browse_nullable (id bigint UNIQUE);
INSERT INTO browse_nullable VALUES (NULL), (1);
CREATE TABLE browse_partial (id bigint NOT NULL);
CREATE UNIQUE INDEX ON browse_partial (id) WHERE id > 0;
CREATE TABLE browse_expression (id bigint NOT NULL);
CREATE UNIQUE INDEX ON browse_expression ((id + 1));
CREATE TABLE browse_uuid (id uuid PRIMARY KEY);
CREATE TABLE browse_parent (id bigint PRIMARY KEY);
CREATE TABLE browse_child () INHERITS (browse_parent);
CREATE VIEW browse_oversized AS SELECT repeat('x', 1048577) AS value;
CREATE VIEW browse_page_limit AS SELECT repeat('x', 20000) AS value FROM generate_series(1, 100);
CREATE TABLE browse_lookahead (id bigint PRIMARY KEY, value text);
INSERT INTO browse_lookahead SELECT i, CASE WHEN i = 101 THEN repeat('x', 1048577) ELSE 'ok' END FROM generate_series(1, 101) AS i;
CREATE VIEW browse_slow AS SELECT 'done'::text AS value FROM pg_sleep(30);
GRANT CONNECT ON DATABASE onetui_fixture TO onetui_reader;
GRANT USAGE ON SCHEMA public TO onetui_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO onetui_reader;
REVOKE SELECT ON restricted_rows FROM onetui_reader;
