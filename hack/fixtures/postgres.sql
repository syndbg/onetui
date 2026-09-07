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
GRANT CONNECT ON DATABASE onetui_fixture TO onetui_reader;
GRANT USAGE ON SCHEMA public TO onetui_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO onetui_reader;
