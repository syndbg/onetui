CREATE ROLE bpearl_reader LOGIN PASSWORD 'fixture-reader-only';
ALTER ROLE bpearl_reader SET default_transaction_read_only = on;
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
GRANT CONNECT ON DATABASE bpearl_fixture TO bpearl_reader;
GRANT USAGE ON SCHEMA public TO bpearl_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO bpearl_reader;

