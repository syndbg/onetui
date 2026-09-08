-- Browse demo.* for realistic volume; public.* retains small regression fixtures.
-- Re-running adds missing IDs only and preserves edits to existing rows.
BEGIN;
CREATE SCHEMA IF NOT EXISTS demo;

CREATE TABLE IF NOT EXISTS demo.customers (
    id bigint PRIMARY KEY,
    external_id uuid NOT NULL,
    name text NOT NULL,
    email text,
    country text,
    city text,
    active boolean,
    age smallint,
    orders integer,
    balance numeric(20, 4),
    created_at timestamptz,
    last_seen timestamp,
    birthday date,
    tags text[],
    preferences jsonb,
    notes text
);
INSERT INTO demo.customers
SELECT n, md5('customer-' || n)::uuid,
    (ARRAY['Анна', 'Mina', 'José', '李明', 'Zoë', 'Sam'])[1 + n % 6] || ' ' || n,
    CASE WHEN n % 11 = 0 THEN NULL ELSE 'customer-' || n || '@example.invalid' END,
    (ARRAY['BG', 'JP', 'BR', 'DE'])[1 + n % 4],
    (ARRAY['София', '東京', 'São Paulo', 'Berlin'])[1 + n % 4],
    n % 3 <> 0, (18 + n % 70)::smallint, n % 90,
    (n * 17.125 - 5000)::numeric(20,4),
    timestamptz '2025-01-01 00:00:00+00' + n * interval '1 hour',
    timestamp '2025-06-01 12:00:00' + n * interval '1 minute',
    date '1950-01-01' + (n % 20000),
    ARRAY['demo', CASE WHEN n % 2 = 0 THEN 'premium' ELSE 'standard' END],
    jsonb_build_object('locale', 'en', 'notifications', n % 2 = 0,
        'limits', jsonb_build_object('daily', n % 100), 'optional', NULL),
    CASE n % 100 WHEN 0 THEN repeat('Long note: София 🌊. ', 600)
        WHEN 1 THEN NULL WHEN 2 THEN '' WHEN 3 THEN 'NULL'
        ELSE 'Synthetic customer ' || n END
FROM generate_series(1, 2000) n
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS demo.events (
    id bigint PRIMARY KEY,
    customer_id bigint REFERENCES demo.customers(id),
    happened_at timestamptz,
    category text,
    severity integer,
    duration_ms double precision,
    success boolean,
    source inet,
    attributes jsonb,
    message text
);
INSERT INTO demo.events
SELECT n, 1 + n % 2000, timestamptz '2025-07-01 00:00:00+00' + n * interval '1 second',
    (ARRAY['login', 'search', 'purchase', 'logout'])[1 + n % 4], n % 5,
    n * 0.125, n % 7 <> 0, '10.0.0.1'::inet + n,
    jsonb_build_object('request', n, 'tags', jsonb_build_array('demo', 'event'), 'retry', n % 3),
    'Synthetic event ' || n
FROM generate_series(1, 5000) n
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS demo.type_samples (
    id bigint PRIMARY KEY,
    small_number smallint,
    integer_number integer,
    large_number bigint,
    exact_number numeric(30,10),
    single_float real,
    double_float double precision,
    enabled boolean,
    fixed_char char(8),
    short_text varchar(80),
    unicode_text text,
    nullable_text text,
    bytes bytea,
    uuid_value uuid,
    date_value date,
    time_value time,
    time_with_zone timetz,
    timestamp_value timestamp,
    timestamp_with_zone timestamptz,
    duration interval,
    json_value json,
    jsonb_value jsonb,
    text_array text[],
    integer_array integer[],
    matrix integer[][],
    address inet,
    network cidr,
    hardware_address macaddr,
    bits bit(8),
    varying_bits varbit,
    integer_range int4range,
    date_range daterange,
    timestamp_range tsrange,
    search_document tsvector,
    search_query tsquery,
    location point,
    boundary box,
    currency money,
    state public.order_state,
    price public.positive_amount
);
INSERT INTO demo.type_samples
SELECT n, (n % 32000)::smallint, n, 9007199254740992 + n::bigint,
    12345678901234567890.1234567890,
    CASE WHEN n % 5 = 0 THEN 'NaN'::real ELSE (n / 7.0)::real END,
    CASE WHEN n % 7 = 0 THEN 'Infinity'::double precision
        WHEN n % 7 = 1 THEN '-Infinity'::double precision ELSE n / 3.0 END,
    CASE WHEN n % 3 = 0 THEN NULL ELSE n % 2 = 0 END,
    'row-' || n, 'varchar ' || n, E'Unicode: София 東京 🌊\nsecond line',
    CASE n % 4 WHEN 0 THEN NULL WHEN 1 THEN '' WHEN 2 THEN 'NULL' ELSE 'text' END,
    decode('00017fff' || lpad(to_hex(n), 4, '0'), 'hex'),
    md5('type-' || n)::uuid, date '2025-01-01' + n,
    time '10:30:00', timetz '10:30:00+02',
    timestamp '2025-01-01 12:00:00' + n * interval '1 hour',
    timestamptz '2025-01-01 12:00:00+00' + n * interval '1 hour',
    n * interval '1 minute',
    json_build_object('id', n, 'empty', '', 'null', NULL),
    jsonb_build_object('nested', jsonb_build_object('items', jsonb_build_array(1, true, NULL, 'София'))),
    ARRAY['alpha', NULL, '', 'NULL', '🌊'], ARRAY[n, n+1, NULL],
    ARRAY[ARRAY[n,n+1], ARRAY[n+2,n+3]], '10.0.0.1'::inet + n,
    '192.168.0.0/24'::cidr, '08:00:2b:01:02:03'::macaddr,
    n::bit(8), B'10101', int4range(n,n+10), daterange(date '2025-01-01', date '2025-02-01'),
    tsrange(timestamp '2025-01-01', timestamp '2025-02-01'),
    to_tsvector('simple', 'synthetic search document ' || n), to_tsquery('simple', 'synthetic & search'),
    point(n / 10.0, n / 20.0), box(point(0,0), point(n,n)), (n / 100.0)::money,
    (CASE WHEN n % 2 = 0 THEN 'paid' ELSE 'pending' END)::public.order_state,
    (n / 100.0)::public.positive_amount
FROM generate_series(1, 250) n
ON CONFLICT DO NOTHING;

DO $$
DECLARE
    column_defs text;
    value_exprs text;
BEGIN
    SELECT string_agg(format('field_%s text', lpad(i::text, 2, '0')), ', ' ORDER BY i),
           string_agg(format('CASE WHEN (n + %s) %% 13 = 0 THEN NULL ELSE ''row-'' || n || '' / field-%s'' END',
               i, lpad(i::text, 2, '0')), ', ' ORDER BY i)
    INTO column_defs, value_exprs FROM generate_series(1, 64) i;
    EXECUTE 'CREATE TABLE IF NOT EXISTS demo.wide_rows (id bigint PRIMARY KEY, ' || column_defs || ')';
    EXECUTE 'INSERT INTO demo.wide_rows SELECT n, ' || value_exprs ||
        ' FROM generate_series(1, 1500) n ON CONFLICT DO NOTHING';
END $$;

CREATE TABLE IF NOT EXISTS demo.empty_rows (id bigint PRIMARY KEY, value text);
DO $$ BEGIN
    IF to_regclass('demo.active_customers') IS NULL THEN
        EXECUTE 'CREATE VIEW demo.active_customers AS SELECT id, name, country, balance FROM demo.customers WHERE active';
    END IF;
END $$;
GRANT USAGE ON SCHEMA demo TO onetui_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA demo TO onetui_reader;
COMMIT;
