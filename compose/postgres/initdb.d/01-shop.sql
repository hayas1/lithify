CREATE SCHEMA shop;

CREATE TABLE shop.customers (
    id         BIGSERIAL PRIMARY KEY,
    email      TEXT        NOT NULL UNIQUE,
    name       TEXT        NOT NULL,
    is_active  BOOLEAN     NOT NULL DEFAULT TRUE,
    tags       TEXT[]      NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TYPE shop.order_status AS ENUM ('pending', 'paid', 'shipped', 'cancelled');

CREATE TABLE shop.orders (
    id          BIGSERIAL PRIMARY KEY,
    customer_id BIGINT            NOT NULL REFERENCES shop.customers (id),
    status      shop.order_status NOT NULL DEFAULT 'pending',
    amount      NUMERIC(12, 2)    NOT NULL,
    attributes  JSONB             NULL,
    ordered_on  DATE              NOT NULL,
    updated_at  TIMESTAMPTZ       NOT NULL DEFAULT now()
);

CREATE TABLE shop.order_items (
    order_id   BIGINT         NOT NULL REFERENCES shop.orders (id),
    line_no    INT            NOT NULL,
    sku        TEXT           NOT NULL,
    quantity   INT            NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL,
    PRIMARY KEY (order_id, line_no)
);

CREATE TABLE shop.products (
    id         BIGSERIAL PRIMARY KEY,
    sku        TEXT           NOT NULL UNIQUE,
    name       TEXT           NOT NULL,
    price      NUMERIC(12, 2) NOT NULL,
    updated_at TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE TABLE shop.heartbeat (
    server_id INT         PRIMARY KEY,
    beat_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE shop.schema_migrations (
    version    TEXT        PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE FUNCTION shop.touch_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER touch_updated_at BEFORE UPDATE ON shop.customers
    FOR EACH ROW EXECUTE FUNCTION shop.touch_updated_at();
CREATE TRIGGER touch_updated_at BEFORE UPDATE ON shop.orders
    FOR EACH ROW EXECUTE FUNCTION shop.touch_updated_at();
CREATE TRIGGER touch_updated_at BEFORE UPDATE ON shop.products
    FOR EACH ROW EXECUTE FUNCTION shop.touch_updated_at();

INSERT INTO shop.customers (email, name, tags) VALUES
    ('alice@example.com', 'Alice', '{vip}'),
    ('bob@example.com', 'Bob', '{}');

INSERT INTO shop.orders (customer_id, status, amount, attributes, ordered_on) VALUES
    (1, 'paid', 1200.50, '{"channel": "web"}', '2026-09-01'),
    (2, 'pending', 30.00, NULL, '2026-09-02');

INSERT INTO shop.order_items (order_id, line_no, sku, quantity, unit_price) VALUES
    (1, 1, 'SKU-001', 2, 600.25),
    (2, 1, 'SKU-002', 3, 10.00);

INSERT INTO shop.products (sku, name, price) VALUES
    ('SKU-001', 'Kettle', 600.25),
    ('SKU-002', 'Mug', 10.00);

INSERT INTO shop.heartbeat (server_id, beat_at) VALUES
    (1, now());

INSERT INTO shop.schema_migrations (version) VALUES
    ('20260901000000'),
    ('20260915000000');
