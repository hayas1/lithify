CREATE DATABASE shop;
USE shop;

CREATE TABLE customers (
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    email      VARCHAR(255) NOT NULL UNIQUE,
    name       VARCHAR(255) NOT NULL,
    is_active  BOOLEAN      NOT NULL DEFAULT TRUE,
    tags       JSON         NOT NULL DEFAULT (JSON_ARRAY()),
    created_at DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
);

CREATE TABLE orders (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    customer_id BIGINT         NOT NULL,
    status      ENUM('pending', 'paid', 'shipped', 'cancelled') NOT NULL DEFAULT 'pending',
    amount      DECIMAL(12, 2) NOT NULL,
    attributes  JSON           NULL,
    ordered_on  DATE           NOT NULL,
    updated_at  DATETIME(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    FOREIGN KEY (customer_id) REFERENCES customers (id)
);

CREATE TABLE order_items (
    order_id   BIGINT         NOT NULL,
    line_no    INT            NOT NULL,
    sku        VARCHAR(64)    NOT NULL,
    quantity   INT            NOT NULL,
    unit_price DECIMAL(12, 2) NOT NULL,
    PRIMARY KEY (order_id, line_no),
    FOREIGN KEY (order_id) REFERENCES orders (id)
);

CREATE TABLE products (
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    sku        VARCHAR(64)    NOT NULL UNIQUE,
    name       VARCHAR(255)   NOT NULL,
    price      DECIMAL(12, 2) NOT NULL,
    updated_at DATETIME(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
);

CREATE TABLE heartbeat (
    server_id INT         PRIMARY KEY,
    beat_at   DATETIME(6) NOT NULL
);

CREATE TABLE schema_migrations (
    version    VARCHAR(64) PRIMARY KEY,
    applied_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);

INSERT INTO customers (email, name, tags) VALUES
    ('alice@example.com', 'Alice', JSON_ARRAY('vip')),
    ('bob@example.com', 'Bob', JSON_ARRAY());

INSERT INTO orders (customer_id, status, amount, attributes, ordered_on) VALUES
    (1, 'paid', 1200.50, '{"channel": "web"}', '2026-09-01'),
    (2, 'pending', 30.00, NULL, '2026-09-02');

INSERT INTO order_items (order_id, line_no, sku, quantity, unit_price) VALUES
    (1, 1, 'SKU-001', 2, 600.25),
    (2, 1, 'SKU-002', 3, 10.00);

INSERT INTO products (sku, name, price) VALUES
    ('SKU-001', 'Kettle', 600.25),
    ('SKU-002', 'Mug', 10.00);

INSERT INTO heartbeat (server_id, beat_at) VALUES
    (1, CURRENT_TIMESTAMP(6));

INSERT INTO schema_migrations (version) VALUES
    ('20260901000000'),
    ('20260915000000');
