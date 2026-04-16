-- 001_schema_pg.sql - Retail Analytics (PostgreSQL / Supabase)
-- Converted from MySQL 001_schema.sql
-- Run this in Supabase SQL Editor, then disable RLS (see below)

-- ======================================================
-- Dimension Tables
-- ======================================================

CREATE TABLE IF NOT EXISTS products (
  product_id          BIGINT PRIMARY KEY,
  sku                 VARCHAR(64) NOT NULL,
  product_name        VARCHAR(255) NOT NULL,
  brand               VARCHAR(120) NOT NULL,
  product_category    VARCHAR(80) NOT NULL,
  product_subcategory VARCHAR(120) NOT NULL,
  target_audience     VARCHAR(32) NULL,
  hair_type           VARCHAR(64) NULL,
  skin_type           VARCHAR(64) NULL,
  scent_profile       VARCHAR(64) NULL,
  claim_tags          VARCHAR(255) NULL,
  package_size_ml_g   INT NULL,
  package_type        VARCHAR(32) NULL,
  unit_cost           DECIMAL(10,2) NOT NULL,
  list_price          DECIMAL(10,2) NOT NULL,
  launch_date         DATE NULL,
  is_active           SMALLINT NOT NULL DEFAULT 1,
  CONSTRAINT uk_products_sku UNIQUE (sku)
);
CREATE INDEX IF NOT EXISTS idx_products_cat_sub ON products (product_category, product_subcategory);
CREATE INDEX IF NOT EXISTS idx_products_brand ON products (brand);


CREATE TABLE IF NOT EXISTS customers (
  customer_id   BIGINT PRIMARY KEY,
  customer_name VARCHAR(255) NOT NULL,
  email_hash    VARCHAR(128) NULL,
  gender        VARCHAR(32) NULL,
  birth_date    DATE NULL,
  city          VARCHAR(120) NULL,
  state         VARCHAR(64) NULL,
  country       VARCHAR(64) NULL,
  income_range  VARCHAR(32) NULL,
  signup_date   DATE NULL,
  loyalty_tier  VARCHAR(32) NULL
);
CREATE INDEX IF NOT EXISTS idx_customers_geo ON customers (country, state, city);
CREATE INDEX IF NOT EXISTS idx_customers_loyalty ON customers (loyalty_tier);


CREATE TABLE IF NOT EXISTS stores (
  store_id   BIGINT PRIMARY KEY,
  store_name VARCHAR(255) NOT NULL,
  channel    VARCHAR(40) NOT NULL,
  city       VARCHAR(120) NULL,
  state      VARCHAR(64) NULL,
  country    VARCHAR(64) NULL,
  region     VARCHAR(64) NULL
);
CREATE INDEX IF NOT EXISTS idx_stores_channel ON stores (channel);
CREATE INDEX IF NOT EXISTS idx_stores_geo ON stores (country, state, city);


-- ======================================================
-- Fact Tables
-- ======================================================

CREATE TABLE IF NOT EXISTS orders (
  order_id        BIGINT PRIMARY KEY,
  customer_id     BIGINT NOT NULL,
  store_id        BIGINT NOT NULL,
  order_datetime  TIMESTAMP NOT NULL,
  payment_method  VARCHAR(40) NULL,
  shipping_type   VARCHAR(40) NULL,
  coupon_code     VARCHAR(60) NULL,
  order_status    VARCHAR(24) NOT NULL,
  gross_amount    DECIMAL(12,2) NOT NULL,
  discount_amount DECIMAL(12,2) NOT NULL DEFAULT 0,
  tax_amount      DECIMAL(12,2) NOT NULL DEFAULT 0,
  shipping_amount DECIMAL(12,2) NOT NULL DEFAULT 0,
  net_amount      DECIMAL(12,2) NOT NULL,
  CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
  CONSTRAINT fk_orders_store    FOREIGN KEY (store_id)    REFERENCES stores(store_id)
);
CREATE INDEX IF NOT EXISTS idx_orders_datetime ON orders (order_datetime);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders (customer_id, order_datetime);
CREATE INDEX IF NOT EXISTS idx_orders_store    ON orders (store_id, order_datetime);


CREATE TABLE IF NOT EXISTS order_items (
  order_item_id   BIGINT PRIMARY KEY,
  order_id        BIGINT NOT NULL,
  product_id      BIGINT NOT NULL,
  quantity        INT NOT NULL,
  unit_price      DECIMAL(12,2) NOT NULL,
  discount_amount DECIMAL(12,2) NOT NULL DEFAULT 0,
  net_line_amount DECIMAL(12,2) NOT NULL,
  CONSTRAINT fk_items_order   FOREIGN KEY (order_id)   REFERENCES orders(order_id),
  CONSTRAINT fk_items_product FOREIGN KEY (product_id) REFERENCES products(product_id)
);
CREATE INDEX IF NOT EXISTS idx_items_order   ON order_items (order_id);
CREATE INDEX IF NOT EXISTS idx_items_product ON order_items (product_id);


CREATE TABLE IF NOT EXISTS inventory_snapshots (
  snapshot_date    DATE NOT NULL,
  store_id         BIGINT NOT NULL,
  product_id       BIGINT NOT NULL,
  stock_on_hand    INT NOT NULL,
  stock_reserved   INT NOT NULL DEFAULT 0,
  stock_in_transit INT NOT NULL DEFAULT 0,
  reorder_point    INT NULL,
  PRIMARY KEY (snapshot_date, store_id, product_id),
  CONSTRAINT fk_inventory_store   FOREIGN KEY (store_id)   REFERENCES stores(store_id),
  CONSTRAINT fk_inventory_product FOREIGN KEY (product_id) REFERENCES products(product_id)
);
CREATE INDEX IF NOT EXISTS idx_inventory_product_date ON inventory_snapshots (product_id, snapshot_date);
CREATE INDEX IF NOT EXISTS idx_inventory_store_date   ON inventory_snapshots (store_id, snapshot_date);


-- ======================================================
-- Text Tables
-- ======================================================

CREATE TABLE IF NOT EXISTS complaints (
  complaint_id         BIGINT PRIMARY KEY,
  customer_id          BIGINT NULL,
  order_id             BIGINT NULL,
  product_id           BIGINT NOT NULL,
  complaint_datetime   TIMESTAMP NOT NULL,
  complaint_channel    VARCHAR(40) NOT NULL,
  complaint_type       VARCHAR(80) NOT NULL,
  complaint_text       TEXT NOT NULL,
  severity             VARCHAR(16) NOT NULL,
  status               VARCHAR(24) NOT NULL,
  resolution_time_days INT NULL,
  CONSTRAINT fk_complaints_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
  CONSTRAINT fk_complaints_order    FOREIGN KEY (order_id)    REFERENCES orders(order_id),
  CONSTRAINT fk_complaints_product  FOREIGN KEY (product_id)  REFERENCES products(product_id)
);
CREATE INDEX IF NOT EXISTS idx_complaints_product_time ON complaints (product_id, complaint_datetime);
CREATE INDEX IF NOT EXISTS idx_complaints_status ON complaints (status);
CREATE INDEX IF NOT EXISTS idx_complaints_type   ON complaints (complaint_type);


CREATE TABLE IF NOT EXISTS reviews (
  review_id       BIGINT PRIMARY KEY,
  customer_id     BIGINT NOT NULL,
  product_id      BIGINT NOT NULL,
  review_datetime TIMESTAMP NOT NULL,
  rating          SMALLINT NOT NULL,
  review_title    VARCHAR(255) NULL,
  review_text     TEXT NOT NULL,
  CONSTRAINT fk_reviews_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
  CONSTRAINT fk_reviews_product  FOREIGN KEY (product_id)  REFERENCES products(product_id)
);
CREATE INDEX IF NOT EXISTS idx_reviews_product_time ON reviews (product_id, review_datetime);
CREATE INDEX IF NOT EXISTS idx_reviews_rating ON reviews (rating);


CREATE TABLE IF NOT EXISTS returns (
  return_id       BIGINT PRIMARY KEY,
  order_id        BIGINT NOT NULL,
  product_id      BIGINT NOT NULL,
  return_datetime TIMESTAMP NOT NULL,
  quantity        INT NOT NULL,
  return_reason   VARCHAR(120) NOT NULL,
  refund_amount   DECIMAL(12,2) NOT NULL,
  CONSTRAINT fk_returns_order   FOREIGN KEY (order_id)   REFERENCES orders(order_id),
  CONSTRAINT fk_returns_product FOREIGN KEY (product_id) REFERENCES products(product_id)
);
CREATE INDEX IF NOT EXISTS idx_returns_product_time ON returns (product_id, return_datetime);
CREATE INDEX IF NOT EXISTS idx_returns_order ON returns (order_id);


-- ======================================================
-- Embeddings (future use)
-- ======================================================

CREATE TABLE IF NOT EXISTS text_embeddings (
  doc_type       VARCHAR(32) NOT NULL,
  doc_id         BIGINT NOT NULL,
  model          VARCHAR(64) NOT NULL,
  embedding_json JSONB NOT NULL,
  created_at     TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (doc_type, doc_id, model)
);


-- ======================================================
-- AFTER RUNNING THIS FILE: Disable RLS for all tables
-- (Required so the anon key can read data)
-- ======================================================
--
-- ALTER TABLE products            DISABLE ROW LEVEL SECURITY;
-- ALTER TABLE customers           DISABLE ROW LEVEL SECURITY;
-- ALTER TABLE stores              DISABLE ROW LEVEL SECURITY;
-- ALTER TABLE orders              DISABLE ROW LEVEL SECURITY;
-- ALTER TABLE order_items         DISABLE ROW LEVEL SECURITY;
-- ALTER TABLE inventory_snapshots DISABLE ROW LEVEL SECURITY;
-- ALTER TABLE reviews             DISABLE ROW LEVEL SECURITY;
-- ALTER TABLE returns             DISABLE ROW LEVEL SECURITY;
-- ALTER TABLE complaints          DISABLE ROW LEVEL SECURITY;
-- ALTER TABLE text_embeddings     DISABLE ROW LEVEL SECURITY;
