-- 001_schema.sql - Retail Analytics (MySQL 8)
-- Executado automaticamente na primeira inicialização do container.

USE retail_analytics;

-- ======================================================
-- Permissões explícitas para acesso remoto via DBeaver
-- ======================================================
GRANT ALL PRIVILEGES ON retail_analytics.* TO 'app_user'@'%';
FLUSH PRIVILEGES;

-- ======================================================
-- Tabelas de Dimensão
-- ======================================================

CREATE TABLE IF NOT EXISTS products (
  product_id BIGINT PRIMARY KEY,
  sku VARCHAR(64) NOT NULL,
  product_name VARCHAR(255) NOT NULL,
  brand VARCHAR(120) NOT NULL,
  product_category VARCHAR(80) NOT NULL,
  product_subcategory VARCHAR(120) NOT NULL,
  target_audience VARCHAR(32) NULL,
  hair_type VARCHAR(64) NULL,
  skin_type VARCHAR(64) NULL,
  scent_profile VARCHAR(64) NULL,
  claim_tags VARCHAR(255) NULL,
  package_size_ml_g INT NULL,
  package_type VARCHAR(32) NULL,
  unit_cost DECIMAL(10,2) NOT NULL,
  list_price DECIMAL(10,2) NOT NULL,
  launch_date DATE NULL,
  is_active TINYINT NOT NULL DEFAULT 1,

  UNIQUE KEY uk_products_sku (sku),
  KEY idx_products_cat_sub (product_category, product_subcategory),
  KEY idx_products_brand (brand)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS customers (
  customer_id BIGINT PRIMARY KEY,
  customer_name VARCHAR(255) NOT NULL,
  email_hash VARCHAR(128) NULL,
  gender VARCHAR(32) NULL,
  birth_date DATE NULL,
  city VARCHAR(120) NULL,
  state VARCHAR(64) NULL,
  country VARCHAR(64) NULL,
  income_range VARCHAR(32) NULL,
  signup_date DATE NULL,
  loyalty_tier VARCHAR(32) NULL,

  KEY idx_customers_geo (country, state, city),
  KEY idx_customers_loyalty (loyalty_tier)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS stores (
  store_id BIGINT PRIMARY KEY,
  store_name VARCHAR(255) NOT NULL,
  channel VARCHAR(40) NOT NULL,
  city VARCHAR(120) NULL,
  state VARCHAR(64) NULL,
  country VARCHAR(64) NULL,
  region VARCHAR(64) NULL,

  KEY idx_stores_channel (channel),
  KEY idx_stores_geo (country, state, city)
) ENGINE=InnoDB;

-- ======================================================
-- Tabelas de Fato
-- ======================================================

CREATE TABLE IF NOT EXISTS orders (
  order_id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  store_id BIGINT NOT NULL,
  order_datetime DATETIME NOT NULL,
  payment_method VARCHAR(40) NULL,
  shipping_type VARCHAR(40) NULL,
  coupon_code VARCHAR(60) NULL,
  order_status VARCHAR(24) NOT NULL,
  gross_amount DECIMAL(12,2) NOT NULL,
  discount_amount DECIMAL(12,2) NOT NULL DEFAULT 0,
  tax_amount DECIMAL(12,2) NOT NULL DEFAULT 0,
  shipping_amount DECIMAL(12,2) NOT NULL DEFAULT 0,
  net_amount DECIMAL(12,2) NOT NULL,

  KEY idx_orders_datetime (order_datetime),
  KEY idx_orders_customer (customer_id, order_datetime),
  KEY idx_orders_store (store_id, order_datetime),

  CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
  CONSTRAINT fk_orders_store FOREIGN KEY (store_id) REFERENCES stores(store_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS order_items (
  order_item_id BIGINT PRIMARY KEY,
  order_id BIGINT NOT NULL,
  product_id BIGINT NOT NULL,
  quantity INT NOT NULL,
  unit_price DECIMAL(12,2) NOT NULL,
  discount_amount DECIMAL(12,2) NOT NULL DEFAULT 0,
  net_line_amount DECIMAL(12,2) NOT NULL,

  KEY idx_items_order (order_id),
  KEY idx_items_product (product_id),
  KEY idx_items_product_order (product_id, order_id),

  CONSTRAINT fk_items_order FOREIGN KEY (order_id) REFERENCES orders(order_id),
  CONSTRAINT fk_items_product FOREIGN KEY (product_id) REFERENCES products(product_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS inventory_snapshots (
  snapshot_date DATE NOT NULL,
  store_id BIGINT NOT NULL,
  product_id BIGINT NOT NULL,
  stock_on_hand INT NOT NULL,
  stock_reserved INT NOT NULL DEFAULT 0,
  stock_in_transit INT NOT NULL DEFAULT 0,
  reorder_point INT NULL,

  PRIMARY KEY (snapshot_date, store_id, product_id),
  KEY idx_inventory_product_date (product_id, snapshot_date),
  KEY idx_inventory_store_date (store_id, snapshot_date),

  CONSTRAINT fk_inventory_store FOREIGN KEY (store_id) REFERENCES stores(store_id),
  CONSTRAINT fk_inventory_product FOREIGN KEY (product_id) REFERENCES products(product_id)
) ENGINE=InnoDB;

-- ======================================================
-- Tabelas de Texto (LLM-friendly)
-- ======================================================

CREATE TABLE IF NOT EXISTS complaints (
  complaint_id BIGINT PRIMARY KEY,
  customer_id BIGINT NULL,
  order_id BIGINT NULL,
  product_id BIGINT NOT NULL,
  complaint_datetime DATETIME NOT NULL,
  complaint_channel VARCHAR(40) NOT NULL,
  complaint_type VARCHAR(80) NOT NULL,
  complaint_text TEXT NOT NULL,
  severity VARCHAR(16) NOT NULL,
  status VARCHAR(24) NOT NULL,
  resolution_time_days INT NULL,

  KEY idx_complaints_product_time (product_id, complaint_datetime),
  KEY idx_complaints_status (status),
  KEY idx_complaints_type (complaint_type),

  CONSTRAINT fk_complaints_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
  CONSTRAINT fk_complaints_order FOREIGN KEY (order_id) REFERENCES orders(order_id),
  CONSTRAINT fk_complaints_product FOREIGN KEY (product_id) REFERENCES products(product_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS reviews (
  review_id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  product_id BIGINT NOT NULL,
  review_datetime DATETIME NOT NULL,
  rating TINYINT NOT NULL,
  review_title VARCHAR(255) NULL,
  review_text TEXT NOT NULL,

  KEY idx_reviews_product_time (product_id, review_datetime),
  KEY idx_reviews_rating (rating),

  CONSTRAINT fk_reviews_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
  CONSTRAINT fk_reviews_product FOREIGN KEY (product_id) REFERENCES products(product_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS returns (
  return_id BIGINT PRIMARY KEY,
  order_id BIGINT NOT NULL,
  product_id BIGINT NOT NULL,
  return_datetime DATETIME NOT NULL,
  quantity INT NOT NULL,
  return_reason VARCHAR(120) NOT NULL,
  refund_amount DECIMAL(12,2) NOT NULL,

  KEY idx_returns_product_time (product_id, return_datetime),
  KEY idx_returns_order (order_id),

  CONSTRAINT fk_returns_order FOREIGN KEY (order_id) REFERENCES orders(order_id),
  CONSTRAINT fk_returns_product FOREIGN KEY (product_id) REFERENCES products(product_id)
) ENGINE=InnoDB;

-- ======================================================
-- Embeddings (para etapa futura)
-- ======================================================

CREATE TABLE IF NOT EXISTS text_embeddings (
  doc_type VARCHAR(32) NOT NULL,
  doc_id BIGINT NOT NULL,
  model VARCHAR(64) NOT NULL,
  embedding_json JSON NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (doc_type, doc_id, model)
) ENGINE=InnoDB;
