-- 002_supabase_functions.sql
-- Run this in Supabase SQL Editor AFTER seeding data.
-- These functions are called by the Next.js API routes via supabase.rpc().

-- ======================================================
-- KPI Summary (8 metrics for the cards row)
-- ======================================================
CREATE OR REPLACE FUNCTION kpi_summary()
RETURNS JSON LANGUAGE sql STABLE AS $$
  SELECT json_build_object(
    'clientes_ativos',     (SELECT COUNT(DISTINCT customer_id) FROM orders),
    'total_pedidos',       (SELECT COUNT(*) FROM orders WHERE order_status <> 'Cancelado'),
    'receita_total',       (SELECT ROUND(SUM(net_amount)::numeric, 2) FROM orders WHERE order_status <> 'Cancelado'),
    'ticket_medio',        (SELECT ROUND(AVG(net_amount)::numeric, 2) FROM orders WHERE order_status <> 'Cancelado'),
    'total_devolucoes',    (SELECT COUNT(*) FROM returns),
    'rating_medio',        (SELECT ROUND(AVG(rating)::numeric, 2) FROM reviews),
    'reclamacoes_abertas', (SELECT COUNT(*) FROM complaints WHERE status = 'Aberto'),
    'produtos_ativos',     (SELECT COUNT(*) FROM products WHERE is_active = 1)
  );
$$;

-- ======================================================
-- Top 5 produtos por receita líquida
-- ======================================================
CREATE OR REPLACE FUNCTION kpi_top_products()
RETURNS TABLE(
  product_name     TEXT,
  product_category TEXT,
  receita_liquida  NUMERIC,
  unidades_vendidas BIGINT
) LANGUAGE sql STABLE AS $$
  SELECT
    p.product_name,
    p.product_category,
    ROUND(SUM(oi.net_line_amount)::numeric, 2) AS receita_liquida,
    SUM(oi.quantity)::BIGINT                   AS unidades_vendidas
  FROM order_items oi
  JOIN products p ON oi.product_id = p.product_id
  GROUP BY p.product_name, p.product_category
  ORDER BY receita_liquida DESC
  LIMIT 5;
$$;

-- ======================================================
-- Receita por canal de venda
-- ======================================================
CREATE OR REPLACE FUNCTION kpi_revenue_by_channel()
RETURNS TABLE(
  channel         TEXT,
  total_pedidos   BIGINT,
  receita_liquida NUMERIC
) LANGUAGE sql STABLE AS $$
  SELECT
    s.channel,
    COUNT(DISTINCT o.order_id)::BIGINT        AS total_pedidos,
    ROUND(SUM(o.net_amount)::numeric, 2)       AS receita_liquida
  FROM orders o
  JOIN stores s ON o.store_id = s.store_id
  GROUP BY s.channel
  ORDER BY receita_liquida DESC;
$$;

-- ======================================================
-- Receita mensal (Jan/2024 – Dez/2025)
-- ======================================================
CREATE OR REPLACE FUNCTION kpi_monthly_revenue()
RETURNS TABLE(
  ano_mes         TEXT,
  total_pedidos   BIGINT,
  receita_liquida NUMERIC,
  ticket_medio    NUMERIC
) LANGUAGE sql STABLE AS $$
  SELECT
    TO_CHAR(order_datetime, 'YYYY-MM')         AS ano_mes,
    COUNT(*)::BIGINT                            AS total_pedidos,
    ROUND(SUM(net_amount)::numeric, 2)          AS receita_liquida,
    ROUND(AVG(net_amount)::numeric, 2)          AS ticket_medio
  FROM orders
  GROUP BY TO_CHAR(order_datetime, 'YYYY-MM')
  ORDER BY ano_mes
  LIMIT 24;
$$;

-- ======================================================
-- 10 pedidos mais recentes
-- ======================================================
CREATE OR REPLACE FUNCTION kpi_recent_orders()
RETURNS TABLE(
  order_id       BIGINT,
  customer_name  TEXT,
  order_datetime TIMESTAMP,
  net_amount     NUMERIC,
  order_status   TEXT,
  channel        TEXT
) LANGUAGE sql STABLE AS $$
  SELECT
    o.order_id,
    c.customer_name,
    o.order_datetime,
    o.net_amount,
    o.order_status,
    s.channel
  FROM orders o
  JOIN customers c ON o.customer_id = c.customer_id
  JOIN stores s    ON o.store_id    = s.store_id
  ORDER BY o.order_datetime DESC
  LIMIT 10;
$$;
