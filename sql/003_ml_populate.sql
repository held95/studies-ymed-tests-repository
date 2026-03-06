-- ============================================================================
-- 003_ml_populate.sql — Popular tabelas ML_ (agregados + features)
-- ============================================================================
-- Execucao: apos 002_ml_tables.sql
-- Tabelas de OUTPUT (predictions) ficam vazias — serao preenchidas pelos modelos
-- ============================================================================

USE retail_analytics;

-- Limpar dados anteriores (idempotente)
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE ML_FACT_DAILY_SALES;
TRUNCATE TABLE ML_FACT_MONTHLY_KPI;
TRUNCATE TABLE ML_FACT_PRODUCT_QUALITY;
TRUNCATE TABLE ML_FEATURES_CHURN;
TRUNCATE TABLE ML_FEATURES_CLV;
TRUNCATE TABLE ML_FEATURES_RETURN_RISK;
TRUNCATE TABLE ML_FEATURES_PRODUCT_RISK;
TRUNCATE TABLE ML_FEATURES_SEGMENTATION;
SET FOREIGN_KEY_CHECKS = 1;

-- **************************************************************************
-- CAMADA 2 — AGREGADOS ANALITICOS
-- **************************************************************************

-- ML_FACT_DAILY_SALES
INSERT INTO ML_FACT_DAILY_SALES
    (sale_date, product_id, store_id, product_category, channel,
     qty_sold, revenue_gross, revenue_net, discount_total,
     orders_count, customers_unique, generated_at)
SELECT
    DATE(o.order_datetime)                              AS sale_date,
    oi.product_id,
    o.store_id,
    p.product_category,
    s.channel,
    SUM(oi.quantity)                                     AS qty_sold,
    ROUND(SUM(oi.unit_price * oi.quantity), 2)           AS revenue_gross,
    ROUND(SUM(oi.net_line_amount), 2)                    AS revenue_net,
    ROUND(SUM(oi.discount_amount), 2)                    AS discount_total,
    COUNT(DISTINCT o.order_id)                           AS orders_count,
    COUNT(DISTINCT o.customer_id)                        AS customers_unique,
    NOW()                                                AS generated_at
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p     ON oi.product_id = p.product_id
JOIN stores s       ON o.store_id = s.store_id
WHERE o.order_status NOT IN ('Cancelado')
GROUP BY DATE(o.order_datetime), oi.product_id, o.store_id,
         p.product_category, s.channel;

-- ML_FACT_MONTHLY_KPI
INSERT INTO ML_FACT_MONTHLY_KPI
    (ano_mes, total_pedidos, pedidos_validos, receita_bruta, receita_liquida,
     ticket_medio, desconto_total, frete_total,
     cancelamentos, pct_cancelamento, devolucoes, reclamacoes,
     clientes_ativos, novos_clientes, rating_medio, generated_at)
SELECT
    DATE_FORMAT(o.order_datetime, '%Y-%m')               AS ano_mes,
    COUNT(*)                                              AS total_pedidos,
    SUM(CASE WHEN o.order_status NOT IN ('Cancelado') THEN 1 ELSE 0 END)
                                                          AS pedidos_validos,
    ROUND(SUM(o.gross_amount), 2)                         AS receita_bruta,
    ROUND(SUM(CASE WHEN o.order_status NOT IN ('Cancelado') THEN o.net_amount ELSE 0 END), 2)
                                                          AS receita_liquida,
    ROUND(AVG(CASE WHEN o.order_status NOT IN ('Cancelado') THEN o.net_amount END), 2)
                                                          AS ticket_medio,
    ROUND(SUM(o.discount_amount), 2)                      AS desconto_total,
    ROUND(SUM(o.shipping_amount), 2)                      AS frete_total,
    SUM(CASE WHEN o.order_status = 'Cancelado' THEN 1 ELSE 0 END)
                                                          AS cancelamentos,
    ROUND(SUM(CASE WHEN o.order_status = 'Cancelado' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)
                                                          AS pct_cancelamento,
    COALESCE(dev.total_dev, 0)                            AS devolucoes,
    COALESCE(rec.total_rec, 0)                            AS reclamacoes,
    COUNT(DISTINCT o.customer_id)                         AS clientes_ativos,
    COALESCE(novos.qtd, 0)                                AS novos_clientes,
    rv.rating_medio                                       AS rating_medio,
    NOW()                                                 AS generated_at
FROM orders o
LEFT JOIN (
    SELECT DATE_FORMAT(return_datetime, '%Y-%m') AS mes, COUNT(*) AS total_dev
    FROM returns GROUP BY mes
) dev ON DATE_FORMAT(o.order_datetime, '%Y-%m') = dev.mes
LEFT JOIN (
    SELECT DATE_FORMAT(complaint_datetime, '%Y-%m') AS mes, COUNT(*) AS total_rec
    FROM complaints GROUP BY mes
) rec ON DATE_FORMAT(o.order_datetime, '%Y-%m') = rec.mes
LEFT JOIN (
    SELECT DATE_FORMAT(signup_date, '%Y-%m') AS mes, COUNT(*) AS qtd
    FROM customers GROUP BY mes
) novos ON DATE_FORMAT(o.order_datetime, '%Y-%m') = novos.mes
LEFT JOIN (
    SELECT DATE_FORMAT(review_datetime, '%Y-%m') AS mes, ROUND(AVG(rating), 2) AS rating_medio
    FROM reviews GROUP BY mes
) rv ON DATE_FORMAT(o.order_datetime, '%Y-%m') = rv.mes
GROUP BY DATE_FORMAT(o.order_datetime, '%Y-%m'),
         dev.total_dev, rec.total_rec, novos.qtd, rv.rating_medio
ORDER BY ano_mes;

-- =========================================================================
-- ML_FACT_PRODUCT_QUALITY
-- Qualidade cumulativa por produto (baseado em Q08 + Q09)
-- Um snapshot por produto com reference_date = data atual
-- =========================================================================
INSERT INTO ML_FACT_PRODUCT_QUALITY
    (reference_date, product_id, product_category,
     reviews_count, avg_rating, reviews_positive, reviews_negative, pct_negative,
     complaints_count, complaints_critical,
     returns_count, units_sold, return_rate_pct, generated_at)
SELECT
    CURDATE()                                            AS reference_date,
    p.product_id,
    p.product_category,
    COALESCE(rev.total_reviews, 0)                       AS reviews_count,
    rev.avg_rating,
    COALESCE(rev.reviews_positive, 0)                    AS reviews_positive,
    COALESCE(rev.reviews_negative, 0)                    AS reviews_negative,
    COALESCE(ROUND(rev.reviews_negative / NULLIF(rev.total_reviews, 0) * 100, 2), 0)
                                                         AS pct_negative,
    COALESCE(comp.total_reclamacoes, 0)                  AS complaints_count,
    COALESCE(comp.criticas, 0)                           AS complaints_critical,
    COALESCE(ret.total_devolucoes, 0)                    AS returns_count,
    COALESCE(vendas.unidades, 0)                         AS units_sold,
    COALESCE(ROUND(ret.total_devolucoes / NULLIF(vendas.unidades, 0) * 100, 2), 0)
                                                         AS return_rate_pct,
    NOW()                                                AS generated_at
FROM products p
LEFT JOIN (
    SELECT product_id,
           COUNT(*) AS total_reviews,
           ROUND(AVG(rating), 2) AS avg_rating,
           SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) AS reviews_positive,
           SUM(CASE WHEN rating <= 2 THEN 1 ELSE 0 END) AS reviews_negative
    FROM reviews GROUP BY product_id
) rev ON p.product_id = rev.product_id
LEFT JOIN (
    SELECT product_id,
           COUNT(*) AS total_reclamacoes,
           SUM(CASE WHEN severity = 'Critica' THEN 1 ELSE 0 END) AS criticas
    FROM complaints GROUP BY product_id
) comp ON p.product_id = comp.product_id
LEFT JOIN (
    SELECT product_id, COUNT(*) AS total_devolucoes
    FROM returns GROUP BY product_id
) ret ON p.product_id = ret.product_id
LEFT JOIN (
    SELECT product_id, SUM(quantity) AS unidades
    FROM order_items GROUP BY product_id
) vendas ON p.product_id = vendas.product_id
WHERE p.is_active = 1;

-- **************************************************************************
-- CAMADA 3 — ML FEATURES
-- **************************************************************************

-- ML_FEATURES_CHURN (baseado em Q23)
INSERT INTO ML_FEATURES_CHURN
    (customer_id, generated_at,
     gender, idade, state, income_range, loyalty_tier, dias_desde_signup,
     total_pedidos, valor_total, ticket_medio,
     dias_desde_ultima_compra, dias_entre_compras_medio, meses_ativos,
     qtd_canais_distintos, pct_online, pct_pedidos_com_cupom,
     total_reclamacoes, reclamacoes_graves, total_devolucoes, valor_devolvido,
     total_reviews, rating_medio, churned)
SELECT
    c.customer_id,
    NOW()                                                AS generated_at,
    c.gender,
    TIMESTAMPDIFF(YEAR, c.birth_date, CURDATE())         AS idade,
    c.state,
    c.income_range,
    c.loyalty_tier,
    DATEDIFF(CURDATE(), c.signup_date)                   AS dias_desde_signup,

    compras.total_pedidos,
    compras.valor_total,
    compras.ticket_medio,
    compras.dias_desde_ultima_compra,
    compras.dias_entre_compras_medio,
    compras.meses_ativos,
    compras.qtd_canais_distintos,
    compras.pct_online,
    compras.pct_pedidos_com_cupom,

    COALESCE(reclam.total_reclamacoes, 0),
    COALESCE(reclam.reclamacoes_graves, 0),
    COALESCE(devol.total_devolucoes, 0),
    COALESCE(devol.valor_devolvido, 0),

    COALESCE(rev.total_reviews, 0),
    COALESCE(rev.rating_medio, 0),

    CASE
        WHEN compras.dias_desde_ultima_compra > 90 THEN 1
        ELSE 0
    END                                                  AS churned

FROM customers c
JOIN (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id)                                            AS total_pedidos,
        ROUND(SUM(net_amount), 2)                                           AS valor_total,
        ROUND(AVG(net_amount), 2)                                           AS ticket_medio,
        DATEDIFF(CURDATE(), MAX(order_datetime))                            AS dias_desde_ultima_compra,
        ROUND(AVG(dias_entre), 0)                                           AS dias_entre_compras_medio,
        COUNT(DISTINCT DATE_FORMAT(order_datetime, '%Y-%m'))                AS meses_ativos,
        COUNT(DISTINCT store_id)                                            AS qtd_canais_distintos,
        ROUND(SUM(CASE WHEN shipping_type != 'Retirada em Loja' THEN 1 ELSE 0 END)
              / COUNT(*) * 100, 2)                                          AS pct_online,
        ROUND(SUM(CASE WHEN coupon_code IS NOT NULL THEN 1 ELSE 0 END)
              / COUNT(*) * 100, 2)                                          AS pct_pedidos_com_cupom
    FROM (
        SELECT o.*,
               DATEDIFF(o.order_datetime,
                        LAG(o.order_datetime) OVER (PARTITION BY o.customer_id ORDER BY o.order_datetime)
               ) AS dias_entre
        FROM orders o
        WHERE o.order_status NOT IN ('Cancelado')
    ) sub
    GROUP BY customer_id
) compras ON c.customer_id = compras.customer_id

LEFT JOIN (
    SELECT customer_id,
           COUNT(*) AS total_reclamacoes,
           SUM(CASE WHEN severity IN ('Alta', 'Critica') THEN 1 ELSE 0 END) AS reclamacoes_graves
    FROM complaints
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
) reclam ON c.customer_id = reclam.customer_id

LEFT JOIN (
    SELECT o.customer_id,
           COUNT(*) AS total_devolucoes,
           ROUND(SUM(ret.refund_amount), 2) AS valor_devolvido
    FROM returns ret
    JOIN orders o ON ret.order_id = o.order_id
    GROUP BY o.customer_id
) devol ON c.customer_id = devol.customer_id

LEFT JOIN (
    SELECT customer_id,
           COUNT(*) AS total_reviews,
           ROUND(AVG(rating), 2) AS rating_medio
    FROM reviews
    GROUP BY customer_id
) rev ON c.customer_id = rev.customer_id;

-- ML_FEATURES_CLV (baseado em Q26)
INSERT INTO ML_FEATURES_CLV
    (customer_id, generated_at,
     gender, idade, state, income_range, loyalty_tier,
     total_pedidos_periodo_base, valor_total_periodo_base, ticket_medio_periodo_base,
     qtd_categorias_distintas, qtd_meses_compra,
     valor_total_periodo_alvo)
SELECT
    c.customer_id,
    NOW()                                                AS generated_at,
    c.gender,
    TIMESTAMPDIFF(YEAR, c.birth_date, '2025-01-01')      AS idade,
    c.state,
    c.income_range,
    c.loyalty_tier,

    ano1.total_pedidos_ano1,
    ano1.valor_total_ano1,
    ano1.ticket_medio_ano1,
    ano1.qtd_categorias_distintas_ano1,
    ano1.qtd_meses_compra_ano1,

    COALESCE(ano2.valor_total_ano2, 0)                   AS valor_total_periodo_alvo

FROM customers c
JOIN (
    SELECT
        o.customer_id,
        COUNT(DISTINCT o.order_id)                         AS total_pedidos_ano1,
        ROUND(SUM(o.net_amount), 2)                        AS valor_total_ano1,
        ROUND(AVG(o.net_amount), 2)                        AS ticket_medio_ano1,
        COUNT(DISTINCT p.product_category)                 AS qtd_categorias_distintas_ano1,
        COUNT(DISTINCT DATE_FORMAT(o.order_datetime, '%Y-%m')) AS qtd_meses_compra_ano1
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    WHERE YEAR(o.order_datetime) = 2024
      AND o.order_status NOT IN ('Cancelado')
    GROUP BY o.customer_id
) ano1 ON c.customer_id = ano1.customer_id
LEFT JOIN (
    SELECT customer_id,
           ROUND(SUM(net_amount), 2) AS valor_total_ano2
    FROM orders
    WHERE YEAR(order_datetime) = 2025
      AND order_status NOT IN ('Cancelado')
    GROUP BY customer_id
) ano2 ON c.customer_id = ano2.customer_id;

-- ML_FEATURES_RETURN_RISK (baseado em Q27)
INSERT INTO ML_FEATURES_RETURN_RISK
    (order_id, generated_at,
     channel, payment_method, shipping_type, tem_cupom,
     gross_amount, discount_amount, net_amount,
     qtd_itens, qtd_categorias, preco_medio_item,
     dia_semana, hora, mes,
     compras_anteriores, devolucoes_anteriores, taxa_devolucao_historica,
     foi_devolvido)
SELECT
    o.order_id,
    NOW()                                                AS generated_at,
    s.channel,
    o.payment_method,
    o.shipping_type,
    CASE WHEN o.coupon_code IS NOT NULL THEN 1 ELSE 0 END AS tem_cupom,
    o.gross_amount,
    o.discount_amount,
    o.net_amount,
    itens.qtd_itens,
    itens.qtd_categorias,
    itens.preco_medio_item,

    DAYOFWEEK(o.order_datetime)                          AS dia_semana,
    HOUR(o.order_datetime)                               AS hora,
    MONTH(o.order_datetime)                              AS mes,

    COALESCE(hist.compras_anteriores, 0)                 AS compras_anteriores,
    COALESCE(hist.devolucoes_anteriores, 0)               AS devolucoes_anteriores,
    COALESCE(hist.taxa_devolucao_historica, 0)            AS taxa_devolucao_historica,

    CASE WHEN ret.order_id IS NOT NULL THEN 1 ELSE 0 END AS foi_devolvido

FROM orders o
JOIN stores s ON o.store_id = s.store_id

JOIN (
    SELECT
        oi.order_id,
        COUNT(*)                                         AS qtd_itens,
        COUNT(DISTINCT p.product_category)               AS qtd_categorias,
        ROUND(AVG(oi.unit_price), 2)                     AS preco_medio_item
    FROM order_items oi
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY oi.order_id
) itens ON o.order_id = itens.order_id

LEFT JOIN (
    SELECT
        o2.order_id,
        COUNT(DISTINCT o_prev.order_id)                  AS compras_anteriores,
        COUNT(DISTINCT r_prev.return_id)                 AS devolucoes_anteriores,
        CASE
            WHEN COUNT(DISTINCT o_prev.order_id) > 0
            THEN ROUND(COUNT(DISTINCT r_prev.return_id) / COUNT(DISTINCT o_prev.order_id) * 100, 2)
            ELSE 0
        END                                              AS taxa_devolucao_historica
    FROM orders o2
    LEFT JOIN orders o_prev ON o2.customer_id = o_prev.customer_id
                            AND o_prev.order_datetime < o2.order_datetime
    LEFT JOIN returns r_prev ON o_prev.order_id = r_prev.order_id
    GROUP BY o2.order_id
) hist ON o.order_id = hist.order_id

LEFT JOIN (
    SELECT DISTINCT order_id FROM returns
) ret ON o.order_id = ret.order_id

WHERE o.order_status NOT IN ('Cancelado');

-- ML_FEATURES_PRODUCT_RISK (baseado em Q28)
INSERT INTO ML_FEATURES_PRODUCT_RISK
    (product_id, generated_at,
     product_category, product_subcategory, target_audience,
     list_price, unit_cost, margem_pct, package_size_ml_g, dias_no_mercado,
     total_vendido, receita,
     total_devolucoes, taxa_devolucao, total_reclamacoes,
     total_reviews, rating_medio, pct_reviews_negativas,
     produto_problematico)
SELECT
    p.product_id,
    NOW()                                                AS generated_at,
    p.product_category,
    p.product_subcategory,
    p.target_audience,
    p.list_price,
    p.unit_cost,
    ROUND((p.list_price - p.unit_cost) / p.list_price * 100, 2) AS margem_pct,
    p.package_size_ml_g,
    DATEDIFF(CURDATE(), p.launch_date)                   AS dias_no_mercado,

    COALESCE(vendas.unidades, 0)                         AS total_vendido,
    COALESCE(vendas.receita, 0)                          AS receita,

    COALESCE(devol.total_devolucoes, 0)                  AS total_devolucoes,
    COALESCE(ROUND(devol.total_devolucoes / NULLIF(vendas.unidades, 0) * 100, 2), 0)
                                                         AS taxa_devolucao,
    COALESCE(reclam.total_reclamacoes, 0)                AS total_reclamacoes,

    COALESCE(rev.total_reviews, 0)                       AS total_reviews,
    COALESCE(rev.rating_medio, 0)                        AS rating_medio,
    COALESCE(rev.pct_negativas, 0)                       AS pct_reviews_negativas,

    CASE WHEN COALESCE(rev.rating_medio, 5) <= 3 THEN 1 ELSE 0 END
                                                         AS produto_problematico

FROM products p
LEFT JOIN (
    SELECT product_id, SUM(quantity) AS unidades, ROUND(SUM(net_line_amount), 2) AS receita
    FROM order_items GROUP BY product_id
) vendas ON p.product_id = vendas.product_id
LEFT JOIN (
    SELECT product_id, COUNT(*) AS total_devolucoes
    FROM returns GROUP BY product_id
) devol ON p.product_id = devol.product_id
LEFT JOIN (
    SELECT product_id, COUNT(*) AS total_reclamacoes
    FROM complaints GROUP BY product_id
) reclam ON p.product_id = reclam.product_id
LEFT JOIN (
    SELECT product_id,
           COUNT(*) AS total_reviews,
           ROUND(AVG(rating), 2) AS rating_medio,
           ROUND(SUM(CASE WHEN rating <= 2 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS pct_negativas
    FROM reviews GROUP BY product_id
) rev ON p.product_id = rev.product_id
WHERE p.is_active = 1;

-- ML_FEATURES_SEGMENTATION (baseado em Q29)
INSERT INTO ML_FEATURES_SEGMENTATION
    (customer_id, generated_at,
     gender, state, income_range, loyalty_tier,
     recency_dias, frequency, monetary, ticket_medio,
     canais_usados, pct_gasto_online, categoria_favorita, categorias_compradas,
     total_reviews, rating_medio_dado, total_reclamacoes, total_devolucoes)
SELECT
    c.customer_id,
    NOW()                                                AS generated_at,
    c.gender,
    c.state,
    c.income_range,
    c.loyalty_tier,

    DATEDIFF(CURDATE(), MAX(o.order_datetime))           AS recency_dias,
    COUNT(DISTINCT o.order_id)                           AS frequency,
    ROUND(SUM(o.net_amount), 2)                          AS monetary,
    ROUND(AVG(o.net_amount), 2)                          AS ticket_medio,

    COUNT(DISTINCT s.channel)                            AS canais_usados,
    ROUND(SUM(CASE WHEN s.channel IN ('E-commerce Proprio', 'Marketplace')
              THEN o.net_amount ELSE 0 END)
          / SUM(o.net_amount) * 100, 2)                  AS pct_gasto_online,

    cat_fav.categoria_favorita,
    cat_fav.categorias_compradas,

    COALESCE(rev_agg.total_reviews, 0)                   AS total_reviews,
    COALESCE(rev_agg.rating_medio, 0)                    AS rating_medio_dado,
    COALESCE(comp_agg.total_reclamacoes, 0)              AS total_reclamacoes,
    COALESCE(ret_agg.total_devolucoes, 0)                AS total_devolucoes

FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN stores s ON o.store_id = s.store_id
LEFT JOIN (
    -- Categoria favorita e total de categorias por cliente
    SELECT
        customer_id,
        MAX(CASE WHEN rk = 1 THEN product_category END) AS categoria_favorita,
        COUNT(DISTINCT product_category)                  AS categorias_compradas
    FROM (
        SELECT
            o2.customer_id,
            p2.product_category,
            ROW_NUMBER() OVER (PARTITION BY o2.customer_id
                               ORDER BY SUM(oi2.net_line_amount) DESC) AS rk
        FROM order_items oi2
        JOIN orders o2 ON oi2.order_id = o2.order_id
        JOIN products p2 ON oi2.product_id = p2.product_id
        WHERE o2.order_status NOT IN ('Cancelado')
        GROUP BY o2.customer_id, p2.product_category
    ) ranked
    GROUP BY customer_id
) cat_fav ON c.customer_id = cat_fav.customer_id
LEFT JOIN (
    SELECT customer_id, COUNT(*) AS total_reviews, ROUND(AVG(rating), 2) AS rating_medio
    FROM reviews GROUP BY customer_id
) rev_agg ON c.customer_id = rev_agg.customer_id
LEFT JOIN (
    SELECT customer_id, COUNT(*) AS total_reclamacoes
    FROM complaints WHERE customer_id IS NOT NULL GROUP BY customer_id
) comp_agg ON c.customer_id = comp_agg.customer_id
LEFT JOIN (
    SELECT o3.customer_id, COUNT(*) AS total_devolucoes
    FROM returns r3 JOIN orders o3 ON r3.order_id = o3.order_id
    GROUP BY o3.customer_id
) ret_agg ON c.customer_id = ret_agg.customer_id
WHERE o.order_status NOT IN ('Cancelado')
GROUP BY c.customer_id, c.gender, c.state, c.income_range, c.loyalty_tier,
         cat_fav.categoria_favorita, cat_fav.categorias_compradas,
         rev_agg.total_reviews, rev_agg.rating_medio,
         comp_agg.total_reclamacoes, ret_agg.total_devolucoes;

-- **************************************************************************
-- VERIFICACAO DE ROW COUNTS
-- **************************************************************************
SELECT 'ML_FACT_DAILY_SALES'     AS tabela, COUNT(*) AS rows_inserted FROM ML_FACT_DAILY_SALES
UNION ALL
SELECT 'ML_FACT_MONTHLY_KPI',     COUNT(*) FROM ML_FACT_MONTHLY_KPI
UNION ALL
SELECT 'ML_FACT_PRODUCT_QUALITY', COUNT(*) FROM ML_FACT_PRODUCT_QUALITY
UNION ALL
SELECT 'ML_FEATURES_CHURN',       COUNT(*) FROM ML_FEATURES_CHURN
UNION ALL
SELECT 'ML_FEATURES_CLV',         COUNT(*) FROM ML_FEATURES_CLV
UNION ALL
SELECT 'ML_FEATURES_RETURN_RISK', COUNT(*) FROM ML_FEATURES_RETURN_RISK
UNION ALL
SELECT 'ML_FEATURES_PRODUCT_RISK',COUNT(*) FROM ML_FEATURES_PRODUCT_RISK
UNION ALL
SELECT 'ML_FEATURES_SEGMENTATION',COUNT(*) FROM ML_FEATURES_SEGMENTATION
UNION ALL
SELECT '--- TABELAS OUTPUT (vazias) ---', 0
UNION ALL
SELECT 'ML_CHURN_PREDICTIONS',     COUNT(*) FROM ML_CHURN_PREDICTIONS
UNION ALL
SELECT 'ML_SALES_FORECAST',        COUNT(*) FROM ML_SALES_FORECAST
UNION ALL
SELECT 'ML_CLV_PREDICTIONS',       COUNT(*) FROM ML_CLV_PREDICTIONS
UNION ALL
SELECT 'ML_RETURN_PREDICTIONS',    COUNT(*) FROM ML_RETURN_PREDICTIONS
UNION ALL
SELECT 'ML_PRODUCT_RISK_SCORES',   COUNT(*) FROM ML_PRODUCT_RISK_SCORES
UNION ALL
SELECT 'ML_CUSTOMER_SEGMENTS',     COUNT(*) FROM ML_CUSTOMER_SEGMENTS
UNION ALL
SELECT 'ML_MARKET_BASKET',         COUNT(*) FROM ML_MARKET_BASKET
UNION ALL
SELECT 'ML_STOCK_RUPTURE_FORECAST',COUNT(*) FROM ML_STOCK_RUPTURE_FORECAST
UNION ALL
SELECT 'ML_PRODUCT_TEXT_INSIGHTS', COUNT(*) FROM ML_PRODUCT_TEXT_INSIGHTS;
