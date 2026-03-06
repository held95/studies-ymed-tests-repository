-- ============================================================================
-- QUERIES ANALITICAS + ML — YMED RETAIL ANALYTICS
-- ============================================================================
-- Banco: retail_analytics (MySQL 8.0)
-- Periodo: Jan/2024 - Dez/2025
-- Organizacao:
--   PARTE 1: Insights de Negocio (dashboard)
--   PARTE 2: Datasets para Modelos de Machine Learning
-- ============================================================================

USE retail_analytics;

-- **************************************************************************
-- PARTE 1 — INSIGHTS DE NEGOCIO
-- **************************************************************************

-- ==========================================================================
-- 1.1  VISAO GERAL DE PERFORMANCE
-- ==========================================================================

-- [Q01] KPIs mensais: receita, pedidos, ticket medio, desconto medio
SELECT
    DATE_FORMAT(order_datetime, '%Y-%m')                  AS ano_mes,
    COUNT(*)                                               AS total_pedidos,
    ROUND(SUM(net_amount), 2)                              AS receita_liquida,
    ROUND(AVG(net_amount), 2)                              AS ticket_medio,
    ROUND(SUM(discount_amount), 2)                         AS desconto_total,
    ROUND(AVG(discount_amount), 2)                         AS desconto_medio,
    ROUND(SUM(shipping_amount), 2)                         AS frete_total,
    SUM(CASE WHEN order_status = 'Cancelado' THEN 1 ELSE 0 END) AS cancelamentos,
    ROUND(SUM(CASE WHEN order_status = 'Cancelado' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS pct_cancelamento
FROM orders
GROUP BY DATE_FORMAT(order_datetime, '%Y-%m')
ORDER BY ano_mes;

-- [Q02] Receita por canal de venda
SELECT
    s.channel,
    COUNT(DISTINCT o.order_id)                             AS total_pedidos,
    ROUND(SUM(o.net_amount), 2)                            AS receita_liquida,
    ROUND(AVG(o.net_amount), 2)                            AS ticket_medio,
    ROUND(SUM(o.net_amount) / (SELECT SUM(net_amount) FROM orders) * 100, 2) AS pct_receita
FROM orders o
JOIN stores s ON o.store_id = s.store_id
GROUP BY s.channel
ORDER BY receita_liquida DESC;

-- [Q03] Top 15 produtos por receita liquida
SELECT
    p.product_id,
    p.sku,
    p.product_name,
    p.product_category,
    p.product_subcategory,
    COUNT(DISTINCT oi.order_id)                            AS pedidos_distintos,
    SUM(oi.quantity)                                        AS unidades_vendidas,
    ROUND(SUM(oi.net_line_amount), 2)                      AS receita_liquida,
    ROUND(AVG(oi.unit_price), 2)                           AS preco_medio_praticado,
    p.list_price,
    p.unit_cost,
    ROUND((p.list_price - p.unit_cost) / p.list_price * 100, 2) AS margem_bruta_pct
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY p.product_id
ORDER BY receita_liquida DESC
LIMIT 15;

-- [Q04] Top 15 clientes por lifetime value (valor total gasto)
SELECT
    c.customer_id,
    c.customer_name,
    c.gender,
    c.city,
    c.state,
    c.loyalty_tier,
    c.income_range,
    COUNT(DISTINCT o.order_id)                             AS total_pedidos,
    ROUND(SUM(o.net_amount), 2)                            AS lifetime_value,
    ROUND(AVG(o.net_amount), 2)                            AS ticket_medio,
    MIN(o.order_datetime)                                  AS primeira_compra,
    MAX(o.order_datetime)                                  AS ultima_compra,
    DATEDIFF(MAX(o.order_datetime), MIN(o.order_datetime)) AS dias_como_cliente
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_status NOT IN ('Cancelado')
GROUP BY c.customer_id
ORDER BY lifetime_value DESC
LIMIT 15;

-- [Q05] Distribuicao por metodo de pagamento
SELECT
    payment_method,
    COUNT(*)                                               AS total_pedidos,
    ROUND(COUNT(*) / (SELECT COUNT(*) FROM orders) * 100, 2) AS pct_pedidos,
    ROUND(SUM(net_amount), 2)                              AS receita,
    ROUND(AVG(net_amount), 2)                              AS ticket_medio
FROM orders
GROUP BY payment_method
ORDER BY total_pedidos DESC;

-- ==========================================================================
-- 1.2  ANALISE DE PRODUTOS
-- ==========================================================================

-- [Q06] Margem bruta por categoria e subcategoria
SELECT
    p.product_category,
    p.product_subcategory,
    COUNT(DISTINCT p.product_id)                           AS qtd_skus,
    ROUND(AVG(p.list_price), 2)                            AS preco_medio,
    ROUND(AVG(p.unit_cost), 2)                             AS custo_medio,
    ROUND(AVG((p.list_price - p.unit_cost) / p.list_price) * 100, 2) AS margem_bruta_media_pct,
    SUM(oi.quantity)                                        AS total_vendido,
    ROUND(SUM(oi.net_line_amount), 2)                      AS receita_total
FROM products p
LEFT JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_category, p.product_subcategory
ORDER BY p.product_category, receita_total DESC;

-- [Q07] Produtos com MAIOR taxa de devolucao
SELECT
    p.product_id,
    p.product_name,
    p.product_category,
    total_vendas.unidades_vendidas,
    COALESCE(total_devolucoes.unidades_devolvidas, 0)      AS unidades_devolvidas,
    ROUND(COALESCE(total_devolucoes.unidades_devolvidas, 0) /
          total_vendas.unidades_vendidas * 100, 2)         AS taxa_devolucao_pct,
    COALESCE(total_devolucoes.valor_devolvido, 0)          AS valor_devolvido
FROM products p
JOIN (
    SELECT product_id, SUM(quantity) AS unidades_vendidas
    FROM order_items
    GROUP BY product_id
) total_vendas ON p.product_id = total_vendas.product_id
LEFT JOIN (
    SELECT product_id, SUM(quantity) AS unidades_devolvidas, SUM(refund_amount) AS valor_devolvido
    FROM returns
    GROUP BY product_id
) total_devolucoes ON p.product_id = total_devolucoes.product_id
WHERE total_vendas.unidades_vendidas > 100
ORDER BY taxa_devolucao_pct DESC
LIMIT 20;

-- [Q08] Rating medio por produto (com volume de reviews)
SELECT
    p.product_id,
    p.product_name,
    p.product_category,
    COUNT(r.review_id)                                     AS total_reviews,
    ROUND(AVG(r.rating), 2)                                AS rating_medio,
    SUM(CASE WHEN r.rating <= 2 THEN 1 ELSE 0 END)        AS reviews_negativas,
    SUM(CASE WHEN r.rating >= 4 THEN 1 ELSE 0 END)        AS reviews_positivas,
    ROUND(SUM(CASE WHEN r.rating <= 2 THEN 1 ELSE 0 END) /
          COUNT(*) * 100, 2)                               AS pct_negativas
FROM products p
JOIN reviews r ON p.product_id = r.product_id
GROUP BY p.product_id
HAVING total_reviews >= 10
ORDER BY rating_medio ASC
LIMIT 20;

-- [Q09] Produtos com mais reclamacoes vs receita gerada
SELECT
    p.product_id,
    p.product_name,
    p.product_category,
    COALESCE(vendas.receita, 0)                            AS receita,
    COALESCE(reclam.total_reclamacoes, 0)                  AS total_reclamacoes,
    COALESCE(reclam.criticas, 0)                           AS reclamacoes_criticas,
    COALESCE(devol.total_devolucoes, 0)                    AS total_devolucoes,
    COALESCE(rev.rating_medio, 0)                          AS rating_medio
FROM products p
LEFT JOIN (
    SELECT product_id, ROUND(SUM(net_line_amount), 2) AS receita
    FROM order_items GROUP BY product_id
) vendas ON p.product_id = vendas.product_id
LEFT JOIN (
    SELECT product_id,
           COUNT(*) AS total_reclamacoes,
           SUM(CASE WHEN severity = 'Critica' THEN 1 ELSE 0 END) AS criticas
    FROM complaints GROUP BY product_id
) reclam ON p.product_id = reclam.product_id
LEFT JOIN (
    SELECT product_id, COUNT(*) AS total_devolucoes
    FROM returns GROUP BY product_id
) devol ON p.product_id = devol.product_id
LEFT JOIN (
    SELECT product_id, ROUND(AVG(rating), 2) AS rating_medio
    FROM reviews GROUP BY product_id
) rev ON p.product_id = rev.product_id
ORDER BY total_reclamacoes DESC
LIMIT 30;

-- ==========================================================================
-- 1.3  ANALISE DE CLIENTES
-- ==========================================================================

-- [Q10] Segmentacao RFM (Recency, Frequency, Monetary)
-- Cada cliente recebe score 1-5 em R, F e M (5 = melhor)
SELECT
    customer_id,
    customer_name,
    loyalty_tier,
    dias_desde_ultima_compra,
    total_pedidos,
    valor_total,
    NTILE(5) OVER (ORDER BY dias_desde_ultima_compra DESC) AS R_score,
    NTILE(5) OVER (ORDER BY total_pedidos ASC)             AS F_score,
    NTILE(5) OVER (ORDER BY valor_total ASC)               AS M_score
FROM (
    SELECT
        c.customer_id,
        c.customer_name,
        c.loyalty_tier,
        DATEDIFF('2026-01-01', MAX(o.order_datetime))      AS dias_desde_ultima_compra,
        COUNT(DISTINCT o.order_id)                         AS total_pedidos,
        ROUND(SUM(o.net_amount), 2)                        AS valor_total
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.order_status NOT IN ('Cancelado')
    GROUP BY c.customer_id, c.customer_name, c.loyalty_tier
) base
ORDER BY valor_total DESC;

-- [Q11] Ticket medio por genero, regiao e faixa etaria
SELECT
    c.gender,
    c.state,
    CASE
        WHEN TIMESTAMPDIFF(YEAR, c.birth_date, CURDATE()) < 25 THEN '18-24'
        WHEN TIMESTAMPDIFF(YEAR, c.birth_date, CURDATE()) < 35 THEN '25-34'
        WHEN TIMESTAMPDIFF(YEAR, c.birth_date, CURDATE()) < 45 THEN '35-44'
        WHEN TIMESTAMPDIFF(YEAR, c.birth_date, CURDATE()) < 55 THEN '45-54'
        ELSE '55+'
    END                                                    AS faixa_etaria,
    COUNT(DISTINCT o.order_id)                             AS total_pedidos,
    ROUND(AVG(o.net_amount), 2)                            AS ticket_medio,
    ROUND(SUM(o.net_amount), 2)                            AS receita_total
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_status NOT IN ('Cancelado')
GROUP BY c.gender, c.state, faixa_etaria
ORDER BY receita_total DESC;

-- [Q12] Cohort Analysis: retencao mensal por mes de signup
SELECT
    cohort_mes,
    mes_atividade,
    TIMESTAMPDIFF(MONTH, STR_TO_DATE(CONCAT(cohort_mes, '-01'), '%Y-%m-%d'),
                         STR_TO_DATE(CONCAT(mes_atividade, '-01'), '%Y-%m-%d')) AS meses_apos_signup,
    total_clientes_ativos,
    ROUND(total_clientes_ativos / primeiro_mes.tamanho_cohort * 100, 2) AS pct_retencao
FROM (
    SELECT
        DATE_FORMAT(c.signup_date, '%Y-%m')                AS cohort_mes,
        DATE_FORMAT(o.order_datetime, '%Y-%m')             AS mes_atividade,
        COUNT(DISTINCT o.customer_id)                      AS total_clientes_ativos
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.order_status NOT IN ('Cancelado')
    GROUP BY cohort_mes, mes_atividade
) atividade
JOIN (
    SELECT
        DATE_FORMAT(signup_date, '%Y-%m')                  AS cohort_mes,
        COUNT(DISTINCT customer_id)                        AS tamanho_cohort
    FROM customers
    GROUP BY DATE_FORMAT(signup_date, '%Y-%m')
) primeiro_mes ON atividade.cohort_mes = primeiro_mes.cohort_mes
ORDER BY cohort_mes, mes_atividade;

-- [Q13] Concentracao geografica de receita (Pareto por cidade)
SELECT
    c.city,
    c.state,
    COUNT(DISTINCT c.customer_id)                          AS total_clientes,
    COUNT(DISTINCT o.order_id)                             AS total_pedidos,
    ROUND(SUM(o.net_amount), 2)                            AS receita,
    ROUND(SUM(o.net_amount) / (SELECT SUM(net_amount) FROM orders WHERE order_status NOT IN ('Cancelado')) * 100, 2) AS pct_receita,
    ROUND(SUM(SUM(o.net_amount)) OVER (ORDER BY SUM(o.net_amount) DESC) /
          (SELECT SUM(net_amount) FROM orders WHERE order_status NOT IN ('Cancelado')) * 100, 2) AS pct_acumulado
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_status NOT IN ('Cancelado')
GROUP BY c.city, c.state
ORDER BY receita DESC;

-- ==========================================================================
-- 1.4  ANALISE DE CANAIS
-- ==========================================================================

-- [Q14] Evolucao mensal de receita por canal
SELECT
    DATE_FORMAT(o.order_datetime, '%Y-%m')                 AS ano_mes,
    s.channel,
    COUNT(DISTINCT o.order_id)                             AS pedidos,
    ROUND(SUM(o.net_amount), 2)                            AS receita
FROM orders o
JOIN stores s ON o.store_id = s.store_id
GROUP BY ano_mes, s.channel
ORDER BY ano_mes, s.channel;

-- [Q15] Taxa de devolucao por canal
SELECT
    s.channel,
    COUNT(DISTINCT o.order_id)                             AS total_pedidos,
    COUNT(DISTINCT ret.return_id)                          AS total_devolucoes,
    ROUND(COUNT(DISTINCT ret.return_id) / COUNT(DISTINCT o.order_id) * 100, 2) AS taxa_devolucao_pct,
    ROUND(COALESCE(SUM(ret.refund_amount), 0), 2)          AS valor_devolvido
FROM orders o
JOIN stores s ON o.store_id = s.store_id
LEFT JOIN returns ret ON o.order_id = ret.order_id
GROUP BY s.channel
ORDER BY taxa_devolucao_pct DESC;

-- [Q16] Custo medio de frete por tipo de envio e canal
SELECT
    s.channel,
    o.shipping_type,
    COUNT(*)                                               AS total_pedidos,
    ROUND(AVG(o.shipping_amount), 2)                       AS frete_medio,
    ROUND(AVG(o.net_amount), 2)                            AS ticket_medio,
    ROUND(AVG(o.shipping_amount) / AVG(o.net_amount) * 100, 2) AS frete_pct_ticket
FROM orders o
JOIN stores s ON o.store_id = s.store_id
GROUP BY s.channel, o.shipping_type
ORDER BY s.channel, frete_medio DESC;

-- ==========================================================================
-- 1.5  ANALISE DE SENTIMENTO E RECLAMACOES
-- ==========================================================================

-- [Q17] Distribuicao de ratings por categoria
SELECT
    p.product_category,
    r.rating,
    COUNT(*)                                               AS total_reviews,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY p.product_category) * 100, 2) AS pct_na_categoria
FROM reviews r
JOIN products p ON r.product_id = p.product_id
GROUP BY p.product_category, r.rating
ORDER BY p.product_category, r.rating DESC;

-- [Q18] Reclamacoes por tipo e severidade
SELECT
    complaint_type,
    severity,
    COUNT(*)                                               AS total,
    ROUND(AVG(resolution_time_days), 1)                    AS tempo_resolucao_medio_dias
FROM complaints
GROUP BY complaint_type, severity
ORDER BY complaint_type, FIELD(severity, 'Critica', 'Alta', 'Media', 'Baixa');

-- [Q19] Evolucao mensal de reclamacoes e NPS proxy
SELECT
    DATE_FORMAT(complaint_datetime, '%Y-%m')               AS ano_mes,
    COUNT(*)                                               AS total_reclamacoes,
    SUM(CASE WHEN severity IN ('Alta', 'Critica') THEN 1 ELSE 0 END) AS reclamacoes_graves,
    SUM(CASE WHEN status IN ('Resolvido', 'Fechado') THEN 1 ELSE 0 END) AS resolvidas,
    ROUND(SUM(CASE WHEN status IN ('Resolvido', 'Fechado') THEN 1 ELSE 0 END) /
          COUNT(*) * 100, 2)                               AS pct_resolucao,
    ROUND(AVG(resolution_time_days), 1)                    AS tempo_medio_resolucao
FROM complaints
GROUP BY ano_mes
ORDER BY ano_mes;

-- [Q20] Canal de reclamacao mais utilizado por tipo de problema
SELECT
    complaint_type,
    complaint_channel,
    COUNT(*)                                               AS total,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY complaint_type) * 100, 1) AS pct_do_tipo
FROM complaints
GROUP BY complaint_type, complaint_channel
ORDER BY complaint_type, total DESC;

-- ==========================================================================
-- 1.6  ANALISE DE ESTOQUE
-- ==========================================================================

-- [Q21] Produtos com maior frequencia de ruptura (stock = 0)
SELECT
    p.product_id,
    p.product_name,
    p.product_category,
    COUNT(*)                                               AS total_snapshots,
    SUM(CASE WHEN inv.stock_on_hand = 0 THEN 1 ELSE 0 END) AS vezes_sem_estoque,
    ROUND(SUM(CASE WHEN inv.stock_on_hand = 0 THEN 1 ELSE 0 END) /
          COUNT(*) * 100, 2)                               AS pct_ruptura,
    ROUND(AVG(inv.stock_on_hand), 0)                       AS estoque_medio
FROM inventory_snapshots inv
JOIN products p ON inv.product_id = p.product_id
GROUP BY p.product_id
HAVING total_snapshots >= 20
ORDER BY pct_ruptura DESC
LIMIT 20;

-- [Q22] Cobertura de estoque em dias (estoque / velocidade de venda)
SELECT
    p.product_id,
    p.product_name,
    p.product_category,
    ultimo_estoque.stock_on_hand                           AS estoque_atual,
    ultimo_estoque.reorder_point,
    ROUND(vendas_recentes.vendas_diarias, 2)               AS vendas_dia_medio,
    CASE
        WHEN vendas_recentes.vendas_diarias > 0
        THEN ROUND(ultimo_estoque.stock_on_hand / vendas_recentes.vendas_diarias, 0)
        ELSE 9999
    END                                                    AS dias_cobertura,
    CASE
        WHEN vendas_recentes.vendas_diarias > 0
             AND ultimo_estoque.stock_on_hand / vendas_recentes.vendas_diarias < 7
        THEN 'CRITICO'
        WHEN vendas_recentes.vendas_diarias > 0
             AND ultimo_estoque.stock_on_hand / vendas_recentes.vendas_diarias < 14
        THEN 'BAIXO'
        WHEN vendas_recentes.vendas_diarias > 0
             AND ultimo_estoque.stock_on_hand / vendas_recentes.vendas_diarias > 90
        THEN 'EXCESSO'
        ELSE 'OK'
    END                                                    AS status_estoque
FROM products p
JOIN (
    -- Ultimo snapshot disponivel por produto (media de todas as lojas)
    SELECT product_id,
           ROUND(AVG(stock_on_hand), 0) AS stock_on_hand,
           ROUND(AVG(reorder_point), 0) AS reorder_point
    FROM inventory_snapshots
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM inventory_snapshots)
    GROUP BY product_id
) ultimo_estoque ON p.product_id = ultimo_estoque.product_id
JOIN (
    -- Media de vendas diarias nos ultimos 90 dias
    SELECT oi.product_id,
           SUM(oi.quantity) / 90.0 AS vendas_diarias
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    WHERE o.order_datetime >= DATE_SUB((SELECT MAX(order_datetime) FROM orders), INTERVAL 90 DAY)
      AND o.order_status NOT IN ('Cancelado')
    GROUP BY oi.product_id
) vendas_recentes ON p.product_id = vendas_recentes.product_id
WHERE p.is_active = 1
ORDER BY dias_cobertura ASC;

-- **************************************************************************
-- PARTE 2 — DATASETS PARA MODELOS DE MACHINE LEARNING
-- **************************************************************************

-- ==========================================================================
-- 2.1  PREVISAO DE CHURN (Classificacao)
-- ==========================================================================

-- [Q23] Dataset de features para modelo de Churn
SELECT
    c.customer_id,
    -- Demographics
    c.gender,
    TIMESTAMPDIFF(YEAR, c.birth_date, '2026-01-01')        AS idade,
    c.state,
    c.income_range,
    c.loyalty_tier,
    DATEDIFF('2026-01-01', c.signup_date)                  AS dias_desde_signup,

    -- Comportamento de compra
    compras.total_pedidos,
    compras.valor_total,
    compras.ticket_medio,
    compras.dias_desde_ultima_compra,
    compras.dias_entre_compras_medio,
    compras.meses_ativos,
    compras.qtd_canais_distintos,
    compras.pct_online,

    -- Cupons
    compras.pct_pedidos_com_cupom,

    -- Problemas
    COALESCE(reclam.total_reclamacoes, 0)                  AS total_reclamacoes,
    COALESCE(reclam.reclamacoes_graves, 0)                 AS reclamacoes_graves,
    COALESCE(devol.total_devolucoes, 0)                    AS total_devolucoes,
    COALESCE(devol.valor_devolvido, 0)                     AS valor_devolvido,

    -- Reviews
    COALESCE(rev.total_reviews, 0)                         AS total_reviews,
    COALESCE(rev.rating_medio, 0)                          AS rating_medio,

    -- TARGET: churned = 1 se nao comprou nos ultimos 90 dias
    CASE
        WHEN compras.dias_desde_ultima_compra > 90 THEN 1
        ELSE 0
    END                                                    AS churned

FROM customers c
JOIN (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id)                           AS total_pedidos,
        ROUND(SUM(net_amount), 2)                          AS valor_total,
        ROUND(AVG(net_amount), 2)                          AS ticket_medio,
        DATEDIFF('2026-01-01', MAX(order_datetime))        AS dias_desde_ultima_compra,
        ROUND(AVG(dias_entre), 0)                          AS dias_entre_compras_medio,
        COUNT(DISTINCT DATE_FORMAT(order_datetime, '%Y-%m')) AS meses_ativos,
        COUNT(DISTINCT store_id)                           AS qtd_canais_distintos,
        ROUND(SUM(CASE WHEN shipping_type != 'Retirada em Loja' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS pct_online,
        ROUND(SUM(CASE WHEN coupon_code IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS pct_pedidos_com_cupom
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
) rev ON c.customer_id = rev.customer_id

ORDER BY c.customer_id;

-- ==========================================================================
-- 2.2  PREVISAO DE DEMANDA (Series Temporais)
-- ==========================================================================

-- [Q24] Serie temporal de vendas por produto e mes
SELECT
    p.product_id,
    p.product_name,
    p.product_category,
    DATE_FORMAT(o.order_datetime, '%Y-%m-01')              AS data_mes,
    SUM(oi.quantity)                                        AS unidades_vendidas,
    ROUND(SUM(oi.net_line_amount), 2)                      AS receita,
    COUNT(DISTINCT o.order_id)                             AS pedidos
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_status NOT IN ('Cancelado')
GROUP BY p.product_id, p.product_name, p.product_category, data_mes
ORDER BY p.product_id, data_mes;

-- [Q25] Serie temporal de vendas por CATEGORIA e mes (agregado)
SELECT
    p.product_category,
    DATE_FORMAT(o.order_datetime, '%Y-%m-01')              AS data_mes,
    SUM(oi.quantity)                                        AS unidades_vendidas,
    ROUND(SUM(oi.net_line_amount), 2)                      AS receita,
    COUNT(DISTINCT o.order_id)                             AS pedidos,
    COUNT(DISTINCT o.customer_id)                          AS clientes_unicos
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_status NOT IN ('Cancelado')
GROUP BY p.product_category, data_mes
ORDER BY p.product_category, data_mes;

-- ==========================================================================
-- 2.3  CUSTOMER LIFETIME VALUE (Regressao)
-- ==========================================================================

-- [Q26] Dataset CLV — features do 1o ano, target do 2o ano
SELECT
    c.customer_id,
    c.gender,
    TIMESTAMPDIFF(YEAR, c.birth_date, '2025-01-01')        AS idade,
    c.state,
    c.income_range,
    c.loyalty_tier,

    ano1.total_pedidos_ano1,
    ano1.valor_total_ano1,
    ano1.ticket_medio_ano1,
    ano1.qtd_categorias_distintas_ano1,
    ano1.qtd_meses_compra_ano1,

    COALESCE(ano2.valor_total_ano2, 0)                     AS valor_total_ano2

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
    SELECT
        customer_id,
        ROUND(SUM(net_amount), 2)                          AS valor_total_ano2
    FROM orders
    WHERE YEAR(order_datetime) = 2025
      AND order_status NOT IN ('Cancelado')
    GROUP BY customer_id
) ano2 ON c.customer_id = ano2.customer_id;

-- ==========================================================================
-- 2.4  PREVISAO DE DEVOLUCAO (Classificacao)
-- ==========================================================================

-- [Q27] Dataset de features por pedido para prever devolucao
SELECT
    o.order_id,
    -- Pedido
    s.channel,
    o.payment_method,
    o.shipping_type,
    CASE WHEN o.coupon_code IS NOT NULL THEN 1 ELSE 0 END  AS tem_cupom,
    o.gross_amount,
    o.discount_amount,
    o.net_amount,
    itens.qtd_itens,
    itens.qtd_categorias,
    itens.preco_medio_item,

    -- Dia/hora
    DAYOFWEEK(o.order_datetime)                            AS dia_semana,
    HOUR(o.order_datetime)                                 AS hora,
    MONTH(o.order_datetime)                                AS mes,

    -- Historico do cliente
    hist.compras_anteriores,
    hist.devolucoes_anteriores,
    hist.taxa_devolucao_historica,

    -- TARGET
    CASE WHEN ret.order_id IS NOT NULL THEN 1 ELSE 0 END  AS foi_devolvido

FROM orders o
JOIN stores s ON o.store_id = s.store_id

JOIN (
    SELECT
        oi.order_id,
        COUNT(*)                                           AS qtd_itens,
        COUNT(DISTINCT p.product_category)                 AS qtd_categorias,
        ROUND(AVG(oi.unit_price), 2)                       AS preco_medio_item
    FROM order_items oi
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY oi.order_id
) itens ON o.order_id = itens.order_id

LEFT JOIN (
    SELECT
        o2.customer_id,
        o2.order_id,
        COUNT(DISTINCT o_prev.order_id)                    AS compras_anteriores,
        COUNT(DISTINCT r_prev.return_id)                   AS devolucoes_anteriores,
        CASE
            WHEN COUNT(DISTINCT o_prev.order_id) > 0
            THEN ROUND(COUNT(DISTINCT r_prev.return_id) / COUNT(DISTINCT o_prev.order_id) * 100, 2)
            ELSE 0
        END                                                AS taxa_devolucao_historica
    FROM orders o2
    LEFT JOIN orders o_prev ON o2.customer_id = o_prev.customer_id
                            AND o_prev.order_datetime < o2.order_datetime
    LEFT JOIN returns r_prev ON o_prev.order_id = r_prev.order_id
    GROUP BY o2.customer_id, o2.order_id
) hist ON o.order_id = hist.order_id

LEFT JOIN (
    SELECT DISTINCT order_id FROM returns
) ret ON o.order_id = ret.order_id

WHERE o.order_status NOT IN ('Cancelado');

-- ==========================================================================
-- 2.5  PREVISAO DE AVALIACAO NEGATIVA (Classificacao)
-- ==========================================================================

-- [Q28] Dataset de features por produto para prever qualidade
SELECT
    p.product_id,
    p.product_category,
    p.product_subcategory,
    p.target_audience,
    p.list_price,
    p.unit_cost,
    ROUND((p.list_price - p.unit_cost) / p.list_price * 100, 2) AS margem_pct,
    p.package_size_ml_g,
    DATEDIFF('2026-01-01', p.launch_date)                  AS dias_no_mercado,

    -- Vendas
    COALESCE(vendas.unidades, 0)                           AS total_vendido,
    COALESCE(vendas.receita, 0)                            AS receita,

    -- Devolucoes
    COALESCE(devol.total_devolucoes, 0)                    AS total_devolucoes,
    COALESCE(ROUND(devol.total_devolucoes / vendas.unidades * 100, 2), 0) AS taxa_devolucao,

    -- Reclamacoes
    COALESCE(reclam.total_reclamacoes, 0)                  AS total_reclamacoes,

    -- Reviews
    COALESCE(rev.total_reviews, 0)                         AS total_reviews,
    COALESCE(rev.rating_medio, 0)                          AS rating_medio,
    COALESCE(rev.pct_negativas, 0)                         AS pct_reviews_negativas,

    -- TARGET
    CASE WHEN COALESCE(rev.rating_medio, 5) <= 3 THEN 1 ELSE 0 END AS produto_problematico

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

-- ==========================================================================
-- 2.6  SEGMENTACAO DE CLIENTES (Clustering — K-Means)
-- ==========================================================================

-- [Q29] Feature matrix para clustering de clientes
SELECT
    c.customer_id,
    -- Demographics (para analise pos-cluster)
    c.gender,
    c.state,
    c.income_range,
    c.loyalty_tier,

    -- RFM
    DATEDIFF('2026-01-01', MAX(o.order_datetime))          AS recency_dias,
    COUNT(DISTINCT o.order_id)                             AS frequency,
    ROUND(SUM(o.net_amount), 2)                            AS monetary,
    ROUND(AVG(o.net_amount), 2)                            AS ticket_medio,

    -- Preferencias
    COUNT(DISTINCT s.channel)                              AS canais_usados,
    ROUND(SUM(CASE WHEN s.channel IN ('E-commerce Proprio', 'Marketplace') THEN o.net_amount ELSE 0 END) /
          SUM(o.net_amount) * 100, 2)                      AS pct_gasto_online,

    -- Categorias preferidas
    MAX(CASE WHEN cat_rank.rk = 1 THEN cat_rank.product_category END) AS categoria_favorita,
    COUNT(DISTINCT cat_rank.product_category)              AS categorias_compradas,

    -- Engagement
    COALESCE(rev_agg.total_reviews, 0)                     AS total_reviews,
    COALESCE(rev_agg.rating_medio, 0)                      AS rating_medio_dado,
    COALESCE(comp_agg.total_reclamacoes, 0)                AS total_reclamacoes,
    COALESCE(ret_agg.total_devolucoes, 0)                  AS total_devolucoes

FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN stores s ON o.store_id = s.store_id
LEFT JOIN (
    SELECT oi2.order_id, p2.product_category,
           ROW_NUMBER() OVER (PARTITION BY o2.customer_id ORDER BY SUM(oi2.net_line_amount) DESC) AS rk
    FROM order_items oi2
    JOIN orders o2 ON oi2.order_id = o2.order_id
    JOIN products p2 ON oi2.product_id = p2.product_id
    GROUP BY o2.customer_id, oi2.order_id, p2.product_category
) cat_rank ON o.order_id = cat_rank.order_id
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
         rev_agg.total_reviews, rev_agg.rating_medio,
         comp_agg.total_reclamacoes, ret_agg.total_devolucoes;

-- ==========================================================================
-- 2.7  ANALISE DE CESTA (Market Basket — Association Rules)
-- ==========================================================================
-- Pares de produtos comprados juntos no mesmo pedido

-- [Q30] Co-ocorrencia de produtos (pares mais frequentes)
SELECT
    p1.product_name                                        AS produto_A,
    p2.product_name                                        AS produto_B,
    p1.product_category                                    AS categoria_A,
    p2.product_category                                    AS categoria_B,
    COUNT(*)                                               AS frequencia_juntos,
    ROUND(COUNT(*) / total_pedidos.n * 100, 4)             AS support_pct
FROM order_items oi1
JOIN order_items oi2 ON oi1.order_id = oi2.order_id
                     AND oi1.product_id < oi2.product_id
JOIN products p1 ON oi1.product_id = p1.product_id
JOIN products p2 ON oi2.product_id = p2.product_id
CROSS JOIN (SELECT COUNT(DISTINCT order_id) AS n FROM orders) total_pedidos
GROUP BY p1.product_name, p2.product_name, p1.product_category, p2.product_category, total_pedidos.n
HAVING frequencia_juntos >= 50
ORDER BY frequencia_juntos DESC
LIMIT 30;

-- [Q31] Co-ocorrencia por CATEGORIA (mais estavel para recomendacao)
SELECT
    p1.product_category                                    AS categoria_A,
    p2.product_category                                    AS categoria_B,
    COUNT(*)                                               AS frequencia_juntos,
    COUNT(DISTINCT oi1.order_id)                           AS pedidos_distintos,
    ROUND(AVG(oi1.net_line_amount + oi2.net_line_amount), 2) AS ticket_medio_par
FROM order_items oi1
JOIN order_items oi2 ON oi1.order_id = oi2.order_id
                     AND oi1.product_id < oi2.product_id
JOIN products p1 ON oi1.product_id = p1.product_id
JOIN products p2 ON oi2.product_id = p2.product_id
GROUP BY p1.product_category, p2.product_category
ORDER BY frequencia_juntos DESC;

-- ==========================================================================
-- 2.8  PREVISAO DE RUPTURA DE ESTOQUE (Classificacao)
-- ==========================================================================

-- [Q32] Dataset para previsao de ruptura (versao otimizada)
-- Muito mais rapido que LATERAL join sobre 525K registros
SELECT
    p.product_id,
    p.product_name,
    p.product_category,

    -- Estado atual (media entre lojas no ultimo snapshot)
    ultimo.stock_on_hand_medio,
    ultimo.stock_reserved_medio,
    ultimo.stock_in_transit_medio,
    ultimo.reorder_point,

    -- Velocidade de venda
    COALESCE(vendas.vendas_7d, 0)                          AS vendas_ultimos_7d,
    COALESCE(vendas.vendas_30d, 0)                         AS vendas_ultimos_30d,
    COALESCE(ROUND(vendas.vendas_30d / 30, 2), 0)         AS vendas_diarias,

    -- Variacao estoque (ultimo vs penultimo snapshot)
    COALESCE(ultimo.stock_on_hand_medio - penultimo.stock_anterior, 0) AS variacao_estoque,

    -- Projecao: dias ate ruptura
    CASE
        WHEN COALESCE(vendas.vendas_30d, 0) > 0
        THEN ROUND(ultimo.stock_on_hand_medio / (vendas.vendas_30d / 30), 0)
        ELSE 9999
    END                                                    AS dias_ate_ruptura,

    -- TARGET: estoque atual <= reorder_point
    CASE
        WHEN ultimo.stock_on_hand_medio <= ultimo.reorder_point THEN 1
        ELSE 0
    END                                                    AS em_ruptura

FROM products p

-- Ultimo snapshot (agregado por produto)
JOIN (
    SELECT product_id,
           ROUND(AVG(stock_on_hand), 0) AS stock_on_hand_medio,
           ROUND(AVG(stock_reserved), 0) AS stock_reserved_medio,
           ROUND(AVG(stock_in_transit), 0) AS stock_in_transit_medio,
           ROUND(AVG(reorder_point), 0) AS reorder_point
    FROM inventory_snapshots
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM inventory_snapshots)
    GROUP BY product_id
) ultimo ON p.product_id = ultimo.product_id

-- Penultimo snapshot (para calcular tendencia)
LEFT JOIN (
    SELECT product_id, ROUND(AVG(stock_on_hand), 0) AS stock_anterior
    FROM inventory_snapshots
    WHERE snapshot_date = (
        SELECT MAX(snapshot_date) FROM inventory_snapshots
        WHERE snapshot_date < (SELECT MAX(snapshot_date) FROM inventory_snapshots)
    )
    GROUP BY product_id
) penultimo ON p.product_id = penultimo.product_id

-- Vendas recentes
LEFT JOIN (
    SELECT oi.product_id,
           SUM(CASE WHEN o.order_datetime >= DATE_SUB(
               (SELECT MAX(order_datetime) FROM orders), INTERVAL 7 DAY) THEN oi.quantity ELSE 0 END) AS vendas_7d,
           SUM(oi.quantity) AS vendas_30d
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    WHERE o.order_datetime >= DATE_SUB((SELECT MAX(order_datetime) FROM orders), INTERVAL 30 DAY)
      AND o.order_status NOT IN ('Cancelado')
    GROUP BY oi.product_id
) vendas ON p.product_id = vendas.product_id

WHERE p.is_active = 1
ORDER BY dias_ate_ruptura ASC;

-- ==========================================================================
-- BONUS: QUERIES DE SUPORTE AO DASHBOARD
-- ==========================================================================

-- [Q33] Resumo executivo — KPIs principais (card metrics)
SELECT
    (SELECT COUNT(DISTINCT customer_id) FROM orders)       AS clientes_ativos,
    (SELECT COUNT(*) FROM orders WHERE order_status NOT IN ('Cancelado')) AS total_pedidos,
    (SELECT ROUND(SUM(net_amount), 2) FROM orders WHERE order_status NOT IN ('Cancelado')) AS receita_total,
    (SELECT ROUND(AVG(net_amount), 2) FROM orders WHERE order_status NOT IN ('Cancelado')) AS ticket_medio,
    (SELECT COUNT(*) FROM returns)                         AS total_devolucoes,
    (SELECT ROUND(AVG(rating), 2) FROM reviews)            AS rating_medio_geral,
    (SELECT COUNT(*) FROM complaints WHERE status = 'Aberto') AS reclamacoes_abertas,
    (SELECT COUNT(*) FROM products WHERE is_active = 1)    AS produtos_ativos;

-- [Q34] Evolucao semanal de receita (granularidade fina para dashboard)
SELECT
    DATE(DATE_SUB(order_datetime, INTERVAL WEEKDAY(order_datetime) DAY)) AS inicio_semana,
    COUNT(*)                                               AS pedidos,
    ROUND(SUM(net_amount), 2)                              AS receita,
    ROUND(AVG(net_amount), 2)                              AS ticket_medio,
    COUNT(DISTINCT customer_id)                            AS clientes_unicos
FROM orders
WHERE order_status NOT IN ('Cancelado')
GROUP BY inicio_semana
ORDER BY inicio_semana;

-- [Q35] Motivos de devolucao — ranking e valor
SELECT
    return_reason,
    COUNT(*)                                               AS total,
    ROUND(COUNT(*) / (SELECT COUNT(*) FROM returns) * 100, 2) AS pct,
    ROUND(SUM(refund_amount), 2)                           AS valor_devolvido,
    ROUND(AVG(refund_amount), 2)                           AS refund_medio
FROM returns
GROUP BY return_reason
ORDER BY total DESC;

-- [Q36] Cupons: eficacia e impacto
SELECT
    coupon_code,
    COUNT(*)                                               AS pedidos,
    ROUND(SUM(net_amount), 2)                              AS receita,
    ROUND(AVG(net_amount), 2)                              AS ticket_medio,
    ROUND(SUM(discount_amount), 2)                         AS desconto_dado,
    ROUND(AVG(discount_amount), 2)                         AS desconto_medio,
    ROUND(SUM(discount_amount) / SUM(gross_amount) * 100, 2) AS pct_desconto_sobre_bruto
FROM orders
WHERE coupon_code IS NOT NULL
  AND order_status NOT IN ('Cancelado')
GROUP BY coupon_code
ORDER BY receita DESC;
