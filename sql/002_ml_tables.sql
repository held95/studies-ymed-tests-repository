-- ============================================================================
-- 002_ml_tables.sql — Camada Analitica + ML/LLM
-- ============================================================================
-- Arquitetura:
--   CAMADA 2: Agregados analiticos (ML_FACT_*)  — dashboard rapido
--   CAMADA 3: Features ML (ML_FEATURES_*)       — inputs para treino
--   CAMADA 3: Outputs ML (ML_*_PREDICTIONS)     — resultados dos modelos
--   CAMADA 3: LLM Insights (ML_PRODUCT_TEXT_*)  — resumos gerados por LLM
--   CONTROLE: ML_MODEL_REGISTRY                 — auditoria de modelos
-- ============================================================================

USE retail_analytics;

-- **************************************************************************
-- CONTROLE — Registro de Modelos
-- **************************************************************************

CREATE TABLE IF NOT EXISTS ML_MODEL_REGISTRY (
    model_id        BIGINT AUTO_INCREMENT PRIMARY KEY,
    model_name      VARCHAR(120) NOT NULL,
    model_version   VARCHAR(32)  NOT NULL,
    model_type      VARCHAR(60)  NOT NULL COMMENT 'classification, regression, clustering, timeseries, association, llm',
    description     VARCHAR(500) NULL,
    accuracy        DECIMAL(6,4) NULL,
    precision_score DECIMAL(6,4) NULL,
    recall_score    DECIMAL(6,4) NULL,
    f1_score        DECIMAL(6,4) NULL,
    rmse            DECIMAL(12,4) NULL,
    mae             DECIMAL(12,4) NULL,
    training_rows   INT NULL,
    hyperparameters JSON NULL,
    is_active       TINYINT NOT NULL DEFAULT 1,
    trained_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uk_model_version (model_name, model_version),
    KEY idx_model_active (is_active, model_name)
) ENGINE=InnoDB;

-- **************************************************************************
-- CAMADA 2 — Agregados Analiticos (materializar para dashboard)
-- **************************************************************************

-- Vendas diarias por produto × loja
CREATE TABLE IF NOT EXISTS ML_FACT_DAILY_SALES (
    sale_date        DATE    NOT NULL,
    product_id       BIGINT  NOT NULL,
    store_id         BIGINT  NOT NULL,
    product_category VARCHAR(80)  NOT NULL,
    channel          VARCHAR(40)  NOT NULL,
    qty_sold         INT     NOT NULL DEFAULT 0,
    revenue_gross    DECIMAL(14,2) NOT NULL DEFAULT 0,
    revenue_net      DECIMAL(14,2) NOT NULL DEFAULT 0,
    discount_total   DECIMAL(14,2) NOT NULL DEFAULT 0,
    orders_count     INT     NOT NULL DEFAULT 0,
    customers_unique INT     NOT NULL DEFAULT 0,
    generated_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (sale_date, product_id, store_id),
    KEY idx_fact_sales_product (product_id, sale_date),
    KEY idx_fact_sales_store (store_id, sale_date),
    KEY idx_fact_sales_category (product_category, sale_date),
    KEY idx_fact_sales_channel (channel, sale_date)
) ENGINE=InnoDB;

-- KPIs mensais consolidados
CREATE TABLE IF NOT EXISTS ML_FACT_MONTHLY_KPI (
    ano_mes              CHAR(7)  NOT NULL PRIMARY KEY COMMENT 'formato YYYY-MM',
    total_pedidos        INT      NOT NULL DEFAULT 0,
    pedidos_validos      INT      NOT NULL DEFAULT 0,
    receita_bruta        DECIMAL(14,2) NOT NULL DEFAULT 0,
    receita_liquida      DECIMAL(14,2) NOT NULL DEFAULT 0,
    ticket_medio         DECIMAL(10,2) NOT NULL DEFAULT 0,
    desconto_total       DECIMAL(14,2) NOT NULL DEFAULT 0,
    frete_total          DECIMAL(14,2) NOT NULL DEFAULT 0,
    cancelamentos        INT      NOT NULL DEFAULT 0,
    pct_cancelamento     DECIMAL(6,2) NOT NULL DEFAULT 0,
    devolucoes           INT      NOT NULL DEFAULT 0,
    reclamacoes          INT      NOT NULL DEFAULT 0,
    clientes_ativos      INT      NOT NULL DEFAULT 0,
    novos_clientes       INT      NOT NULL DEFAULT 0,
    rating_medio         DECIMAL(4,2) NULL,
    generated_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    KEY idx_kpi_date (ano_mes)
) ENGINE=InnoDB;

-- Qualidade de produto (agregado cumulativo)
CREATE TABLE IF NOT EXISTS ML_FACT_PRODUCT_QUALITY (
    reference_date       DATE    NOT NULL,
    product_id           BIGINT  NOT NULL,
    product_category     VARCHAR(80) NOT NULL,
    reviews_count        INT     NOT NULL DEFAULT 0,
    avg_rating           DECIMAL(4,2) NULL,
    reviews_positive     INT     NOT NULL DEFAULT 0,
    reviews_negative     INT     NOT NULL DEFAULT 0,
    pct_negative         DECIMAL(6,2) NOT NULL DEFAULT 0,
    complaints_count     INT     NOT NULL DEFAULT 0,
    complaints_critical  INT     NOT NULL DEFAULT 0,
    returns_count        INT     NOT NULL DEFAULT 0,
    units_sold           INT     NOT NULL DEFAULT 0,
    return_rate_pct      DECIMAL(6,2) NOT NULL DEFAULT 0,
    generated_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (reference_date, product_id),
    KEY idx_quality_product (product_id, reference_date),
    KEY idx_quality_category (product_category, reference_date)
) ENGINE=InnoDB;

-- **************************************************************************
-- CAMADA 3 — ML Features (inputs para treino/inferencia)
-- **************************************************************************

-- Features para modelo de Churn
CREATE TABLE IF NOT EXISTS ML_FEATURES_CHURN (
    customer_id               BIGINT   NOT NULL,
    generated_at              DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Demographics
    gender                    VARCHAR(32)  NULL,
    idade                     INT      NULL,
    state                     VARCHAR(64)  NULL,
    income_range              VARCHAR(32)  NULL,
    loyalty_tier              VARCHAR(32)  NULL,
    dias_desde_signup         INT      NULL,

    -- Comportamento de compra
    total_pedidos             INT      NOT NULL DEFAULT 0,
    valor_total               DECIMAL(14,2) NOT NULL DEFAULT 0,
    ticket_medio              DECIMAL(10,2) NOT NULL DEFAULT 0,
    dias_desde_ultima_compra  INT      NOT NULL DEFAULT 0,
    dias_entre_compras_medio  INT      NULL,
    meses_ativos              INT      NOT NULL DEFAULT 0,
    qtd_canais_distintos      INT      NOT NULL DEFAULT 0,
    pct_online                DECIMAL(6,2) NOT NULL DEFAULT 0,
    pct_pedidos_com_cupom     DECIMAL(6,2) NOT NULL DEFAULT 0,

    -- Problemas
    total_reclamacoes         INT      NOT NULL DEFAULT 0,
    reclamacoes_graves        INT      NOT NULL DEFAULT 0,
    total_devolucoes          INT      NOT NULL DEFAULT 0,
    valor_devolvido           DECIMAL(14,2) NOT NULL DEFAULT 0,

    -- Reviews
    total_reviews             INT      NOT NULL DEFAULT 0,
    rating_medio              DECIMAL(4,2) NOT NULL DEFAULT 0,

    -- Target
    churned                   TINYINT  NOT NULL DEFAULT 0 COMMENT '1 = nao comprou nos ultimos 90 dias',

    PRIMARY KEY (customer_id, generated_at),
    KEY idx_churn_target (churned, generated_at),
    KEY idx_churn_tier (loyalty_tier, churned)
) ENGINE=InnoDB;

-- Features para modelo de CLV (Customer Lifetime Value)
CREATE TABLE IF NOT EXISTS ML_FEATURES_CLV (
    customer_id                    BIGINT   NOT NULL,
    generated_at                   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Demographics
    gender                         VARCHAR(32) NULL,
    idade                          INT      NULL,
    state                          VARCHAR(64) NULL,
    income_range                   VARCHAR(32) NULL,
    loyalty_tier                   VARCHAR(32) NULL,

    -- Features (periodo base)
    total_pedidos_periodo_base     INT      NOT NULL DEFAULT 0,
    valor_total_periodo_base       DECIMAL(14,2) NOT NULL DEFAULT 0,
    ticket_medio_periodo_base      DECIMAL(10,2) NOT NULL DEFAULT 0,
    qtd_categorias_distintas       INT      NOT NULL DEFAULT 0,
    qtd_meses_compra               INT      NOT NULL DEFAULT 0,

    -- Target (periodo futuro)
    valor_total_periodo_alvo       DECIMAL(14,2) NOT NULL DEFAULT 0,

    PRIMARY KEY (customer_id, generated_at),
    KEY idx_clv_target (valor_total_periodo_alvo)
) ENGINE=InnoDB;

-- Features para modelo de previsao de devolucao (por pedido)
CREATE TABLE IF NOT EXISTS ML_FEATURES_RETURN_RISK (
    order_id                  BIGINT   NOT NULL,
    generated_at              DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Pedido
    channel                   VARCHAR(40)  NULL,
    payment_method            VARCHAR(40)  NULL,
    shipping_type             VARCHAR(40)  NULL,
    tem_cupom                 TINYINT  NOT NULL DEFAULT 0,
    gross_amount              DECIMAL(12,2) NOT NULL DEFAULT 0,
    discount_amount           DECIMAL(12,2) NOT NULL DEFAULT 0,
    net_amount                DECIMAL(12,2) NOT NULL DEFAULT 0,
    qtd_itens                 INT      NOT NULL DEFAULT 0,
    qtd_categorias            INT      NOT NULL DEFAULT 0,
    preco_medio_item          DECIMAL(10,2) NOT NULL DEFAULT 0,

    -- Temporal
    dia_semana                TINYINT  NULL,
    hora                      TINYINT  NULL,
    mes                       TINYINT  NULL,

    -- Historico do cliente
    compras_anteriores        INT      NOT NULL DEFAULT 0,
    devolucoes_anteriores     INT      NOT NULL DEFAULT 0,
    taxa_devolucao_historica  DECIMAL(6,2) NOT NULL DEFAULT 0,

    -- Target
    foi_devolvido             TINYINT  NOT NULL DEFAULT 0,

    PRIMARY KEY (order_id, generated_at),
    KEY idx_return_target (foi_devolvido, generated_at)
) ENGINE=InnoDB;

-- Features para modelo de risco de produto
CREATE TABLE IF NOT EXISTS ML_FEATURES_PRODUCT_RISK (
    product_id                BIGINT   NOT NULL,
    generated_at              DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    product_category          VARCHAR(80) NOT NULL,
    product_subcategory       VARCHAR(120) NULL,
    target_audience           VARCHAR(32) NULL,
    list_price                DECIMAL(10,2) NOT NULL DEFAULT 0,
    unit_cost                 DECIMAL(10,2) NOT NULL DEFAULT 0,
    margem_pct                DECIMAL(6,2) NOT NULL DEFAULT 0,
    package_size_ml_g         INT      NULL,
    dias_no_mercado           INT      NOT NULL DEFAULT 0,

    -- Vendas
    total_vendido             INT      NOT NULL DEFAULT 0,
    receita                   DECIMAL(14,2) NOT NULL DEFAULT 0,

    -- Problemas
    total_devolucoes          INT      NOT NULL DEFAULT 0,
    taxa_devolucao            DECIMAL(6,2) NOT NULL DEFAULT 0,
    total_reclamacoes         INT      NOT NULL DEFAULT 0,

    -- Reviews
    total_reviews             INT      NOT NULL DEFAULT 0,
    rating_medio              DECIMAL(4,2) NOT NULL DEFAULT 0,
    pct_reviews_negativas     DECIMAL(6,2) NOT NULL DEFAULT 0,

    -- Target
    produto_problematico      TINYINT  NOT NULL DEFAULT 0 COMMENT '1 = rating medio <= 3',

    PRIMARY KEY (product_id, generated_at),
    KEY idx_product_risk_target (produto_problematico, generated_at)
) ENGINE=InnoDB;

-- Features para segmentacao de clientes (clustering)
CREATE TABLE IF NOT EXISTS ML_FEATURES_SEGMENTATION (
    customer_id               BIGINT   NOT NULL,
    generated_at              DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Demographics
    gender                    VARCHAR(32) NULL,
    state                     VARCHAR(64) NULL,
    income_range              VARCHAR(32) NULL,
    loyalty_tier              VARCHAR(32) NULL,

    -- RFM
    recency_dias              INT      NOT NULL DEFAULT 0,
    frequency                 INT      NOT NULL DEFAULT 0,
    monetary                  DECIMAL(14,2) NOT NULL DEFAULT 0,
    ticket_medio              DECIMAL(10,2) NOT NULL DEFAULT 0,

    -- Preferencias
    canais_usados             INT      NOT NULL DEFAULT 0,
    pct_gasto_online          DECIMAL(6,2) NOT NULL DEFAULT 0,
    categoria_favorita        VARCHAR(80) NULL,
    categorias_compradas      INT      NOT NULL DEFAULT 0,

    -- Engagement
    total_reviews             INT      NOT NULL DEFAULT 0,
    rating_medio_dado         DECIMAL(4,2) NOT NULL DEFAULT 0,
    total_reclamacoes         INT      NOT NULL DEFAULT 0,
    total_devolucoes          INT      NOT NULL DEFAULT 0,

    PRIMARY KEY (customer_id, generated_at),
    KEY idx_seg_tier (loyalty_tier, generated_at)
) ENGINE=InnoDB;

-- **************************************************************************
-- CAMADA 3 — ML Outputs (resultados dos modelos)
-- **************************************************************************

-- Previsoes de churn
CREATE TABLE IF NOT EXISTS ML_CHURN_PREDICTIONS (
    customer_id         BIGINT   NOT NULL,
    model_id            BIGINT   NOT NULL,
    churn_probability   DECIMAL(6,4) NOT NULL,
    churn_label         TINYINT  NOT NULL COMMENT '0 = ativo, 1 = churn previsto',
    risk_tier           VARCHAR(16) NOT NULL COMMENT 'Alto, Medio, Baixo',
    generated_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (customer_id, model_id),
    KEY idx_churn_pred_risk (risk_tier, churn_probability DESC),
    KEY idx_churn_pred_model (model_id),
    CONSTRAINT fk_churn_model FOREIGN KEY (model_id) REFERENCES ML_MODEL_REGISTRY(model_id)
) ENGINE=InnoDB;

-- Previsao de demanda (series temporais)
CREATE TABLE IF NOT EXISTS ML_SALES_FORECAST (
    forecast_date       DATE     NOT NULL,
    product_id          BIGINT   NOT NULL,
    model_id            BIGINT   NOT NULL,
    product_category    VARCHAR(80) NOT NULL,
    yhat                DECIMAL(12,2) NOT NULL COMMENT 'previsao pontual (unidades ou receita)',
    yhat_lower          DECIMAL(12,2) NULL COMMENT 'limite inferior do intervalo de confianca',
    yhat_upper          DECIMAL(12,2) NULL COMMENT 'limite superior do intervalo de confianca',
    forecast_type       VARCHAR(20) NOT NULL DEFAULT 'units' COMMENT 'units ou revenue',
    generated_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (forecast_date, product_id, model_id, forecast_type),
    KEY idx_forecast_product (product_id, forecast_date),
    KEY idx_forecast_category (product_category, forecast_date),
    KEY idx_forecast_model (model_id),
    CONSTRAINT fk_forecast_model FOREIGN KEY (model_id) REFERENCES ML_MODEL_REGISTRY(model_id)
) ENGINE=InnoDB;

-- Previsao de CLV
CREATE TABLE IF NOT EXISTS ML_CLV_PREDICTIONS (
    customer_id         BIGINT   NOT NULL,
    model_id            BIGINT   NOT NULL,
    predicted_value_12m DECIMAL(14,2) NOT NULL,
    confidence_lower    DECIMAL(14,2) NULL,
    confidence_upper    DECIMAL(14,2) NULL,
    clv_tier            VARCHAR(16) NOT NULL COMMENT 'Diamante, Ouro, Prata, Bronze',
    generated_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (customer_id, model_id),
    KEY idx_clv_pred_tier (clv_tier),
    KEY idx_clv_pred_model (model_id),
    CONSTRAINT fk_clv_model FOREIGN KEY (model_id) REFERENCES ML_MODEL_REGISTRY(model_id)
) ENGINE=InnoDB;

-- Previsao de devolucao por pedido
CREATE TABLE IF NOT EXISTS ML_RETURN_PREDICTIONS (
    order_id            BIGINT   NOT NULL,
    model_id            BIGINT   NOT NULL,
    return_probability  DECIMAL(6,4) NOT NULL,
    return_label        TINYINT  NOT NULL COMMENT '0 = nao devolvido, 1 = devolvido previsto',
    risk_tier           VARCHAR(16) NOT NULL COMMENT 'Alto, Medio, Baixo',
    generated_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (order_id, model_id),
    KEY idx_return_pred_risk (risk_tier),
    KEY idx_return_pred_model (model_id),
    CONSTRAINT fk_return_model FOREIGN KEY (model_id) REFERENCES ML_MODEL_REGISTRY(model_id)
) ENGINE=InnoDB;

-- Score de risco por produto
CREATE TABLE IF NOT EXISTS ML_PRODUCT_RISK_SCORES (
    product_id          BIGINT   NOT NULL,
    model_id            BIGINT   NOT NULL,
    risk_score          DECIMAL(6,4) NOT NULL COMMENT '0.0 = sem risco, 1.0 = risco maximo',
    risk_label          TINYINT  NOT NULL COMMENT '0 = ok, 1 = problematico',
    top_risk_factor     VARCHAR(120) NULL COMMENT 'ex: alta taxa de devolucao, reviews negativas',
    generated_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (product_id, model_id),
    KEY idx_product_risk_score (risk_score DESC),
    KEY idx_product_risk_model (model_id),
    CONSTRAINT fk_product_risk_model FOREIGN KEY (model_id) REFERENCES ML_MODEL_REGISTRY(model_id)
) ENGINE=InnoDB;

-- Segmentos de clientes (output de clustering)
CREATE TABLE IF NOT EXISTS ML_CUSTOMER_SEGMENTS (
    customer_id         BIGINT   NOT NULL,
    model_id            BIGINT   NOT NULL,
    segment_id          INT      NOT NULL,
    segment_name        VARCHAR(60) NULL COMMENT 'ex: VIP Digital, Esporadico Presencial',
    cluster_distance    DECIMAL(10,4) NULL COMMENT 'distancia ao centroide do cluster',
    generated_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (customer_id, model_id),
    KEY idx_segment_id (segment_id, model_id),
    KEY idx_segment_model (model_id),
    CONSTRAINT fk_segment_model FOREIGN KEY (model_id) REFERENCES ML_MODEL_REGISTRY(model_id)
) ENGINE=InnoDB;

-- Regras de associacao (market basket)
CREATE TABLE IF NOT EXISTS ML_MARKET_BASKET (
    product_a_id        BIGINT   NOT NULL,
    product_b_id        BIGINT   NOT NULL,
    model_id            BIGINT   NOT NULL,
    frequency           INT      NOT NULL DEFAULT 0,
    support             DECIMAL(8,6) NOT NULL DEFAULT 0 COMMENT 'fracao dos pedidos com ambos',
    confidence          DECIMAL(6,4) NOT NULL DEFAULT 0 COMMENT 'P(B|A)',
    lift                DECIMAL(8,4) NOT NULL DEFAULT 0 COMMENT 'lift > 1 = associacao positiva',
    generated_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (product_a_id, product_b_id, model_id),
    KEY idx_basket_lift (lift DESC),
    KEY idx_basket_model (model_id),
    CONSTRAINT fk_basket_model FOREIGN KEY (model_id) REFERENCES ML_MODEL_REGISTRY(model_id)
) ENGINE=InnoDB;

-- Previsao de ruptura de estoque
CREATE TABLE IF NOT EXISTS ML_STOCK_RUPTURE_FORECAST (
    product_id              BIGINT   NOT NULL,
    store_id                BIGINT   NOT NULL,
    forecast_date           DATE     NOT NULL,
    model_id                BIGINT   NOT NULL,
    rupture_probability     DECIMAL(6,4) NOT NULL,
    days_until_rupture      INT      NULL,
    current_stock           INT      NOT NULL DEFAULT 0,
    recommended_reorder_qty INT      NULL,
    generated_at            DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (product_id, store_id, forecast_date, model_id),
    KEY idx_rupture_prob (rupture_probability DESC),
    KEY idx_rupture_date (forecast_date),
    KEY idx_rupture_model (model_id),
    CONSTRAINT fk_rupture_model FOREIGN KEY (model_id) REFERENCES ML_MODEL_REGISTRY(model_id)
) ENGINE=InnoDB;

-- Insights de texto gerados por LLM (resumos de reviews/complaints)
CREATE TABLE IF NOT EXISTS ML_PRODUCT_TEXT_INSIGHTS (
    product_id          BIGINT      NOT NULL,
    insight_type        VARCHAR(40) NOT NULL COMMENT 'review_summary, complaint_summary, sentiment_analysis',
    model_id            BIGINT      NOT NULL,
    summary_text        TEXT        NULL,
    top_topics          JSON        NULL COMMENT '["hidratacao", "textura", "preco"]',
    sentiment_score     DECIMAL(4,2) NULL COMMENT '-1.0 a 1.0',
    key_phrases         JSON        NULL COMMENT '{"positivas": [...], "negativas": [...]}',
    reviews_analyzed    INT         NULL,
    complaints_analyzed INT         NULL,
    generated_at        DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (product_id, insight_type, model_id),
    KEY idx_text_insight_type (insight_type),
    KEY idx_text_sentiment (sentiment_score),
    KEY idx_text_model (model_id),
    CONSTRAINT fk_text_insight_model FOREIGN KEY (model_id) REFERENCES ML_MODEL_REGISTRY(model_id)
) ENGINE=InnoDB;
