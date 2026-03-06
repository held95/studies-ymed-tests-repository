# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Retail analytics platform for Brazilian cosmetics brand YMED. Runs MySQL 8.0 in Docker, generates ~750K synthetic records, and provides SQL queries + ML feature tables for dashboards and model training.

## Commands

### Setup (first time or after reset)
```bash
# 1. Start MySQL container and initialize schema
python3 setup_mysql_retail_analytics.py --port 3307

# 2. Seed all tables with synthetic data (~15 seconds)
python3 seed_data.py --force

# 3. Create ML_ aggregate and feature tables
docker exec -i ymed-retail-mysql mysql -uroot -p"$MYSQL_ROOT_PASSWORD" retail_analytics < sql/002_ml_tables.sql
docker exec -i ymed-retail-mysql mysql -uroot -p"$MYSQL_ROOT_PASSWORD" retail_analytics < sql/003_ml_populate.sql
```

### Database access
```bash
# Load .env first, then connect
source .env
docker exec -it ymed-retail-mysql mysql -uroot -p"$MYSQL_ROOT_PASSWORD" retail_analytics

# Run a SQL file
docker exec -i ymed-retail-mysql mysql -uroot -p"$MYSQL_ROOT_PASSWORD" retail_analytics < sql/some_file.sql

# Run an inline query
docker exec ymed-retail-mysql mysql -uroot -p"$MYSQL_ROOT_PASSWORD" retail_analytics -e "SELECT COUNT(*) FROM orders;"
```

### Reset everything
```bash
docker compose down -v   # destroys container + named volume
python3 setup_mysql_retail_analytics.py
python3 seed_data.py --force
```

### Container status
```bash
docker ps --filter "name=ymed"    # should show "healthy"
docker compose logs mysql-retail
```

## Architecture

### 3-Layer Table Design

**Layer 1 — Normalized base** (`sql/001_schema.sql`, auto-loaded by Docker on first start)
- Dimension tables: `products` (200), `customers` (10K), `stores` (50)
- Fact tables: `orders` (100K), `order_items` (~237K), `inventory_snapshots` (~525K)
- Text tables: `complaints`, `reviews`, `returns`, `text_embeddings`

**Layer 2 — Aggregated analytics** (`ML_FACT_*` in `sql/002_ml_tables.sql`, populated by `003`)
- Pre-computed for dashboard performance — avoids live JOINs across hundreds of thousands of rows
- `ML_FACT_DAILY_SALES`: product × store × day (~211K rows)
- `ML_FACT_MONTHLY_KPI`: one row per month (24 rows)
- `ML_FACT_PRODUCT_QUALITY`: one row per product (180 rows)

**Layer 3 — ML Features + Outputs** (`ML_FEATURES_*` and prediction tables)
- Feature tables hold engineered inputs for model training, each with a `generated_at` PK component for historical snapshots
- Output tables (`ML_CHURN_PREDICTIONS`, `ML_SALES_FORECAST`, etc.) are empty — filled when Python ML models run
- LLM insights tables (`ML_PRODUCT_TEXT_*`) hold LLM-generated summaries per product
- `ML_MODEL_REGISTRY` links every prediction row to its trained model via `model_id` FK

### Key files
| File | Purpose |
|---|---|
| `setup_mysql_retail_analytics.py` | Docker orchestrator: generates `.env` credentials, starts container, waits for health check, writes `credentials.json` |
| `seed_data.py` | Generates all synthetic data; uses `random.seed(42)` for reproducibility; accepts `--force` to skip confirmation |
| `queries_analytics.sql` | 36 validated queries: Q01–Q22 dashboard insights, Q23–Q32 ML feature datasets, Q33–Q36 bonus KPIs |
| `sql/001_schema.sql` | Auto-executed by Docker on first start via `/docker-entrypoint-initdb.d/` mount |
| `sql/002_ml_tables.sql` | DDL for all 18 ML_ tables; idempotent (`CREATE TABLE IF NOT EXISTS`) |
| `sql/003_ml_populate.sql` | `INSERT...SELECT` from base tables; opens with `TRUNCATE` block so it's idempotent |

### Docker / connectivity
- Container name: `ymed-retail-mysql`, port `3307` on host (maps to `3306` inside)
- Named volume: `ymed_mysql_data` (survives `docker compose down`, destroyed with `-v`)
- Authentication: `mysql_native_password` plugin for DBeaver compatibility
- LAN access: binds to `0.0.0.0` — other machines connect to host's LAN IP on port 3307
- Credentials live in `.env` (gitignored); `credentials.json` is generated for sharing with team

### Python dependencies
No `requirements.txt`. Uses only stdlib + one of these DB drivers (tried in order):
- `mysql-connector-python`
- `pymysql`

### Data conventions
- All PKs are `BIGINT` (not `AUTO_INCREMENT` in base tables — seed script assigns IDs)
- Brazilian locale: cities, names, seasonality (May = Mother's Day +30%, Nov = Black Friday +50%, Dec = Christmas +60%)
- Deterministic seed: `random.seed(42)` in `seed_data.py` — re-running always produces same dataset
- ML feature tables use composite PK `(entity_id, generated_at)` to support multiple historical snapshots
