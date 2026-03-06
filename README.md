# YMED Retail Analytics — Study Repository

> A hands-on study project for retail analytics using a fictional Brazilian cosmetics brand (**YMED**).
> It combines database engineering, SQL analytics, and machine-learning dataset preparation in a fully containerised MySQL environment.

---

## What This Project Is About

This repository simulates the data infrastructure of a mid-sized Brazilian cosmetics retailer operating across physical stores, e-commerce, and marketplaces (Jan 2024 – Dec 2025).

The goals are:

- Practice **database modelling** for retail operations (orders, inventory, returns, reviews, complaints).
- Write **analytical SQL** covering business KPIs, customer segmentation, and product performance.
- Prepare **ML-ready feature datasets** for common retail prediction tasks (churn, demand forecasting, CLV, etc.).
- Keep everything **reproducible** via Docker Compose and Python automation scripts.

---

## Repository Structure

```
.
├── docker-compose.yml              # MySQL 8 container (port 3307)
├── setup_mysql_retail_analytics.py # Automated DB setup & credential generation
├── seed_data.py                    # Generates ~750 000 realistic records
├── queries_analytics.sql           # 36 analytical + ML queries
├── sql/
│   ├── 001_schema.sql              # Core retail schema
│   ├── 002_ml_tables.sql           # ML feature tables
│   └── 003_ml_populate.sql         # ML table population
└── .gitignore
```

---

## Database Schema

Nine tables covering the full retail lifecycle:

| Table | Description |
|---|---|
| `products` | ~185 cosmetic SKUs (skincare, haircare, fragrance, makeup, body care) |
| `customers` | ~10 000 profiles with demographics, location, and loyalty tier |
| `stores` | ~50 locations across physical, e-commerce, and marketplace channels |
| `orders` | ~100 000 transactions with payment method and shipping details |
| `order_items` | Line-item detail for each order |
| `inventory_snapshots` | Weekly stock levels per store/product |
| `reviews` | ~15 000 customer ratings (1-5 stars) with text |
| `complaints` | ~5 000 support tickets by type, channel, and severity |
| `returns` | ~8 000 return transactions with reasons and refund amounts |

Seed data includes realistic Brazilian seasonality patterns (Mother's Day, Black Friday, Christmas).

---

## Analytics Queries (`queries_analytics.sql`)

36 queries split into two parts:

### Part 1 — Business Insights (queries 1-22)

- Monthly KPIs: orders, net revenue, average transaction value, cancellation rate
- Revenue by sales channel with market-share breakdown
- Top products and customers by lifetime value
- Margin analysis by category and subcategory
- Return rates and quality metrics via reviews
- RFM customer segmentation (Recency, Frequency, Monetary — scored 1-5)
- Demographic analysis by gender, region, and age bracket
- Cohort retention analysis by customer signup month
- Geographic Pareto concentration of revenue
- Shipping cost analysis relative to order value
- Inventory coverage and stock-rupture frequency
- Complaint tracking by type, severity, and resolution time

### Part 2 — ML Feature Datasets (queries 23-32)

Feature matrices ready for model training:

| Model | Target |
|---|---|
| Churn prediction | 90-day inactivity threshold |
| Demand forecasting | Time-series sales by product/category |
| Customer Lifetime Value | Year-over-year spending regression |
| Return prediction | Order characteristics + customer history |
| Product quality classification | Quality metrics + review sentiment |
| Customer clustering | Behavioural segmentation |
| Market basket analysis | Frequently co-purchased products |
| Stock rupture forecasting | Velocity + reorder points |

### Bonus (queries 33-36)

Executive dashboards, weekly revenue trends, return reason analysis, and coupon campaign effectiveness.

---

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Python 3.8+

### 1 — Start the database

```bash
python setup_mysql_retail_analytics.py
```

This will:
1. Validate that Docker is running and port 3307 is free.
2. Generate secure random credentials and save them to `.env` and `credentials.json`.
3. Launch a MySQL 8 container via Docker Compose and wait for it to be healthy.
4. Run the SQL scripts in `sql/` automatically on first start.

### 2 — Seed the data

```bash
python seed_data.py
```

Populates all nine tables with ~750 000 records in 3-5 minutes.

### 3 — Run the analytics

Connect to the database (default port **3307**) with any MySQL client and run `queries_analytics.sql`.

```bash
mysql -h 127.0.0.1 -P 3307 -u <app_user> -p retail_analytics < queries_analytics.sql
```

Credentials are printed after setup and stored in `credentials.json`.

---

## Tech Stack

| Tool | Role |
|---|---|
| MySQL 8 | Relational database |
| Docker Compose | Container orchestration |
| Python 3 | Automation & data generation |
| SQL | Analytics & ML dataset preparation |

---

*This is a study/knowledge-testing project — not a production system.*
