# 🛒 Retail Data Platform

An end-to-end retail analytics pipeline built with **Apache Airflow**, **Python**, **Snowflake**, and **Databricks** — simulating a real-world e-commerce data platform for supply chain and sales analytics.

> Built to demonstrate production-grade data engineering practices including pipeline orchestration, data quality validation, cloud warehousing, and analytics-ready transformations.

---

## 📐 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│         Kaggle E-Commerce Dataset (CSV / API)                   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                     INGESTION LAYER                             │
│              Python ETL Scripts (pandas, boto3)                 │
│                  Raw files staged to AWS S3                     │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                   ORCHESTRATION LAYER                           │
│                Apache Airflow (Docker)                          │
│     DAGs with SLA monitoring, retry logic & alerting            │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                     STORAGE LAYER                               │
│                  Snowflake Data Warehouse                       │
│         RAW schema → STAGING schema → ANALYTICS schema          │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                  TRANSFORMATION LAYER                           │
│            Databricks (PySpark / Delta Lake)                    │
│       Feature engineering + business metric computation         │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ANALYTICS LAYER                              │
│         Snowflake Analytics Tables + Summary Reports            │
│              Daily Revenue | Top Products | Retention           │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🧰 Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Python, Pandas |
| Orchestration | Apache Airflow (Docker) |
| Cloud Storage | AWS S3 |
| Data Warehouse | Snowflake |
| Transformation | Databricks (PySpark) |
| Data Quality | Custom Python validators |
| Version Control | Git / GitHub |

---

## 📦 Dataset

This project uses the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — a real-world retail dataset containing:

- 100,000+ orders across 2016–2018
- Product catalog, customer info, seller data
- Order reviews, payments, and geolocation

**Tables ingested:**
- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_products_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_sellers_dataset.csv`

---

## 🗂️ Project Structure

```
retail-data-platform/
│
├── dags/                          # Airflow DAGs
│   ├── ingest_retail_dag.py       # Main ingestion DAG
│   └── transform_retail_dag.py    # Transformation trigger DAG
│
├── ingestion/                     # Python ETL scripts
│   ├── ingest.py                  # Loads CSVs into Snowflake RAW
│   ├── validate.py                # Data quality checks
│   └── utils.py                   # Shared helpers
│
├── transformations/               # Databricks notebooks
│   ├── 01_staging.py              # Raw → Staging transformations
│   ├── 02_analytics.py            # Staging → Analytics metrics
│   └── 03_data_quality.py        # Post-transform validation
│
├── snowflake/                     # Snowflake setup scripts
│   ├── setup.sql                  # DB, schema, warehouse setup
│   └── ddl/                       # Table DDL definitions
│       ├── raw_tables.sql
│       ├── staging_tables.sql
│       └── analytics_tables.sql
│
├── docs/                          # Screenshots and architecture docs
│   ├── airflow_dag.png
│   ├── snowflake_tables.png
│   └── architecture.png
│
├── docker-compose.yml             # Airflow local setup
├── requirements.txt               # Python dependencies
├── .env.example                   # Environment variable template
└── README.md
```

---

## 📊 Analytics Produced

| Metric | Description |
|---|---|
| Daily Revenue | Total order revenue grouped by day |
| Top 10 Products | Best-selling products by volume and revenue |
| Customer Retention Rate | Repeat purchase rate by month |
| Average Order Value | Mean order size by product category |
| Seller Performance | Revenue and review score per seller |
| Order Fulfillment Time | Average days from order to delivery |

---

## ⚙️ Setup & Installation

### Prerequisites
- Python 3.9+
- Docker & Docker Compose
- Snowflake account (free trial at [snowflake.com](https://snowflake.com))
- Databricks account (free Community Edition at [databricks.com](https://databricks.com))
- AWS account (for S3) or use local file system for dev

### 1. Clone the repo
```bash
git clone https://github.com/divyagit24/retail-data-platform.git
cd retail-data-platform
```

### 2. Set up environment variables
```bash
cp .env.example .env
# Fill in your Snowflake, AWS, and Databricks credentials
```

### 3. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 4. Set up Snowflake
```bash
# Run the setup script in your Snowflake worksheet
snowflake/setup.sql
```

### 5. Start Airflow locally
```bash
docker-compose up -d
# Access Airflow UI at http://localhost:8080
# Default credentials: admin / admin
```

### 6. Download the dataset
- Go to [Kaggle dataset page](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- Download and place CSV files in `data/raw/`

### 7. Trigger the pipeline
```bash
# Via Airflow UI — enable and trigger `ingest_retail_dag`
# Or run manually:
python ingestion/ingest.py
```

---

## 🔄 Airflow DAG Overview

```
ingest_retail_dag (runs daily @ 6AM UTC)
│
├── check_source_files        # Verify CSV files exist
├── validate_raw_data         # Null checks, row count validation
├── load_to_snowflake_raw     # Python → Snowflake RAW schema
├── trigger_databricks_job    # Kick off Databricks transformation
├── validate_analytics_tables # Post-load data quality checks
└── send_pipeline_alert       # Slack/email alert on success or failure
```

**DAG features:**
- SLA monitoring with automatic alerting
- Idempotent design — safe to re-run without duplicates
- Retry logic with exponential backoff
- Full run logging via Airflow metadata DB

---

## ✅ Data Quality Checks

Each pipeline run validates:
- No null values in key fields (order_id, customer_id, product_id)
- Row counts match expected thresholds
- No duplicate primary keys
- Date fields within valid range
- Revenue values are non-negative

---

## 🚀 Key Design Decisions

**Why Snowflake?** Columnar storage and separation of compute/storage makes it ideal for analytical workloads with variable query patterns — the same reason it's widely adopted in enterprise retail platforms.

**Why Airflow?** Provides production-grade orchestration with dependency management, SLA tracking, and observability out of the box — critical for pipelines that business teams depend on daily.

**Why Databricks?** PySpark on Databricks handles large-scale transformations efficiently and integrates natively with Snowflake and S3, making it the natural choice for the transformation layer.

**Three-layer Snowflake schema (RAW → STAGING → ANALYTICS):** Mirrors industry-standard medallion architecture, ensuring raw data is always preserved and transformations are auditable.

---

## 📈 Sample Output

```
Daily Revenue Summary (2018-01)
─────────────────────────────────
Date          Orders    Revenue (USD)
2018-01-01    342       $28,450.00
2018-01-02    398       $33,120.50
2018-01-03    415       $35,890.25
...

Top 5 Products by Revenue
─────────────────────────────────
1. health_beauty         $142,300
2. watches_gifts         $128,900
3. bed_bath_table        $115,400
4. sports_leisure        $98,200
5. computers_accessories $87,600
```

---

## 🔮 Future Enhancements

- Add dbt models for additional transformation layer
- Integrate Great Expectations for advanced data quality
- Build Tableau dashboard on top of Snowflake analytics tables
- Add ML pipeline for demand forecasting using Databricks MLflow
- Containerize ingestion layer with Docker

---

## 👩‍💻 Author

**Divya P Jagadeesan**


[LinkedIn](https://www.linkedin.com/in/divya-pj/) • [GitHub](https://github.com/divyagit24)

---

## 📄 License

MIT License — free to use and adapt.
