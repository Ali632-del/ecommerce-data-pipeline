# E-Commerce Data Pipeline — Portfolio Project

A production-grade, end-to-end data engineering pipeline built on an
Indian e-commerce product dataset (1,000 SKUs, 24 columns, 97 categories).

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                 │
│   CSV dataset  ──►  producer.py (batch simulator)                   │
│                      │   (optional: → Kafka topic → consumer.py)   │
└──────────────────────┼──────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     RAW DATA LAKE  (immutable)                      │
│   data/raw/<YYYY>/<MM>/<DD>/<HH>/batch_NNNN.parquet                │
│   • Partitioned by ingest hour                                      │
│   • Never overwritten — append-only                                 │
└──────────────────────┼──────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   PROCESSING LAYER  (transform.py)                  │
│   1. Schema enforcement & type casting                              │
│   2. Price cleaning  (₹3,995.00 → 3995.00 float)                  │
│   3. Missing-value strategy  (median / constant fills)             │
│   4. Feature engineering                                            │
│      • discount_amount  • price_bucket  • ratings_tier             │
│      • has_seller       • rating_x_count (popularity score)        │
│   5. Deduplication by product_id                                    │
│   6. Range validation                                               │
│                                                                     │
│   Output → data/processed/products_cleaned.parquet                 │
└──────────────────────┼──────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│              DATA WAREHOUSE — PostgreSQL  (Star Schema)             │
│                                                                     │
│   dim_category ◄──── fact_product_listing ────► dim_seller         │
│                             │                                       │
│                        dim_product                                  │
│                                                                     │
│   Schema: dw  │  Batch upserts  │  ON CONFLICT DO NOTHING / UPDATE │
└──────────────────────┼──────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      ANALYTICS LAYER                                │
│   8 SQL queries → Power BI / Tableau dashboards                     │
│   KPIs: Revenue Potential │ Avg Rating │ Discount Effectiveness     │
│         Seller Performance │ Price Segmentation │ Listing Quality   │
└─────────────────────────────────────────────────────────────────────┘

ORCHESTRATION: Apache Airflow DAG (hourly schedule)
  start → ingest_raw_data → transform_data → load_warehouse
        → run_analytics_check → end
```

---

## Project Structure

```
ecommerce_pipeline/
├── config/
│   ├── config.yaml          # All pipeline parameters
│   └── settings.py          # Config loader + logger factory
├── dags/
│   └── ecommerce_pipeline_dag.py   # Airflow DAG
├── ingestion/
│   ├── producer.py          # Batch simulator / Kafka producer
│   └── consumer.py          # Kafka consumer (optional)
├── processing/
│   └── transform.py         # Full transformation pipeline
├── warehouse/
│   └── loader.py            # PostgreSQL Star Schema loader
├── analytics/
│   └── analytics_queries.sql   # BI queries + dashboard blueprint
├── data/
│   ├── source/              # Place Combined_dataset.csv here
│   ├── raw/                 # Data lake (auto-created)
│   └── processed/           # Cleaned Parquet (auto-created)
├── logs/                    # Rotating log files (auto-created)
├── requirements.txt
├── .env.example
└── README.md
```

---

## Quick Start

### 1. Environment setup
```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env               # Fill in DB_PASSWORD
```

### 2. Place source data
```bash
mkdir -p data/source
cp /path/to/Combined_dataset.csv data/source/
```

### 3. Run pipeline manually (no Airflow)
```bash
# Ingest — writes raw Parquet to data/raw/
python ingestion/producer.py

# Transform — writes cleaned Parquet to data/processed/
python processing/transform.py

# Load — pushes to PostgreSQL
python warehouse/loader.py
```

### 4. Run with Airflow
```bash
export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create --role Admin --username admin \
    --firstname Data --lastname Engineer \
    --email admin@example.com --password admin

# Copy DAG
cp dags/ecommerce_pipeline_dag.py ~/airflow/dags/

airflow webserver --port 8080 &
airflow scheduler &
# Open http://localhost:8080 → trigger ecommerce_pipeline DAG
```

### 5. PostgreSQL setup
```sql
CREATE DATABASE ecommerce_dw;
CREATE USER pipeline_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE ecommerce_dw TO pipeline_user;
-- Tables are created automatically by loader.py
```

---

## Design Decisions

| Decision | Choice | Justification |
|---|---|---|
| Processing engine | **Pandas** | Dataset is 1K rows; Pandas is zero-config and faster to prototype. PySpark upgrade is 5-line change. |
| Storage format | **Parquet** | Columnar, compressed, schema-preserving — industry standard for data lakes. |
| Warehouse | **PostgreSQL** | Free, ACID, supported by every BI tool. Snowflake/BigQuery are drop-in replacements via SQLAlchemy. |
| Orchestration | **Airflow** | Industry standard; DAG-as-code makes pipelines reviewable and testable. |
| Data Lake partitioning | **Hourly** | Balances file count vs query pruning. Daily is fine for portfolio. |
| Missing seller | **"Unknown Seller"** | Keeps FK integrity in dim_seller; filterable in analytics. |

---

## Kafka Integration (Optional)

To enable real streaming:

1. Start Kafka: `docker compose up kafka zookeeper`
2. In `ingestion/producer.py`, uncomment the `KafkaProducer` block
3. In `ingestion/consumer.py`, uncomment the `KafkaConsumer` loop
4. Run producer and consumer in separate terminals

The rest of the pipeline (transform → load) is unchanged.

---

## KPIs & Metrics

| KPI | SQL Reference |
|---|---|
| Total SKUs & revenue potential | Query 1 |
| Category revenue ranking | Query 2 |
| Price tier distribution | Query 3 |
| Top sellers by catalog size | Query 4 |
| Rating quality funnel | Query 5 |
| Popularity-weighted top products | Query 6 |
| Discount ↔ Rating correlation | Query 7 |
| Listing completeness vs performance | Query 8 |
