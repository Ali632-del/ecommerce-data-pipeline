# 🚀 Real-Time E-Commerce Data Pipeline & Analytics System

<div align="center">

![Python](https://img.shields.io/badge/Python-3.14-blue?style=for-the-badge&logo=python)
![Pandas](https://img.shields.io/badge/Pandas-2.2-150458?style=for-the-badge&logo=pandas)
![SQLite](https://img.shields.io/badge/SQLite-Data_Warehouse-003B57?style=for-the-badge&logo=sqlite)
![Airflow](https://img.shields.io/badge/Apache_Airflow-Orchestration-017CEE?style=for-the-badge&logo=apacheairflow)
![Jupyter](https://img.shields.io/badge/Jupyter-Analytics-F37626?style=for-the-badge&logo=jupyter)

**A production-grade, end-to-end data engineering portfolio project**  
*Built by [Ali Hamdy Rizq](https://github.com/Ali632-del)*

</div>

---

## 📌 Project Overview

This project simulates a **real-world data engineering pipeline** for an Indian e-commerce platform. It covers the full data lifecycle — from raw ingestion to business-ready analytics — following modern **Data Lakehouse** architecture principles.

| Dataset | 1,000 Products | 97 Categories | 24 Columns |
|---|---|---|---|
| Avg Rating | 3.62 / 5 | Avg Discount | 53.8% |
| Revenue Potential | INR 17,06,096 | Known Sellers | 699 |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        DATA SOURCE                          │
│              Combined_dataset.csv (E-Commerce)              │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              INGESTION LAYER  (producer.py)                 │
│   Splits CSV into 50-row batches → simulates streaming      │
│   Optional: Apache Kafka integration ready                  │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│           RAW DATA LAKE  (Immutable Parquet)                │
│   data/raw/YYYY/MM/DD/HH/batch_NNNN.parquet                │
│   Partitioned by ingest hour — append-only                  │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│          PROCESSING LAYER  (transform.py)                   │
│   ✓ Schema enforcement & type casting                       │
│   ✓ Price cleaning  (₹3,995.00 → 3995.0)                  │
│   ✓ Missing value strategy                                  │
│   ✓ Feature Engineering (9 new columns)                     │
│   ✓ Deduplication & validation                              │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│        DATA WAREHOUSE — SQLite  (Star Schema)               │
│                                                             │
│   dim_category ◄── fact_product_listing ──► dim_seller      │
│                           │                                 │
│                      dim_product                            │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│            ANALYTICS LAYER  (analytics.ipynb)               │
│   6 Business Charts | KPI Dashboard | SQL Insights          │
└─────────────────────────────────────────────────────────────┘

         ORCHESTRATION: Apache Airflow DAG (hourly)
         start → ingest → transform → load → quality_check → end
```

---

## 📁 Project Structure

```
ecommerce-data-pipeline/
├── 📂 config/
│   ├── config.yaml          # All pipeline parameters
│   └── settings.py          # Config loader + logger factory
├── 📂 ingestion/
│   ├── producer.py          # Batch simulator / Kafka producer
│   └── consumer.py          # Kafka consumer (optional)
├── 📂 processing/
│   └── transform.py         # Full transformation pipeline
├── 📂 warehouse/
│   └── loader.py            # SQLite Star Schema loader
├── 📂 data/
│   ├── raw/                 # Data lake (Parquet partitions)
│   └── processed/           # Cleaned Parquet
├── 📊 analytics.ipynb       # Business insights & charts
├── 📄 analytics_queries.sql # BI SQL queries
├── 📄 requirements.txt
└── 📄 README.md
```

---

## ⚙️ Feature Engineering

| New Column | Description |
|---|---|
| `discount_amount` | initial_price - final_price |
| `is_discounted` | Boolean — any discount applied |
| `price_bucket` | budget / mid_range / premium / luxury |
| `ratings_tier` | excellent / good / average / poor |
| `has_seller` | Boolean — known seller |
| `has_variations` | Boolean — product has variants |
| `category_slug` | Normalized category name |
| `title_word_count` | Listing completeness proxy |
| `rating_x_count` | Weighted popularity score |

---

## 📊 Analytics Insights

### Price Segmentation
| Tier | Products | % of Catalog |
|---|---|---|
| Budget (< ₹500) | 151 | 15.1% |
| Mid-Range (₹500–₹2K) | 592 | 59.2% |
| Premium (₹2K–₹5K) | 217 | 21.7% |
| Luxury (> ₹5K) | 40 | 4.0% |

### Rating Quality
| Tier | Products |
|---|---|
| Excellent (≥ 4.5★) | 165 |
| Good (4.0–4.4★) | 450 |
| Average (3.0–3.9★) | 247 |
| Poor (< 3.0★) | 138 |

---

## 🚀 Quick Start

### 1. Clone & Setup
```bash
git clone https://github.com/Ali632-del/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline

python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### 2. Place Source Data
```bash
mkdir -p data/source
# Copy Combined_dataset.csv into data/source/
```

### 3. Run the Pipeline
```bash
python ingestion/producer.py     # Ingest → Data Lake
python processing/transform.py   # Transform → Clean Data
python warehouse/loader.py       # Load → Data Warehouse
```

### 4. Explore Analytics
```bash
jupyter notebook analytics.ipynb
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.14 |
| Data Processing | Pandas, NumPy |
| Storage Format | Apache Parquet |
| Data Warehouse | SQLite (Star Schema) |
| Orchestration | Apache Airflow |
| Analytics | Jupyter, Matplotlib, Seaborn |
| Streaming (Optional) | Apache Kafka |
| Version Control | Git / GitHub |

---

## 🔮 Future Enhancements

- [ ] Migrate warehouse to PostgreSQL / Snowflake
- [ ] Enable Apache Kafka for true real-time streaming
- [ ] Add Great Expectations for data quality validation
- [ ] Build Power BI / Tableau dashboard
- [ ] Containerize with Docker Compose
- [ ] Deploy pipeline to AWS (S3 + Glue + Redshift)

---

## 👨‍💻 Author

**Ali Hamdy Rizq**  
Data Engineer | Python | SQL | ETL Pipelines  
🔗 [GitHub](https://github.com/Ali632-del)

---

<div align="center">
⭐ If you found this project useful, please give it a star!
</div>
