# E-commerce Customer Behavior Analytics Platform
## Data Lakehouse Architecture with Medallion Pattern (Bronze → Silver → Gold)

![Python](https://img.shields.io/badge/Python-3.11.9-blue.svg)
![PySpark](https://img.shields.io/badge/PySpark-3.5.0-orange.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)
![TFM](https://img.shields.io/badge/TFM-UCM%20Big%20Data-red.svg)

**Trabajo Fin de Máster** | Big Data & Data Engineering  
**Universidad Complutense de Madrid** | 2025-2026  
**Autor:** Michael Pinzas Villalta  
**Tutores:** Jorge Centeno y Alberto González

---

## Overview

This project implements a comprehensive Data Lakehouse platform for analyzing customer behavior in e-commerce, processing **113.3 million events** using a **Medallion architecture** (Bronze → Silver → Gold) pattern.

The system transforms 14GB of raw data into actionable business insights through a complete batch pipeline implemented with **Apache Spark** and **Parquet**, optimized for consumer-grade hardware (16GB RAM).

### Key Features

- Distributed processing of 113.3M events using PySpark
- Three-layer Medallion architecture (Bronze → Silver → Gold)
- Efficient compression: 14GB CSV → 4GB Parquet (-71%)
- Quality validation at each layer with automated metrics
- Granular temporal analysis (daily and hourly)
- Five critical business insights with quantitative support

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    RAW DATA (Kaggle CSV)                        │
│                         14GB, 109.9M                            │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  BRONZE LAYER (Raw Data)                        │
│   • 109.9M records preserved without transformations            │
│   • Format: Parquet partitioned by date                         │
│   • Storage: ~4GB compressed (-71% vs CSV)                      │
│   • Traceability: 100% original records                         │
└────────────────────────┬────────────────────────────────────────┘
                         │ PySpark Transformations
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              SILVER LAYER (Cleaned & Enriched)                  │
│   • 113.3M validated and enriched records                       │
│   • Deduplication: -3.3M exact duplicates                       │
│   • Enrichment: temporal features + categories                  │
│   • Validations: ranges, types, nulls, distributions            │
└────────────────────────┬────────────────────────────────────────┘
                         │ PySpark Aggregations
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│          GOLD LAYER (Business Analytics - 3 Fact Tables)        │
│                                                                 │
│   fact_daily_metrics (62 records)                               │
│      • Daily aggregated metrics Oct-Nov 2019                    │
│      • KPIs: conversion_rate, revenue, AOV, abandonment         │
│                                                                 │
│   fact_hourly_metrics (1,464 records)                           │
│      • 24/7 hourly pattern analysis                             │
│      • Golden hours identification (2-4 AM)                     │
│                                                                 │
│   fact_category_metrics (14 categories)                         │
│      • Performance by product line                              │
│      • Top performers and improvement opportunities             │
└─────────────────────────────────────────────────────────────────┘
                         │
                         ▼
                  BUSINESS INSIGHTS
```

---

## Key Results

Processing **113.3 million events** over **62 days** (October-November 2019) revealed **five critical insights**:

### 1. More Traffic ≠ Better Conversion
- **November:** +66.8% traffic vs October → Conversion **-19%** (1.47% vs 1.82%)
- **Conclusion:** Massive campaigns attracted low-quality users

### 2. Early Deals Outperform Black Friday
- **Nov 15-17:** $84.4M revenue (29% of month)
- **Black Friday:** Only $9.3M revenue (**5.8x less**)
- **Conclusion:** Pre-sale cannibalized traditional event

### 3. Users Purchase at Dawn
- **Golden Hours (2-4 AM):** 42.2% of purchases, 2.17% conversion
- **Morning (6-12h):** 37.4% traffic, only 32.9% purchases
- **Conclusion:** Users browse during day, decide at night

### 4. Electronics Dominates Business
- **Revenue:** $393.9M (**75.6%** of total)
- **Conversion:** 2.48% (2.3x better than average)
- **Conclusion:** Business is tech store, not generic marketplace

### 5. Critical Cart Abandonment
- **Average:** 57.75% abandonment
- **Worst:** Country Yard (66.9%)
- **Best:** Kids (49.5%)
- **Conclusion:** Opportunity to recover ~$300M in revenue

---

## Quick Start

### Prerequisites

```bash
# Operating System
Windows 10/11 or Linux

# Software
Python 3.11.9
Java 17+ (for PySpark)
Git 2.x
16GB RAM (minimum recommended)
```

### Installation

```bash
# 1. Clone repository
git clone https://github.com/MichaelPinzas/tfm-ecommerce-analytics.git
cd tfm-ecommerce-analytics

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows

# 3. Install dependencies
pip install -r requirements.txt
```

### Pipeline Execution

```bash
# NOTE: This pipeline assumes you have the dataset downloaded
# Expected location: data/raw/2019-Oct.csv and data/raw/2019-Nov.csv

# Step 1: Bronze Layer (raw ingestion)
python src/ingestion/csv_to_bronze.py

# Step 2: Silver Layer (cleaning + enrichment)
python src/processing/bronze_to_silver.py

# Step 3: Gold Layer (3 fact tables)
python src/processing/silver_to_gold.py
python src/processing/silver_to_gold_hourly.py
python src/processing/silver_to_gold_category.py

# Validations (optional)
python src/validation/validate_gold_daily.py
python src/validation/validate_gold_hourly.py
python src/validation/validate_gold_category.py
```

### Execution Times (16GB RAM, 8 cores)

| Layer | Script | Time | Output |
|-------|--------|------|--------|
| Bronze | csv_to_bronze.py | ~5 min | 109.9M records |
| Silver | bronze_to_silver.py | ~15 min | 113.3M records |
| Gold Daily | silver_to_gold.py | ~3 min | 62 records |
| Gold Hourly | silver_to_gold_hourly.py | ~4 min | 1,464 records |
| Gold Category | silver_to_gold_category.py | ~3 min | 14 records |

**Total:** ~30 minutes for complete pipeline

---

## Project Structure

```
tfm-ecommerce-analytics/
├── src/                           # Source code
│   ├── ingestion/                 # Ingestion scripts
│   │   └── csv_to_bronze.py       # CSV → Bronze Layer
│   ├── processing/                # Transformations
│   │   ├── bronze_to_silver.py    # Bronze → Silver
│   │   ├── silver_to_gold.py      # Daily metrics
│   │   ├── silver_to_gold_hourly.py    # Hourly metrics
│   │   └── silver_to_gold_category.py  # Category metrics
│   ├── validation/                # Validation scripts
│   │   ├── validate_gold_daily.py
│   │   ├── validate_gold_hourly.py
│   │   └── validate_gold_category.py
│   └── utils/                     # Utilities
│       ├── config.py              # Centralized configuration
│       └── logger.py              # Structured logging
├── data/                          # Data (not included in Git)
│   ├── raw/                       # Original CSV files
│   ├── bronze/                    # Bronze layer (Parquet)
│   ├── silver/                    # Silver layer (Parquet)
│   └── gold/                      # Gold layer (3 fact tables)
├── notebooks/                     # Jupyter notebooks (analysis)
├── tests/                         # Unit tests
├── docker/                        # Docker configuration
├── docs/                          # Documentation
│   ├── BUSINESS_INSIGHTS.md       # Detailed analysis
│   └── TECHNICAL_NOTES.md         # Technical documentation
├── requirements.txt               # Python dependencies
├── docker-compose.yml             # Local orchestration
└── README.md                      # This file
```

---

## Technology Stack

### Core Technologies

| Component | Technology | Version | Purpose |
|-----------|------------|---------|---------|
| **Language** | Python | 3.11.9 | Primary development |
| **Big Data Engine** | Apache Spark (PySpark) | 3.5.0 | Distributed processing |
| **Storage Format** | Apache Parquet | - | Compressed columnar storage |
| **Runtime** | Java | 17 | JVM for Spark |

### Python Libraries

```
pyspark==3.5.0           # Spark engine
pandas==2.0.3            # Data analysis
pyarrow==14.0.1          # Parquet backend
kaggle==1.5.16           # Dataset download
pytest==7.4.3            # Unit testing
black==23.11.0           # Code formatting
flake8==6.1.0            # Linting
```

### Infrastructure

- **Development:** PyCharm IDE + Git
- **Execution:** Local (Windows 10, 16GB RAM)
- **Version Control:** GitHub
- **Documentation:** Markdown + Jupyter

---

## Dataset

### Source

**Kaggle:** [eCommerce behavior data from Multi Category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)

### Characteristics

| Attribute | Value |
|-----------|-------|
| **Total records** | 109,915,006 events |
| **Time period** | 2019-10-01 to 2019-11-30 (61 days) |
| **Downloaded size** | ~14GB (compressed CSV) |
| **Event types** | view, cart, purchase |
| **Product categories** | 14 level-1 categories |
| **Unique users** | ~15.1M (daily sum) |
| **Transactions** | 1.71M purchases |
| **Total revenue** | $520.7M USD |

### Data Schema

```python
event_time: timestamp     # Event timestamp (UTC)
event_type: string        # view | cart | purchase
product_id: long          # Unique product ID
category_id: long         # Hierarchical category ID
category_code: string     # electronics.smartphone
brand: string             # Product brand
price: double             # Price in USD
user_id: long             # Unique user ID
user_session: string      # Session UUID
```

---

## Methodology

### Iterative Development Approach

Development followed an iterative validation approach for each component:

1. **Bronze Layer** → Complete validation
2. **Silver Layer** → Complete validation
3. **Gold Layer** → Complete validation

### Quality Validations

Each layer implements automated validations:

#### Bronze Layer
- Record count validation vs expected
- Schema validation (9 columns)
- Duplicate detection
- Null analysis by column

#### Silver Layer
- Successful deduplication (3.3M removed)
- Value ranges (prices, dates)
- Event type distribution (96% views expected)
- Correct temporal features

#### Gold Layer
- Coherent business metrics
- Aggregation sums = Silver total
- Logical conversion rates (0-100%)
- Verified revenue calculations

---

## Lessons Learned

### Technical Challenges Overcome

1. **RAM Constraint (16GB)**
   - **Problem:** OOM error processing 14GB complete
   - **Solution:** Daily iterative processing
   - **Result:** 15 min/month vs total crash

2. **Data Quality**
   - **Problem:** 35M records without category_code
   - **Decision:** Maintain as "Unknown" (real dataset limitation)
   - **Result:** 10.4% revenue documented as limitation

3. **Storage Optimization**
   - **Problem:** 14GB CSV impossible to version
   - **Solution:** Compressed Parquet + .gitignore
   - **Result:** 4GB Parquet (-71% storage)

4. **Java Version**
   - **Problem:** PySpark 3.5 incompatible with Java 11
   - **Solution:** Upgrade to Java 17
   - **Result:** Full compatibility

### Best Practices Applied

- **Git Management:** Robust .gitignore, frequent commits, descriptive messages
- **Code Quality:** Docstrings, structured logging, type hints
- **Documentation:** Updated PROJECT_STATUS.md, inline comments
- **Testing:** Automated validations per layer with metrics
- **Reproducibility:** Versioned requirements.txt, parameterized scripts

---

## Future Improvements

### Short Term
- Interactive dashboard (Power BI / Tableau)
- Unit tests with pytest (80%+ coverage)
- Jupyter notebook with visualizations

### Medium Term
- CI/CD pipeline (GitHub Actions)
- Apache Airflow orchestration
- Complete containerization (Docker)

### Long Term
- Real-time streaming (Kafka + Spark Streaming)
- ML/AI (churn prediction, recommendations)
- Cloud deployment (AWS EMR / Databricks)

---

## References

### Dataset
- Kaggle: [eCommerce Behavior Data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
- Paper: REES46 Marketing Platform for E-commerce

### Technologies
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Parquet Format Specification](https://parquet.apache.org/docs/)
- [Medallion Architecture - Databricks](https://www.databricks.com/glossary/medallion-architecture)

### Academia
- **University:** Universidad Complutense de Madrid
- **Master's Program:** Big Data & Data Engineering
- **Academic Year:** 2025-2026

---

## Author

**Michael Pinzas Villalta**  
Master's Student in Big Data & Data Engineering  
Universidad Complutense de Madrid

Email: [Available on UCM profile]  
LinkedIn: [michael-pinzas](https://www.linkedin.com/in/michael-pinzas/)  
GitHub: [@MichaelPinzas](https://github.com/MichaelPinzas)

### TFM Advisors

**Jorge Centeno** - Academic Advisor  
**Alberto González** - Academic Advisor

---

## License

This project is part of a Master's Final Project (TFM) academic work.  
All rights reserved © 2026 Michael Pinzas Villalta

Source code is available for educational and reference purposes.

---

## Acknowledgments

- **Advisors:** Jorge Centeno and Alberto González for their guidance and mentorship
- **UCM:** Universidad Complutense de Madrid for technical training
- **Kaggle:** Community for providing quality datasets
- **Apache Foundation:** For exceptional open-source tools

---

<div align="center">

**If this project was useful to you, consider giving it a star**

Developed in Madrid, Spain

</div>
