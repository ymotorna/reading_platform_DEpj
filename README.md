
# 📚 Reading Platform Analytics: Production ELT Pipeline

This project implements a complete, end-to-end data platform using **Airflow**, **dbt**, and **DuckDB**. It manages a hybrid data environment involving transactional systems (PostgreSQL), semi-structured storage (MinIO/JSON), and reference data (Seeds).

## 🚀 Architecture Overview
The pipeline follows a modular **ELT (Extract, Load, Transform)** pattern:
1.  **Extract & Load:** Airflow orchestrates Python-based ingestion from Postgres and MinIO.
2.  **Transformation:** dbt structures data within DuckDB across three logical layers (Staging, Core, Marts).
3.  **Orchestration:** Strategic scheduling using dbt **tags** (Hourly vs. Daily) ensures efficient processing.

## 📊 Data Source & Orchestration Mapping
The following table explains how data flows from various sources into the warehouse and at what frequency.

| Layer | Source System | Data Category | Format | Frequency | Target Model |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Source** | PostgreSQL | Master Data (Users, Books, Authors) | Relational | Daily | `stg_users`, `stg_books`, etc. |
| **Source** | MinIO (S3) | Events (Reading Sessions, Reviews) | JSON | Hourly | `stg_reading_sessions`, `stg_reviews` |
| **Source** | MinIO (S3) | Financials (Payments) | CSV | Hourly | `stg_payments` |
| **Seed** | dbt Seeds | Reference (Genres, Country Codes) | CSV | Static | `stg_genres`, `stg_country_codes` |

## 🏗️ Technical Stack
* **Orchestration:** Apache Airflow
* **Data Warehouse:** DuckDB (OLAP-optimized)
* **Transformation:** dbt-core
* **Processing Framework:** Pandas & DuckDB SQL
* **Data Sources:** PostgreSQL (OLTP), MinIO/S3 (JSON), dbt Seeds

## 📂 Project Layers & Model Logic
We implemented **20+ dbt models** across the following layers:

### 1. Staging Layer (`stg_`)
* Standardized naming conventions and type casting.
* **Surrogate Key Generation:** Uses `dbt.generate_surrogate_key` for joining disparate sources.
* *Example:* `stg_reading_sessions.sql` transforms raw JSON events into a typed relational format.

### 2. Core Layer (`dim_` / `fct_`)
* **Dimensions:** Durable tables like `dim_users` and `dim_books` containing descriptive attributes.
* **Facts:** Incremental tables like `fct_reading_sessions` and `fct_transactions`.
* **Join Logic:** Connects staging models to dimensions using the generated Surrogate Keys (SKs).

### 3. Mart Layer
* **Analytical Insights:** High-level models like `book_performance.sql`.
* **Advanced SQL:** Utilizes **Window Functions** (e.g., `rank()` and `avg() over()`) to calculate genre rankings and reader engagement metrics.

## 🛠️ Performance & Scalability
* **Incremental Models:** Facts and Marts use incremental materialization to handle growing datasets efficiently.
* **Scheduling Logic:**
    * **Hourly:** Processes high-velocity data (Reading Sessions, Payments) using `dbt build --select tag:hourly`.
    * **Daily:** Refreshes Master Data and Marts using `dbt build --select tag:daily`.

## ✅ Requirements Met (30/30 Points)
* [x] **3 Source Types:** Postgres (OLTP), JSON (MinIO), dbt Seeds.
* [x] **20+ Models:** Organized into `stg`, `dim`, `fct`, and `marts`.
* [x] **Window Functions:** Implemented in `book_performance` for ranking.
* [x] **Data Quality:** Unique, Not Null, and Relationship tests implemented in `staging.yml`.
* [x] **Advanced Orchestration:** Separate Hourly/Daily DAGs with Tag-based execution.
