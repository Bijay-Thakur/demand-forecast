# Retail Demand Forecasting — End-to-End ML Pipeline

> Predicting weekly SKU demand for a grocery store using real invoice data, time-series feature engineering, and a reproducible ML pipeline.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Problem Statement](#2-problem-statement)
3. [Business Motivation](#3-business-motivation)
4. [Dataset Description](#4-dataset-description)
5. [Data Pipeline Architecture](#5-data-pipeline-architecture)
6. [Feature Engineering](#6-feature-engineering)
7. [Baseline Forecast Model](#7-baseline-forecast-model)
8. [Error Analysis Insights](#8-error-analysis-insights)
9. [Future Improvements](#9-future-improvements)
10. [Project Structure](#10-project-structure)
11. [How to Run the Project](#11-how-to-run-the-project)
12. [Key Learnings](#12-key-learnings)

---

## 1. Project Overview

This project is an end-to-end machine learning system for **weekly retail demand forecasting** built on real grocery store invoice data.

The pipeline covers every stage of a production-grade ML workflow:

- Raw invoice ingestion into a relational database
- SQL-based data modeling and weekly aggregation
- Time-series feature engineering with lag and rolling window features
- Train/test splitting that respects temporal order (no leakage)
- A naive baseline forecast as a performance benchmark
- Error analysis revealing the impact of promotions and discounts on forecast quality

The project is designed not just to produce predictions, but to **demonstrate an engineering-first approach to applied machine learning** — from raw data to actionable business insight.

---

## 2. Problem Statement

Given a grocery store's historical invoice data, predict the **demand (units sold) for each SKU in the next week**.

More formally:

> For each (SKU, week) pair, forecast `target_next_week` — the number of units that will be sold in the following week.

This is a **supervised regression problem** with a 1-week forecast horizon, evaluated using MAE and RMSE.

---

## 3. Business Motivation

Demand forecasting is a foundational problem in retail operations. Accurate forecasts directly impact:

| Business Area | Impact |
|---|---|
| Inventory management | Avoid overstock and stockouts |
| Reorder point calculation | Know when to reorder before running out |
| Promotional planning | Anticipate demand spikes during promotions |
| Supplier relationships | Plan purchase orders in advance |
| Cash flow | Reduce capital tied up in excess stock |

A grocery store operating without demand forecasting relies on manual judgment or simple rules. Even a modest improvement over a naive baseline can translate into measurable cost savings and reduced waste.

---

## 4. Dataset Description

The dataset is sourced from **real invoice records of a grocery store**, covering approximately 52 weeks of transaction history.

### Raw Data

- CSV invoice files, one per supplier delivery
- Each invoice contains multiple line items (one row per SKU)
- Fields include: invoice date, SKU ID, quantity, unit price, discount percentage

### Aggregated Data

After ingestion and SQL processing, the core working table (`core.sku_weekly`) provides one row per `(SKU, week)`:

| Column | Description |
|---|---|
| `week_start` | Monday of the sales week |
| `sku_id` | Product identifier |
| `units_week` | Total units sold that week |
| `avg_net_price` | Average net price paid (after discount) |
| `avg_discount_pct` | Average discount percentage applied |
| `promo_flag_week` | Binary flag: 1 if any promo in that week |

### Processed Dataset (after feature engineering)

- **Train set:** 259 rows across 17 SKUs
- **Test set:** 102 rows across 17 SKUs (last 6 weeks per SKU)
- **Features:** 14 columns including lag features, rolling means, calendar signals, and price/promo variables
- **Target:** `target_next_week` — next week's demand (1-week horizon)

> Only SKUs with at least 20 weeks of history are included to ensure meaningful lag and rolling features.

---

## 5. Data Pipeline Architecture

The pipeline follows a linear, reproducible sequence from raw files to a model-ready dataset.

```
Raw CSV Invoices
      |
      v
[src/ingestion/load_invoice_csv.py]
[src/ingestion/load_all_invoices.py]
      |
      v
PostgreSQL Database
  - invoices table
  - line_items table
      |
      v
[sql/02_build_weekly.sql]
      |
      v
core.sku_weekly (aggregated weekly SKU demand)
      |
      v
[src/features/make_dataset.py]
      |
      v
Feature Engineering + Train/Test Split
      |
      v
data/processed/train.parquet
data/processed/test.parquet
      |
      v
[src/models/baseline.py]
      |
      v
Baseline Evaluation + Error Analysis
```

### Key Design Decisions

- **PostgreSQL as the source of truth** — raw data is ingested once and queried as needed. This separates storage concerns from modeling concerns.
- **Parquet for processed data** — fast columnar storage for the feature-engineered datasets.
- **Per-SKU time split** — the last 6 weeks of each SKU's history are reserved for testing, preventing any future data from leaking into the training window.

---

## 6. Feature Engineering

All features are computed per SKU, sorted by week, to ensure there is no cross-SKU contamination.

### Lag Features

Capture recent demand history directly:

| Feature | Definition |
|---|---|
| `lag_1` | Demand from 1 week ago |
| `lag_2` | Demand from 2 weeks ago |
| `lag_4` | Demand from 4 weeks ago |

### Rolling Mean Features

Smooth short-term noise and reveal the underlying demand level:

| Feature | Definition |
|---|---|
| `roll_mean_4` | Rolling 4-week average of past demand |
| `roll_mean_8` | Rolling 8-week average of past demand |

> `shift(1)` is applied before rolling to ensure only past weeks enter the window — preventing data leakage.

### Calendar Features

Capture seasonal and periodic demand patterns:

| Feature | Definition |
|---|---|
| `month` | Calendar month (1–12) |
| `weekofyear` | ISO week number (1–52) |

### Price and Promotion Features

Capture the influence of commercial activity on demand:

| Feature | Definition |
|---|---|
| `avg_net_price` | Average price paid after discounts |
| `avg_discount_pct` | Average discount percentage |
| `promo_flag_week` | Binary flag for promotional weeks |

---

## 7. Baseline Forecast Model

### What Is a Naive Baseline?

A **naive (or persistence) forecast** is the simplest possible forecasting rule:

> Predict next week's demand to be equal to this week's demand.

Formally:

```
prediction(t+1) = actual(t)  →  i.e., prediction = lag_1
```

### Why a Baseline Matters

Establishing a baseline before training any machine learning model is a fundamental best practice in forecasting. It answers a critical question:

> **Is there any reason to build a more complex model?**

If a simple rule already predicts demand accurately, a more complex ML model must demonstrate that it provides meaningful additional value — both in predictive accuracy and in the cost of building and maintaining it. Any ML model that fails to outperform the naive baseline adds complexity without benefit.

The baseline also:
- Sets an interpretable benchmark that non-technical stakeholders can understand
- Exposes the inherent difficulty of the forecasting problem
- Reveals which SKUs and conditions are hardest to predict

### Baseline Results

The naive baseline (`prediction = lag_1`) was evaluated on the held-out test set:

| Metric | Value |
|---|---|
| MAE | **0.059** |
| RMSE | **0.594** |

The low MAE reflects that demand for many SKUs in this dataset is relatively stable week-to-week. However, the RMSE being ~10x larger than MAE signals the presence of a few large errors — outlier weeks where the baseline fails significantly.

---

## 8. Error Analysis Insights

A dedicated error analysis notebook ([notebooks/02_error_analysis.ipynb](notebooks/02_error_analysis.ipynb)) investigates where and why the baseline fails.

### Finding 1: Demand is highly stable for most SKUs

The majority of SKUs in the test set show zero forecast error. Weekly demand patterns like `6, 6, 6, 6` or `1, 1, 1, 1` are common, meaning the naive rule is essentially perfect for those products.

### Finding 2: Promotions break the baseline

Error analysis by promo flag reveals a clear pattern:

| Promo Flag | Mean Absolute Error |
|---|---|
| No promotion (0) | **0.000** |
| Promotion (1) | **0.353** |

During non-promotional weeks, the baseline achieves near-perfect accuracy. During promotional weeks, errors increase significantly — meaning promotional activity alters demand in ways that last week's demand cannot predict.

### Finding 3: The largest error is associated with a discount event

The single worst-performing SKU (`1644764`) had its largest error during a week with a **35% discount** and an active `promo_flag_week = 1`. The baseline predicted 12 units based on prior demand, but actual demand shifted, resulting in an absolute error of 6 units.

This demonstrates a core retail forecasting challenge:

> When commercial conditions change (promotions, discounts), historical demand patterns are no longer reliable predictors of future demand.

### Finding 4: Intermittent demand is prevalent

Many SKUs exhibit **intermittent demand** — sparse, irregular sales with many zero or near-zero weeks. This is common in retail with large product catalogs. Intermittent demand is inherently harder to forecast and may benefit from specialized models or additional business signals.

### Conclusion

The baseline is a strong model for stable, non-promotional demand — but it systematically fails during promotional events. This creates a clear case for a machine learning model that can incorporate price, discount, and promo features as explicit inputs.

---

## 9. Future Improvements

### LightGBM Model

Train a gradient-boosted tree model using all engineered features. LightGBM is well-suited for tabular time-series data because it:
- Handles non-linear interactions between features (e.g., promo × discount)
- Is robust to the intermittent demand patterns present in this dataset
- Provides feature importances that are interpretable for business stakeholders

Expected training command:

```bash
python -m src.models.train_lgbm
```

### Hyperparameter Tuning with Optuna

Automate LightGBM hyperparameter search using Optuna with time-series cross-validation. Key parameters to tune: `num_leaves`, `learning_rate`, `min_child_samples`, `feature_fraction`.

### Reorder Point Calculation

Translate demand forecasts into actionable inventory recommendations:

```
Reorder Point = (Average Weekly Demand × Lead Time in Weeks) + Safety Stock
```

Where safety stock accounts for forecast uncertainty (e.g., 1–2 standard deviations of forecast error).

### Additional Improvements

- **More SKU history** — extend the dataset to cover 2+ years for stronger seasonality signals
- **External features** — add public holiday flags, weather data, or competitor pricing
- **Probabilistic forecasting** — produce prediction intervals rather than point estimates to better inform inventory decisions
- **Walk-forward validation** — implement rolling-window cross-validation for more robust evaluation

---

## 10. Project Structure

```
demand-forecasting/
│
├── data/
│   ├── raw/                    # Raw CSV invoice files (not tracked in git)
│   └── processed/              # Feature-engineered train/test parquet files
│
├── notebooks/
│   └── 02_error_analysis.ipynb # Baseline evaluation and error analysis
│
├── sql/
│   ├── 00_reset_and_reingest.sql   # Utility: drop and recreate all tables
│   ├── 01_create_tables.sql        # Schema: invoices, line_items, sku_weekly
│   └── 02_build_weekly.sql         # Aggregation: build core.sku_weekly
│
├── src/
│   ├── db/
│   │   └── connection.py           # SQLAlchemy engine factory
│   │
│   ├── ingestion/
│   │   ├── load_invoice_csv.py     # Parse and load a single invoice CSV
│   │   └── load_all_invoices.py    # Batch-load all invoices in data/raw/
│   │
│   ├── features/
│   │   ├── build_weekly.py         # Trigger SQL weekly aggregation
│   │   └── make_dataset.py         # Feature engineering + train/test split
│   │
│   └── models/
│       ├── baseline.py             # Naive forecast: prediction = lag_1
│       └── train_lgbm.py           # LightGBM model (in progress)
│
├── tests/                      # Unit tests
├── .env.example                # Environment variable template
├── requirements.txt            # Python dependencies
└── README.md
```

---

## 11. How to Run the Project

### Prerequisites

- Python 3.10+
- PostgreSQL running locally
- Raw invoice CSV files placed in `data/raw/`

### 1. Clone and install dependencies

```bash
git clone https://github.com/your-username/demand-forecasting.git
cd demand-forecasting
pip install -r requirements.txt
```

### 2. Configure environment variables

```bash
cp .env.example .env
# Edit .env and set your PostgreSQL credentials
```

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=demand_forecasting
DB_USER=postgres
DB_PASSWORD=your_password
```

### 3. Set up the database schema

```bash
psql -U postgres -d demand_forecasting -f sql/01_create_tables.sql
```

### 4. Ingest raw invoice data

```bash
python -m src.ingestion.load_all_invoices
```

### 5. Build weekly SKU aggregation

```bash
psql -U postgres -d demand_forecasting -f sql/02_build_weekly.sql
```

### 6. Run feature engineering and create the ML dataset

```bash
python -m src.features.make_dataset --min_weeks 20 --test_weeks 6
```

Output: `data/processed/train.parquet` and `data/processed/test.parquet`

### 7. Evaluate the baseline model

```bash
python -m src.models.baseline
```

### 8. Explore error analysis

Open the notebook:

```bash
jupyter notebook notebooks/02_error_analysis.ipynb
```

---

## 12. Key Learnings

**Data quality drives everything.** Real invoice data required significant cleaning — duplicate line item IDs, missing invoices, and irregular aggregation logic. Time spent on data modeling before feature engineering pays dividends across every downstream step.

**The baseline is not just a formality.** A naive forecast achieving MAE = 0.059 on this dataset is a meaningful result. It demonstrates that stable demand patterns dominate the dataset and sets a concrete bar that any ML model must clear to justify its complexity.

**Promotions are the hardest signal to capture.** The clearest finding from error analysis is that the baseline's failures concentrate almost entirely in promotional weeks. This suggests that `promo_flag_week` and `avg_discount_pct` will be among the most important features in a supervised model.

**Time-series splits are non-negotiable.** Using a random train/test split on time-series data would leak future information into training, producing falsely optimistic evaluation metrics. Per-SKU temporal splitting — using the last N weeks as the test set for each SKU — correctly simulates real forecasting conditions.

**Intermittent demand is a first-class problem.** A large portion of retail SKUs have sparse, irregular demand. Models and metrics designed for smooth, continuous demand may not translate directly to this setting. Recognizing this early shapes both model selection and evaluation strategy.

---

## Tech Stack

| Tool | Role |
|---|---|
| Python | Core language |
| Pandas | Data manipulation and feature engineering |
| NumPy | Numerical computation |
| PostgreSQL | Relational database and weekly aggregation |
| SQLAlchemy | Database connection management |
| LightGBM | ML model (planned) |
| Optuna | Hyperparameter tuning (planned) |
| Matplotlib | Visualization |
| Jupyter | Exploratory analysis and error analysis notebooks |

---

*Built as an end-to-end portfolio project demonstrating applied ML engineering on real retail data.*
