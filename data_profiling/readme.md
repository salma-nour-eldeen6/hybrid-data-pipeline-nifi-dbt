# Insurance Data Warehouse — EDA Overview

## Introduction

This project performs a full **Exploratory Data Analysis (EDA)** across a multi-layer data warehouse built using a **Medallion Architecture (Raw → Bronze → Silver → Gold)**.

The goal of this analysis is to:

* Understand data quality at each layer
* Track how data is transformed and improved
* Validate cleaning, enrichment, and modeling steps
* Generate business-ready insights in the Gold layer

---

## Data Architecture Overview

The system follows a layered architecture:

```
Raw Data (NiFi Ingestion)
        ↓
Bronze Layer (Raw structured data)
        ↓
Silver Layer (Cleaned + imputed + enriched)
        ↓
Gold Layer (Business models + KPIs + analytics)
```

---

## Data Model View

Below is the physical view of the database structure showing tables and relationships:

![Database Views](imgs/viewsInDB.png)

---

## EDA Breakdown by Layer

### 1. Raw Layer EDA (`raw_eda.sql`)

**Purpose:**

* Validate ingestion from NiFi
* Check completeness and schema correctness

**Key checks:**

* Row count consistency
* Null value detection
* Basic statistical distribution
* Duplicate detection

---

### 2. Bronze Layer EDA (`bronze_eda.sql`)

**Purpose:**

* Analyze structured raw data after initial loading
* Identify data quality issues before cleaning

**Key checks:**

* Null distribution per column
* Invalid value detection (e.g., negative income, invalid age)
* Categorical distributions
* Basic profiling of numeric fields

---

### 3. Silver Layer EDA (`silver_eda.sql`)

**Purpose:**

* Analyze cleaned and imputed dataset
* Evaluate transformations and feature engineering

**Key checks:**

* Post-imputation validation
* Segment distributions (risk, age groups, income tiers)
* Derived features (risk_category, lifestyle_profile)
* Data quality tiers impact

---

### 4. Gold Layer EDA (`gold_eda.sql`)

**Purpose:**

* Business-level analytics and KPI generation
* Final consumer-ready data exploration

**Key insights:**

* Risk tier segmentation
* Customer Lifetime Value (LTV)
* Premium pricing analysis
* Policy and geographic insights
* Revenue contribution by segments

---

## Key Analytical Themes

Across all layers, the analysis focuses on:

* Data quality progression
* Customer risk profiling
* Financial behavior (income, claims, premiums)
* Business segmentation (policy, geography, lifestyle)
* Revenue and LTV optimization

---

## Outcome

This EDA pipeline ensures:

* Trustworthy data pipeline from ingestion to analytics
* Clear visibility into transformation impact
* Business-ready datasets for decision making

---
 