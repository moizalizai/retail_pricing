# retail_pricing

> Built as a production-style Python + Azure ETL pipeline with analytics and prescriptive modeling layered on top.

# Inferring Retail Demand from Pricing Behavior
Using behavioral pricing signals to enable smarter promotions and margin protection.

## Overview
This project analyzes retail pricing behavior to infer demand signals when direct sales data is unavailable or delayed. It combines pricing strategy diagnostics with predictive modeling and prescriptive “what-if” simulations to support promotion planning.

## Business Problem
Retailers frequently adjust prices and promotions but often lack real-time demand visibility, creating risk of:
- blanket discounting
- margin erosion
- ineffective promotions

## Key Insights
- Retailers exhibit distinct pricing identities (e.g., EDLP-like stability vs. promotion-driven patterns).
- Pricing position relative to MSRP correlates with inferred demand strength.
- Uniform discounting is risky: simulated outcomes vary by SKU.

## Deliverables (Fast → Deep)
- **Virtual Poster (1-minute scan):** `docs/virtual_poster.pdf`
- **Consulting-style Deck (10–15 min):** `docs/consulting_slide-deck.pdf`
- **Research Report (full detail):** `docs/full_report.pdf`

## Approach (High Level)
1. Collect daily pricing data via retailer APIs (Walmart, eBay)
2. Build a complete end-to-end ETL data pipeline
3. Engineer behavioral pricing signals (stability, volatility, promotion intensity)
4. Infer demand strength using a composite indicator (DSI)
5. Train a predictive model to learn pricing–demand relationships
6. Run prescriptive simulations (e.g., +5% discount depth) to assess SKU-level response

## Repository Structure
- `docs/` → poster, deck, research report 
- `src/` → end-to-end pipeline scripts
- `data/` → data directories

## 🔧 End-to-End Data Engineering & ETL Pipeline (Technical Core)

This project was designed and implemented as a **production-style, end-to-end ETL pipeline**, not a one-off analysis.  
The pipeline mirrors real-world data engineering workflows used in analytics and consulting environments.

### Architecture Overview

- **Extract**  
  Automated Python ingestion from public retailer APIs (Walmart, eBay)

- **Transform**  
  Multi-stage data cleaning, normalization, and feature engineering

- **Load**  
  Analytics-ready datasets prepared for modeling, simulation, and visualization

- **Deploy / Store**  
  Cloud-ready design compatible with **Azure-based data architectures**

---

### Pipeline Design (Bronze → Silver → Gold)

The pipeline follows a layered architecture to ensure reproducibility, traceability, and scalability:

- **Bronze (Raw Ingestion)**
  - API data pulled via Python scripts with request signing, pagination handling, and schema validation
  - Raw data preserved to support reprocessing and auditability

- **Silver (Cleaned & Standardized)**
  - Deduplication, missing-value handling, and schema normalization
  - Cross-retailer SKU matching and taxonomy alignment
  - Pricing metrics engineered (discount depth, volatility, promotion intensity)

- **Gold (Analytics-Ready)**
  - Feature-complete datasets optimized for:
    - Demand inference modeling
    - Prescriptive pricing simulations
    - Visualization and executive reporting
  - Outputs exported in efficient formats (CSV / Parquet) for downstream consumption

---

### Technology Stack

- **Python**: ETL orchestration, API ingestion, data transformation, feature engineering  
- **Azure**: Cloud-oriented data pipeline design (Blob-compatible storage, scalable processing patterns)  
- **Data Formats**: CSV, Parquet  
- **Tooling**: Modular scripts, config-driven execution, environment isolation  

---

### Why This Matters

This pipeline was built to demonstrate the ability to:

- Design **scalable, cloud-ready data pipelines**
- Translate messy, real-world API data into **decision-grade datasets**
- Bridge **data engineering → analytics → prescriptive decision support**
- Work at the intersection of **technical depth and business impact**

> The analytics and simulations in this project are downstream products of the ETL system — the pipeline is the foundation.

---

### Reproducibility

All ETL steps are implemented as modular Python scripts.  
Raw data is not included in the repository, but the pipeline can be rerun end-to-end using the provided code structure and configuration templates.
