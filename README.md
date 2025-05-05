# CPE393 MLOps Lab – Feature Engineering Pipeline

## 1. Project Overview

**Dataset**: Taiwan Semiconductor Manufacturing Company (TSMC) historical prices
File: `TSM_1997-10-08_2025-04-22.csv` (≈ 70 000 rows)
Columns: `date`, `open`, `high`, `low`, `close`, `adj_close`, `volume`

**Goal**: Build an Airflow-orchestrated workflow that cleans the data, engineers new features, and produces analytical views answering two–three business-driven questions.

---