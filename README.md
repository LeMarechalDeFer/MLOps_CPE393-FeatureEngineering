# CPE393 MLOps Lab – Feature Engineering Pipeline

## 1. Project Overview

**Dataset**: Taiwan Semiconductor Manufacturing Company (TSMC) historical stock prices
File: `TSM_1997-10-08_2025-04-22.csv` (≈ 70,000 rows)
Columns: `date`, `open`, `high`, `low`, `close`, `adj_close`, `volume`

**Objective**: Design and implement an **Airflow** workflow to clean raw data, engineer technical features, generate analytical views addressing business questions, and produce a dataset ready for machine learning.

---

## 2. Business Questions & Insights

1. **How does the closing price behave relative to its 30-day moving average?**

   * **Answer**: The `daily_trend.csv` and the `daily_price_trend.png` chart highlight crossovers between the closing price and the 30-day MA, indicating potential trend reversals useful for buy/sell signals.

2. **Which days exhibit abnormal volatility spikes?**

   * **Answer**: The `volatility_spikes.csv` identifies dates where absolute volatility exceeds mean + 2×standard deviation. These spikes often align with company earnings releases or macroeconomic events.

3. **What are the weekly trading volume patterns?**

   * **Answer**: The `weekly_summary.csv` and the `weekly_volume.png` visualization show weekly volume distributions, revealing periodic fluctuations around month-ends and market openings.

---

## 3. Pipeline Architecture

Each transformation is implemented as a `PythonOperator` in Airflow:

1. **ingest\_data**     – Read raw CSV and save as `raw.parquet`
2. **clean\_data**      – Remove duplicates, handle missing days (business-day reindex + interpolation), and save as `clean.parquet`
3. **engineer\_features** – Calculate technical indicators (volatility, moving averages, RSI, MACD, Bollinger Bands, ATR, OBV, seasonal features) and save as `with_indicators.parquet`
4. **build\_views**     – Aggregate data into analytical CSVs in `views/`:

   * `daily_trend.csv`, `volatility_spikes.csv`, `weekly_summary.csv`, `monthly_summary.csv`
5. **generate\_visuals** – Create plots in `plots/`:

   * `daily_price_trend.png`, `volatility_spikes.png`, `weekly_volume.png`, `rsi_zones.png`
6. **create\_target**   – Add binary `target_up` indicator and save as `final_features.parquet`
7. **export\_csv**      – Export final features to `tsmc_features.csv`

The DAG runs **once** for the initial full pipeline, then can be scheduled `@daily` for incremental updates.

---

## 4. Installation & Setup

1. **Clone the repository**

   ```bash
   git clone https://github.com/LeMarechalDeFer/MLOps_CPE393-FeatureEngineering
   cd MLOps_CPE393-FeatureEngineering
   ```

2. **Requirements**

   * docker
   * docker-compose

3. **Set environment variables**

   ```bash
   export AIRFLOW_UID=$(id -u)
   export AIRFLOW_IMAGE_NAME=apache/airflow:3.0.0
   export _AIRFLOW_WWW_USER_USERNAME=airflow
   export _AIRFLOW_WWW_USER_PASSWORD=airflow
   ```

4. **Deploy the DAG**

     ```bash
     docker-compose up --build --remove-orphans
     ```

5. **Access the Airflow UI**

   * http://localhost:8080/
   * http://localhost:5555/

---

## 5. Execution & Monitoring

* **Initial run**: Trigger the DAG manually in the Airflow UI to process the full historical dataset.
* **Daily updates**: Schedule the DAG to run daily to fetch new data and update features.
* **Monitoring**: Use the Airflow interface to check task status, logs, and rerun tasks if needed.

---

## 6. Project Structure

```
MLOps_CPE393-FeatureEngineering/
├── dags/
│   └── tsmc_improved_pipeline.py    
├── data/
│   ├── TSM_1997-10-08_2025-04-22.csv
│   ├── raw.parquet
│   ├── clean.parquet
│   ├── with_indicators.parquet
│   ├── final_features.parquet
│   ├── views/
│   │   ├── daily_trend.csv
│   │   ├── volatility_spikes.csv
│   │   ├── weekly_summary.csv
│   │   └── monthly_summary.csv
│   └── plots/
│       ├── daily_price_trend.png
│       ├── volatility_spikes.png
│       ├── weekly_volume.png
│       └── rsi_zones.png
└── README.md
```

---

## 7. Author

**Romain Blanchot**
CPE393 – MLOps Lab
May 2025
