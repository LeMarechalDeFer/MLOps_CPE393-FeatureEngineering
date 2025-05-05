# from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag

# import numpy as np
import pandas as pd
import os
from datetime import datetime
import matplotlib.pyplot as plt
import logging

# ==== Config ====
DATA_PATH = "/opt/airflow/data/TSM_1997-10-08_2025-04-22.csv"

# ==== Functions ====

def ingest_data():
    logging.info("Ingesting data")
    df = pd.read_csv(DATA_PATH, parse_dates=["date"])
    df.to_parquet("/opt/airflow/data/raw.parquet", index=False)


def clean_data():
    logging.info("Cleaning data")
    df = pd.read_parquet("/opt/airflow/data/raw.parquet")
    # Remove duplicates & critical NAs
    df = df.drop_duplicates(subset=["date"])
    df = df.dropna(subset=["open","high","low","close","volume"])
    # Filter non-positive volumes
    df = df[df["volume"] > 0]
    # Types & indexing
    df["date"] = pd.to_datetime(df["date"])
    df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].astype(float)
    df = df.sort_values("date").set_index("date")
    # Remove extreme outliers (1st-99th percentiles)
    for col in ["open","high","low","close","volume"]:
        q_low, q_high = df[col].quantile([0.01, 0.99])
        df = df[(df[col] >= q_low) & (df[col] <= q_high)]
    df.to_parquet("/opt/airflow/data/clean.parquet")


def engineer_features():
    logging.info("Engineering features")
    df = pd.read_parquet("/opt/airflow/data/clean.parquet")
    # Volatility
    df["volatility_abs"] = df["high"] - df["low"]
    df["volatility_pct"] = df["volatility_abs"] / df["low"]
    # Returns & rolling volatility
    df["return"] = df["close"].pct_change()
    df["vol_roll_std_7d"] = df["return"].rolling(7).std()
    # Moving averages
    df["sma_10"] = df["close"].rolling(10).mean()
    df["ema_10"] = df["close"].ewm(span=10).mean()
    df["ma_close_30d"] = df["close"].rolling(30).mean()
    df["ma_vol_7d"] = df["volume"].rolling(7).mean()
    # RSI (14j)
    delta = df["close"].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = -delta.clip(upper=0).rolling(14).mean()
    rs = gain / loss
    df["rsi_14"] = 100 - (100 / (1 + rs))
    # Lags
    df["lag_close_1d"] = df["close"].shift(1)
    df["lag_volume_1d"] = df["volume"].shift(1)
    # Seasonal features
    df["month"] = df.index.month
    df["weekday"] = df.index.weekday
    df.to_parquet("/opt/airflow/data/with_indicators.parquet")


def build_views():
    logging.info("Building views")
    df = pd.read_parquet("/opt/airflow/data/with_indicators.parquet")
    os.makedirs("/opt/airflow/data/views", exist_ok=True)
    # Daily trend view
    daily = df[["close","ma_close_30d","rsi_14"]].dropna().reset_index()
    daily.to_csv("/opt/airflow/data/views/daily_trend.csv", index=False)
    # Volatility spikes view
    mean_vol = df["volatility_abs"].mean()
    std_vol = df["volatility_abs"].std()
    spikes = df[["volatility_abs"]].copy()
    spikes["is_spike"] = spikes["volatility_abs"] > (mean_vol + 2 * std_vol)
    spikes = spikes.reset_index()
    spikes.to_csv("/opt/airflow/data/views/volatility_spikes.csv", index=False)
    # Weekly summary view
    weekly = df.resample("W").agg({
        "close": ["first","last","mean"],
        "volume": "sum",
        "return": "std",
        "volatility_abs": "max"
    })
    weekly.columns = ["_".join(col) for col in weekly.columns]
    weekly = weekly.reset_index()
    weekly.to_csv("/opt/airflow/data/views/weekly_summary.csv", index=False)
    # Monthly summary view
    monthly = df.resample("M").agg({
        "close": ["first","last","mean"],
        "volume": "sum",
        "return": "std"
    })
    monthly.columns = ["_".join(col) for col in monthly.columns]
    monthly = monthly.reset_index()
    monthly.to_csv("/opt/airflow/data/views/monthly_summary.csv", index=False)


def generate_visuals():
    logging.info("Generating visuals")
    os.makedirs("/opt/airflow/data/plots", exist_ok=True)
    # Daily price trend plot
    daily = pd.read_csv("/opt/airflow/data/views/daily_trend.csv", parse_dates=["date"], index_col="date")
    plt.figure()
    plt.plot(daily["close"], label="Close Price")
    plt.plot(daily["ma_close_30d"], label="MA 30d", linestyle="--")
    plt.title("Daily Close Price with MA30")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("/opt/airflow/data/plots/daily_price_trend.png")
    plt.close()
    # Volatility spikes plot
    spikes = pd.read_csv("/opt/airflow/data/views/volatility_spikes.csv", parse_dates=["date"], index_col="date")
    plt.figure()
    plt.plot(spikes["volatility_abs"], label="Volatility")
    plt.scatter(spikes[spikes["is_spike"]]["volatility_abs"].index, spikes[spikes["is_spike"]]["volatility_abs"], s=10, label="Spike")
    plt.title("Volatility Spikes")
    plt.xlabel("Date")
    plt.ylabel("Volatility")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("/opt/airflow/data/plots/volatility_spikes.png")
    plt.close()
    # Weekly volume plot
    weekly = pd.read_csv("/opt/airflow/data/views/weekly_summary.csv", parse_dates=["date"], index_col="date")
    plt.figure()
    plt.bar(weekly.index, weekly["volume_sum"])
    plt.title("Weekly Volume")
    plt.xlabel("Week")
    plt.ylabel("Total Volume")
    plt.tight_layout()
    plt.savefig("/opt/airflow/data/plots/weekly_volume.png")
    plt.close()

def create_target():
    logging.info("Creating target")
    df = pd.read_parquet("/opt/airflow/data/with_indicators.parquet")
    df["target_up"] = (df["close"].shift(-1) > df["close"]).astype(int)
    df = df.dropna()
    df.to_parquet("/opt/airflow/data/final_features.parquet")

def export_csv():
    logging.info("Exporting CSV")
    df = pd.read_parquet("/opt/airflow/data/final_features.parquet")
    df.to_csv("/opt/airflow/data/tsmc_features.csv", index=False)

@dag(
    start_date=datetime(2025,1,1),
    schedule="@once",
    catchup=False,
    description="Enhanced TSMC pipeline with views and visuals",
    tags=["tsmc","mlops"]
)
def tsmc_improved_prediction_pipeline():
    t1 = PythonOperator(task_id="ingest_data", python_callable=ingest_data)
    t2 = PythonOperator(task_id="clean_data", python_callable=clean_data)
    t3 = PythonOperator(task_id="engineer_features", python_callable=engineer_features)
    t4 = PythonOperator(task_id="build_views", python_callable=build_views)
    t5 = PythonOperator(task_id="generate_visuals", python_callable=generate_visuals)
    t6 = PythonOperator(task_id="create_target", python_callable=create_target)
    t7 = PythonOperator(task_id="export_csv", python_callable=export_csv)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7

dag = tsmc_improved_prediction_pipeline()
