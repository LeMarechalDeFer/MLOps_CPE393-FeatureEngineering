# from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag

# import numpy as np
import pandas as pd
import os
from datetime import datetime
import matplotlib.pyplot as plt
import logging
import numpy as np
import matplotlib.dates as mdates
import seaborn as sns

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
   
    df = df.drop_duplicates(subset=["date"])
    df = df.dropna(subset=["open","high","low","close","volume"])
    df = df[df["volume"] > 0]

    df["date"] = pd.to_datetime(df["date"])
    df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].astype(float)
    df = df.sort_values("date").set_index("date")
    
    idx = pd.date_range(df.index.min(), df.index.max(), freq="B")
    df = df.reindex(idx)

    df[["open","high","low","close"]] = df[["open","high","low","close"]].interpolate()
    df["volume"] = df["volume"].fillna(0)
    
    df = df.dropna(subset=["open","close"])
    
    df.to_parquet("/opt/airflow/data/clean.parquet")


def engineer_features():
    logging.info("Engineering features")
    df = pd.read_parquet("/opt/airflow/data/clean.parquet")
    
    # Volatility
    df["volatility_abs"] = df["high"] - df["low"]
    df["volatility_pct"] = df["volatility_abs"] / df["low"]
  
    # Moving averages
    df["sma_10"] = df["close"].rolling(10).mean()
    df["ema_10"] = df["close"].ewm(span=10).mean()
    df["ma_close_30d"] = df["close"].rolling(30).mean()
    df["ma_vol_7d"] = df["volume"].rolling(7).mean()
    
    # RSI (14d)
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
    
    # MACD (12,26,9)
    exp12 = df["close"].ewm(span=12).mean()
    exp26 = df["close"].ewm(span=26).mean()
    df["macd"] = exp12 - exp26
    df["macd_signal"] = df["macd"].ewm(span=9).mean()

    # Bollinger Bands (20d)
    bb_mid = df["close"].rolling(20).mean()
    bb_std = df["close"].rolling(20).std()
    df["bb_upper"] = bb_mid + 2 * bb_std
    df["bb_lower"] = bb_mid - 2 * bb_std

    # ATR (14d)
    high_low = df["high"] - df["low"]
    high_prev = (df["high"] - df["close"].shift()).abs()
    low_prev  = (df["low"]  - df["close"].shift()).abs()
    tr = pd.concat([high_low, high_prev, low_prev], axis=1).max(axis=1)
    df["atr_14"] = tr.rolling(14).mean()

    # On-Balance Volume
    df["obv"] = (np.sign(df["close"].diff()) * df["volume"]).cumsum()
    
    # Temporal structure
    df["quarter"] = df.index.quarter
    df["day_of_year"] = df.index.dayofyear
    
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
        # "return": "std",
        "volatility_abs": "max"
    })
    weekly.columns = ["_".join(col) for col in weekly.columns]
    weekly = weekly.reset_index()
    weekly.to_csv("/opt/airflow/data/views/weekly_summary.csv", index=False)
    
    # Monthly summary view
    monthly = df.resample("M").agg({
        "close": ["first","last","mean"],
        "volume": "sum",
        # "return": "std"
    })
    monthly.columns = ["_".join(col) for col in monthly.columns]
    monthly = monthly.reset_index()
    monthly.to_csv("/opt/airflow/data/views/monthly_summary.csv", index=False)


def generate_visuals():
    logging.info("Generating visuals")
    os.makedirs("/opt/airflow/data/plots", exist_ok=True)
    
    # Daily price trend plot with improved time formatting
    daily = pd.read_csv("/opt/airflow/data/views/daily_trend.csv")
    logging.info(f"Available columns in daily: {daily.columns.tolist()}")
    date_col = daily.columns[0] 
    daily[date_col] = pd.to_datetime(daily[date_col])
    daily = daily.set_index(date_col)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(daily.index, daily["close"], label="Closing Price")
    ax.plot(daily.index, daily["ma_close_30d"], "--", label="MA30")
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=(1,4,7,10)))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.grid(True, which="major", alpha=0.3)
    ax.set_title("Daily Price Evolution with MA30")
    ax.set_xlabel("Date")
    ax.set_ylabel("Price")
    ax.legend()
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig("/opt/airflow/data/plots/daily_price_trend.png")
    plt.close()
    
    spikes = pd.read_csv("/opt/airflow/data/views/volatility_spikes.csv")
    logging.info(f"Available columns in spikes: {spikes.columns.tolist()}")
    date_col = spikes.columns[0] 
    spikes[date_col] = pd.to_datetime(spikes[date_col])
    spikes = spikes.set_index(date_col)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(spikes.index, spikes["volatility_abs"], label="Volatility")
    ax.scatter(spikes[spikes["is_spike"]].index, spikes[spikes["is_spike"]]["volatility_abs"], color='red', s=30, label="Volatility Spike")
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.grid(True, which="major", alpha=0.3)
    ax.set_title("Volatility Spikes")
    ax.set_xlabel("Date")
    ax.set_ylabel("Volatility")
    ax.legend()
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig("/opt/airflow/data/plots/volatility_spikes.png")
    plt.close()
    
    
    # Weekly volume plot 
    weekly = pd.read_csv("/opt/airflow/data/views/weekly_summary.csv")
    logging.info(f"Available columns in weekly: {weekly.columns.tolist()}")
    date_col = weekly.columns[0] 
    weekly[date_col] = pd.to_datetime(weekly[date_col])
    weekly = weekly.set_index(date_col)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(weekly.index, weekly["volume_sum"], width=5) 
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.set_title("Continuous Weekly Volume")
    ax.set_xlabel("Week")
    ax.set_ylabel("Total Volume")
    ax.grid(True, axis='y', alpha=0.3)
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig("/opt/airflow/data/plots/weekly_volume.png")
    plt.close()
    
    
    # RSI
    daily = pd.read_csv("/opt/airflow/data/views/daily_trend.csv")
    logging.info(f"Available columns in daily (RSI): {daily.columns.tolist()}")

    date_col = daily.columns[0] 
    daily[date_col] = pd.to_datetime(daily[date_col])
    daily = daily.set_index(date_col)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(daily.index, daily["rsi_14"], label="RSI(14)")
    ax.axhline(y=70, color='r', linestyle='-', alpha=0.3)
    ax.axhline(y=30, color='g', linestyle='-', alpha=0.3)
    ax.fill_between(daily.index, 70, 100, color='r', alpha=0.1, label="Overbought")
    ax.fill_between(daily.index, 0, 30, color='g', alpha=0.1, label="Oversold")
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.set_title("Relative Strength Index (RSI)")
    ax.set_xlabel("Date")
    ax.set_ylabel("RSI")
    ax.set_ylim(0, 100)
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig("/opt/airflow/data/plots/rsi_zones.png")
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
