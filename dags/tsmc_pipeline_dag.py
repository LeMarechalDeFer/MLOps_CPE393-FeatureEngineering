# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.decorators import dag

# import numpy as np
# from datetime import datetime
# import pandas as pd
# # import os

# # ==== Config ====
# DATA_PATH = "/opt/airflow/data/TSM_1997-10-08_2025-04-22.csv"
# EXPORT_PATH = "/opt/airflow/data/tsmc_features.csv"

# # ==== Functions ====
# def ingest_data():
#     df = pd.read_csv(DATA_PATH, parse_dates=['date'])
#     df.to_parquet("/opt/airflow/data/raw.parquet")


# def clean_data():
#     df = pd.read_parquet("/opt/airflow/data/raw.parquet")
#     df = df.dropna()
#     df = df[df['volume'] > 0]
#     df.to_parquet("/opt/airflow/data/clean.parquet")


# def calc_indicators():
#     df = pd.read_parquet("/opt/airflow/data/clean.parquet")
#     df['sma_10'] = df['close'].rolling(window=10).mean()
#     df['ema_10'] = df['close'].ewm(span=10).mean()
#     delta = df['close'].diff()
#     gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
#     loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
#     rs = gain / loss
#     df['rsi_14'] = 100 - (100 / (1 + rs))
#     df.to_parquet("/opt/airflow/data/with_indicators.parquet")


# def create_target():
#     df = pd.read_parquet("/opt/airflow/data/with_indicators.parquet")
#     df['target_up'] = (df['close'].shift(-1) > df['close']).astype(int)
#     df = df.dropna()
#     df.to_parquet("/opt/airflow/data/final_features.parquet")


# def export_csv():
#     df = pd.read_parquet("/opt/airflow/data/final_features.parquet")
#     df.to_csv(EXPORT_PATH, index=False)


# # ==== DAG Definition ====
# @dag(
#     start_date=datetime.now(),
#     schedule="@once",
#     catchup=False,
#     description="Feature engineering and target generation pipeline for TSMC data",
#     tags=["tsmc", "mlops"]
# )
# def tsmc_prediction_pipeline():
#     t1 = PythonOperator(task_id="ingest_data", python_callable=ingest_data)
#     t2 = PythonOperator(task_id="clean_data", python_callable=clean_data)
#     t3 = PythonOperator(task_id="calc_indicators", python_callable=calc_indicators)
#     t4 = PythonOperator(task_id="create_target", python_callable=create_target)
#     t5 = PythonOperator(task_id="export_csv", python_callable=export_csv)

#     t1 >> t2 >> t3 >> t4 >> t5

# dag = tsmc_prediction_pipeline()