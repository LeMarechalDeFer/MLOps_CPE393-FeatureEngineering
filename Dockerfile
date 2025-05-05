FROM apache/airflow:3.0.0

COPY requirements.txt .
RUN pip install -r requirements.txt


