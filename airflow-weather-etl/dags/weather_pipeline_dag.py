"""
Mini project: Weather ETL pipeline with Apache Airflow + Postgres

Pipeline:  extract  >>  transform  >>  load_to_db  >>  report  >>  done

- extract:     download hourly temperatures from the free Open-Meteo API
- transform:   clean the raw JSON into simple rows
- load_to_db:  save the rows into a Postgres table (weather_hourly)
- report:      query the table and print min / max / average temperature
- done:        a BashOperator that prints a finish message
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ---------- Settings ----------
DATA_DIR = Path("/tmp/weather_pipeline")
RAW_FILE = DATA_DIR / "raw.json"
CLEAN_FILE = DATA_DIR / "clean.json"

# Change latitude/longitude to any city you like
URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=-1.29&longitude=36.82&hourly=temperature_2m"
    "&forecast_days=1&timezone=auto"
)

# Connection details for the weather-db container (see docker-compose.override.yaml)
DB_CONFIG = {
    "host": "weather-db",
    "port": 5432,
    "dbname": "weather",
    "user": "weather",
    "password": "weather",
}


# ---------- Task functions ----------
def extract():
    """Download raw data and save it to a file."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    response = requests.get(URL, timeout=30)
    response.raise_for_status()
    RAW_FILE.write_text(response.text)
    print(f"Saved raw data to {RAW_FILE}")


def transform():
    """Turn the raw JSON into a clean list of rows."""
    raw = json.loads(RAW_FILE.read_text())
    times = raw["hourly"]["time"]
    temps = raw["hourly"]["temperature_2m"]

    rows = []
    for t, temp in zip(times, temps):
        if temp is None:  # skip missing values
            continue
        rows.append({"time": t, "temp_c": temp, "is_hot": temp > 25})

    CLEAN_FILE.write_text(json.dumps(rows))
    print(f"Cleaned {len(rows)} rows")


def load_to_db():
    """Save the clean rows into Postgres. Re-running will not create duplicates."""
    rows = json.loads(CLEAN_FILE.read_text())
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS weather_hourly (
                    time      TIMESTAMP PRIMARY KEY,
                    temp_c    DOUBLE PRECISION,
                    is_hot    BOOLEAN,
                    loaded_at TIMESTAMP DEFAULT NOW()
                )
                """
            )
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO weather_hourly (time, temp_c, is_hot)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (time) DO UPDATE
                    SET temp_c = EXCLUDED.temp_c,
                        is_hot = EXCLUDED.is_hot,
                        loaded_at = NOW()
                    """,
                    (r["time"], r["temp_c"], r["is_hot"]),
                )
        print(f"Loaded {len(rows)} rows into weather_hourly")
    finally:
        conn.close()


def report():
    """Query the table and print a quick summary. Check this in the task logs."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*), MIN(temp_c), MAX(temp_c), AVG(temp_c)
                FROM weather_hourly
                """
            )
            count, low, high, avg = cur.fetchone()
        print(f"Rows in table: {count}")
        print(f"Min temp:      {low:.1f} C")
        print(f"Max temp:      {high:.1f} C")
        print(f"Average temp:  {avg:.1f} C")
    finally:
        conn.close()


# ---------- DAG arguments ----------
default_args = {
    "owner": "your_name",
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# ---------- DAG definition ----------
with DAG(
    dag_id="weather_pipeline",
    description="Mini ETL project: weather data into Postgres",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:

    # ---------- Task definitions ----------
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load_to_db", python_callable=load_to_db)
    report_task = PythonOperator(task_id="report", python_callable=report)

    # A BashOperator example
    done_task = BashOperator(
        task_id="done",
        bash_command='echo "Weather pipeline finished successfully"',
    )

    # ---------- Task pipeline ----------
    extract_task >> transform_task >> load_task >> report_task >> done_task