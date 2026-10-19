"""
Mini project: Weather ETL pipeline with Apache Airflow

Pipeline:  extract  >>  transform  >>  load  >>  report

- extract:   download hourly temperatures from the free Open-Meteo API
- transform: clean the raw JSON into simple rows
- load:      save the rows to a CSV file
- report:    print min / max / average temperature (see it in the task logs)
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ---------- Settings ----------
DATA_DIR = Path("/tmp/weather_pipeline")
RAW_FILE = DATA_DIR / "raw.json"
CLEAN_FILE = DATA_DIR / "clean.json"
OUTPUT_CSV = DATA_DIR / "weather.csv"

# Change latitude/longitude to any city you like
URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=51.5&longitude=-0.12&hourly=temperature_2m&forecast_days=1"
)


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


def load():
    """Save the clean rows to a CSV file."""
    rows = json.loads(CLEAN_FILE.read_text())
    with open(OUTPUT_CSV, "w") as f:
        f.write("time,temp_c,is_hot\n")
        for r in rows:
            f.write(f"{r['time']},{r['temp_c']},{r['is_hot']}\n")
    print(f"Wrote {OUTPUT_CSV}")


def report():
    """Print a quick summary. Check this in the task logs."""
    rows = json.loads(CLEAN_FILE.read_text())
    temps = [r["temp_c"] for r in rows]
    print(f"Min temp:     {min(temps):.1f} C")
    print(f"Max temp:     {max(temps):.1f} C")
    print(f"Average temp: {sum(temps) / len(temps):.1f} C")


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
    description="Mini ETL project: weather data",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:

    # ---------- Task definitions ----------
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load", python_callable=load)
    report_task = PythonOperator(task_id="report", python_callable=report)

    # A BashOperator example: show the first lines of the final CSV
    preview_task = BashOperator(
        task_id="preview_csv",
        bash_command=f"head -n 5 {OUTPUT_CSV}",
    )

    # ---------- Task pipeline ----------
    extract_task >> transform_task >> load_task >> [report_task, preview_task]