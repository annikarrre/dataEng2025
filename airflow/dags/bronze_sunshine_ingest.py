from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os, glob

import clickhouse_connect

DATA_DIR = "/opt/airflow/datasets/sunshine"

def ch():
    return clickhouse_connect.get_client(host="clickhouse", port=8123, username="default", password="")

def load_sources():
    path = os.path.join(DATA_DIR, "sources.txt")
    if not os.path.exists(path):
        print("sources.txt not found")
        return
    rows = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        header_found = False
        for ln in f:
            if not ln.strip() or ln.startswith("#"): 
                continue
            if ("STAID" in ln and "SOUID" in ln and "PARNAME" in ln) and not header_found:
                header_found = True
                continue
            if not header_found:
                continue
            parts = [p.strip() for p in ln.split(",")]
            if len(parts) < 12: 
                continue
            staid = int(parts[0]); souid = int(parts[1])
            station_name = parts[2]
            country = (parts[3] or "EE")[:2]
            lat, lon = parts[4], parts[5]
            elevation = int(parts[6] or 0)
            begin = int(parts[8] or 0); end = int(parts[9] or 0)
            parname = parts[11]
            rows.append([staid, souid, station_name, country, lat, lon, elevation, begin, end, parname])

    if rows:
        ch().insert(
            "bronze.sunshine_sources",
            rows,
            column_names=["staid","souid","station_name","country","lat","lon","elevation","begin","end","parname"],
        )

def load_daily():
    files = glob.glob(os.path.join(DATA_DIR, "SS_SOUID*.txt"))
    if not files:
        print("No SS_SOUID*.txt files found")
        return
    client = ch()
    batch = []
    for fp in files:
        with open(fp, "r", encoding="utf-8", errors="ignore") as f:
            header_found = False
            for raw in f:
                if not raw.strip() or raw.startswith("#"): 
                    continue
                if ("STAID" in raw and "DATE" in raw and "Q_SS" in raw) and not header_found:
                    header_found = True
                    continue
                if not header_found:
                    continue
                parts = [p.strip() for p in raw.split(",")]
                if len(parts) < 5:
                    continue
                staid = int(parts[0]); souid = int(parts[1])
                d = parts[2]; date_iso = f"{d[0:4]}-{d[4:6]}-{d[6:8]}"
                ss_raw = int(parts[3]); q_ss = int(parts[4])
                batch.append([staid, souid, date_iso, ss_raw, q_ss])
                if len(batch) >= 20_000:
                    client.insert("bronze.sunshine_daily", batch,
                                  column_names=["staid","souid","date","ss_raw","q_ss"])
                    batch = []
    if batch:
        client.insert("bronze.sunshine_daily", batch,
                      column_names=["staid","souid","date","ss_raw","q_ss"])

def dq_check():
    client = ch()
    bad_q = client.query("SELECT count() FROM bronze.sunshine_daily WHERE q_ss NOT IN (0,1,9)").result_rows[0][0]
    bad_future = client.query("SELECT count() FROM bronze.sunshine_daily WHERE date > today()").result_rows[0][0]
    if bad_q > 0 or bad_future > 0:
        raise ValueError(f"DQ failed: invalid_q={bad_q}, future_dates={bad_future}")
    print("Sunshine DQ passed")

with DAG(
    dag_id="bronze_sunshine_ingest",
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    tags=["bronze","sunshine"],
) as dag:
    t1 = PythonOperator(task_id="load_sources", python_callable=load_sources)
    t2 = PythonOperator(task_id="load_daily", python_callable=load_daily)
    t3 = PythonOperator(task_id="dq_check", python_callable=dq_check)
    t1 >> t2 >> t3