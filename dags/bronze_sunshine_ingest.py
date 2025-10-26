from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os, glob

DEFAULT_DATA_DIR = "/opt/airflow/datasets/sunshine"

def _client():
    import clickhouse_connect
    return clickhouse_connect.get_client(host="clickhouse", port=8123, username="default", password="mysecret")

def _to_int(v, default=None):
    try:
        return int(v) if str(v).strip() != "" else default
    except Exception:
        return default

def _to_float(v, default=None):
    try:
        return float(v) if str(v).strip() != "" else default
    except Exception:
        return default

def load_sources(**context):
    data_dir = context["params"].get("data_dir", DEFAULT_DATA_DIR)
    path = os.path.join(data_dir, "sources.txt")
    if not os.path.exists(path):
        print("sources.txt not found:", path)
        return
    rows = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        header = False
        for ln in f:
            if not ln.strip() or ln.startswith("#"):
                continue
            if ("STAID" in ln and "SOUID" in ln and "PARNAME" in ln) and not header:
                header = True
                continue
            if not header:
                continue
            parts = [p.strip() for p in ln.split(",")]
            if len(parts) < 12:
                continue
            staid = _to_int(parts[0])
            souid = _to_int(parts[1])
            station_name = parts[2] or None
            country = (parts[3] or "EE")[:2]
            lat = _to_float(parts[4])
            lon = _to_float(parts[5])
            elevation = _to_int(parts[6])
            begin = _to_int(parts[8])
            end = _to_int(parts[9])
            parname = parts[11] or None
            rows.append([staid, souid, station_name, country, lat, lon, elevation, begin, end, parname])
    if rows:
        _client().insert(
            "bronze.sunshine_sources",
            rows,
            column_names=["staid","souid","station_name","country","lat","lon","elevation","begin","end","parname"]
        )

def load_daily(**context):
    data_dir = context["params"].get("data_dir", DEFAULT_DATA_DIR)
    files = glob.glob(os.path.join(data_dir, "SS_SOUID*.txt"))
    if not files:
        print("no SS_SOUID*.txt files found in", data_dir)
        return
    client = _client()
    batch = []
    for fp in files:
        with open(fp, "r", encoding="utf-8", errors="ignore") as f:
            header = False
            for raw in f:
                if not raw.strip() or raw.startswith("#"):
                    continue
                if ("STAID" in raw and "DATE" in raw and "Q_SS" in raw) and not header:
                    header = True
                    continue
                if not header:
                    continue
                parts = [p.strip() for p in raw.split(",")]
                if len(parts) < 5:
                    continue
                staid = _to_int(parts[0])
                souid = _to_int(parts[1])
                d = parts[2]
                # Convert to datetime.date (ClickHouse Date needs this)
                date_obj = datetime.strptime(d, "%Y%m%d").date()
                if date_obj.year < 1900:
                    continue
                ss_raw = _to_int(parts[3])
                q_ss = _to_int(parts[4])

                # ECAD missing values often use -9999; make them NULL if your table is Nullable
                if ss_raw == -9999:
                    ss_raw = None
                if q_ss == -9 or q_ss is None:
                    q_ss = None

                batch.append([staid, souid, date_obj, ss_raw, q_ss])

                if len(batch) >= 20000:
                    client.insert(
                        "bronze.sunshine_daily",
                        batch,
                        column_names=["staid","souid","date","ss_raw","q_ss"]
                    )
                    batch = []
    if batch:
        client.insert(
            "bronze.sunshine_daily",
            batch,
            column_names=["staid","souid","date","ss_raw","q_ss"]
        )

def dq_check():
    client = _client()
    bad_q = client.query("SELECT count() FROM bronze.sunshine_daily WHERE q_ss NOT IN (0,1,9) AND q_ss IS NOT NULL").result_rows[0][0]
    bad_future = client.query("SELECT count() FROM bronze.sunshine_daily WHERE date > today()").result_rows[0][0]
    if bad_q > 0 or bad_future > 0:
        raise ValueError(f"DQ failed: invalid_q={bad_q}, future_dates={bad_future}")
    print("Sunshine DQ passed")

from datetime import datetime as _dt
with DAG(
    dag_id="bronze_sunshine_ingest",
    start_date=_dt(2025, 10, 1),
    schedule=None,
    catchup=False,
    tags=["bronze","sunshine"],
    default_args={"owner": "you"},
    params={
        "data_dir": "/opt/airflow/datasets/sunshine",
    },
):
    t1 = PythonOperator(task_id="load_sources", python_callable=load_sources)
    t2 = PythonOperator(task_id="load_daily", python_callable=load_daily)
    t3 = PythonOperator(task_id="dq_check", python_callable=dq_check)
    t1 >> t2 >> t3
