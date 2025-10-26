# dags/bronze_weather_ingest.py
# Robust Excel → ClickHouse (bronze.weather_hourly) loader

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os, glob, re
import pandas as pd
import numpy as np


# ------------------ Config ------------------

DEFAULT_DATA_DIR = "/opt/airflow/datasets/weather"

# Memory controls
BATCH_SZ = 5_000
CHUNK_ROWS = 50_000
MAX_ROWS_PER_FILE = 1_200_000

# ------------------ ClickHouse client ------------------

def _client():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host="clickhouse", port=8123,
        username="default", password="mysecret"
    )

# ------------------ Cleaning helpers ------------------

def _norm(s):
    if s is None:
        return ""
    # normalize header strings (remove NBSP, collapse spaces)
    s = str(s).replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s.strip())
    return s

def _clean_wind_dir_int(series: pd.Series) -> pd.Series:
    """Return int in [0..360] or None; safe for Nullable(UInt16)."""
    def conv(v):
        if v is None or v is pd.NA or (isinstance(v, float) and np.isnan(v)):
            return None
        s = _norm(v)
        if s == "" or s.lower() in {"nan", "none", "null"}:
            return None
        try:
            val = float(s)  # read as float
        except Exception:
            return None
        if not np.isfinite(val):
            return None
        # Check range and convert to int
        return int(round(val)) if 0 <= val <= 360 else None

    # Do conversion
    s2 = series.astype(object).map(conv).astype(object)
    # Set data type
    return s2.astype("Int64")

def _to_python_scalars(df: pd.DataFrame) -> pd.DataFrame:
    """Convert pandas/NumPy nulls/scalars → plain Python types."""
    def _one(v):
        if v is pd.NA or (isinstance(v, float) and np.isnan(v)):
            return None
        if isinstance(v, np.generic):
            return v.item()
        return v
    for c in df.columns:
        df[c] = df[c].map(_one)
    return df

# ------------------ Column mapping / aliases ------------------

OUT_COLS = [
    "station","ts","t_air_c","t_min_c","t_max_c",
    "wind_dir_deg","wind_ms","wind_gust_ms","precip_mm"
]

# Canonical to possible header aliases (after _norm)
ALIASES = {
    "year": {"Aasta", "Year"},
    "month": {"Kuu", "Month"},
    "day": {"Päev", "Paev", "Day"},
    "time_utc": {
        "Kell (UTC)", "Kell(UTC)", "Kell", "Aeg (UTC)", "Time (UTC)", "Time",
        "Kell (UTC+0)", "Kell (UTC +0)"
    },
    "t_air_c": {"Õhutemperatuur °C", "Ohutemperatuur °C", "Air temperature °C", "Air temp °C", "T (°C)"},
    "t_min_c": {"Tunni miinimum õhutemperatuur °C", "Hourly min air temperature °C"},
    "t_max_c": {"Tunni maksimum õhutemperatuur °C", "Hourly max air temperature °C"},
    "wind_dir_deg": {"10 minuti keskmine tuule suund °", "10-min avg wind direction °", "WD (deg)"},
    "wind_ms": {"10 minuti keskmine tuule kiirus m/s", "10-min avg wind speed m/s", "WS (m/s)"},
    "wind_gust_ms": {"Tunni maksimum tuule kiirus m/s", "Hourly max wind speed m/s", "Gust (m/s)"},
    "precip_mm": {"Tunni sademete summa mm", "Hourly precipitation mm", "RR (mm)"},
}

# Minimum set to parse rows
MIN_KEYS = {"year", "month", "day", "time_utc"}

def _alias_reverse():
    rev = {}
    for canon, names in ALIASES.items():
        for n in names:
            rev[_norm(n)] = canon
    return rev

REV_ALIAS = _alias_reverse()

# ------------------ Header detection & sheet scan ------------------

def _find_header_and_sheet(xl: pd.ExcelFile):
    """
    Iterate sheets; for each, scan first ~120 rows to find a header row that
    contains at least 2 of the MIN_KEYS aliases. Return (sheet_name, header_row, columns_raw_list)
    """
    for sheet in xl.sheet_names:
        probe = pd.read_excel(xl, sheet_name=sheet, header=None, nrows=120, engine="openpyxl")
        for i in range(len(probe)):
            row_vals = [_norm(x) for x in probe.iloc[i].tolist()]
            # Count how many canonical keys we can map in this row
            mapped = set()
            for cell in row_vals:
                if cell in REV_ALIAS and REV_ALIAS[cell] in MIN_KEYS:
                    mapped.add(REV_ALIAS[cell])
            if len(mapped) >= 2:
                return sheet, i, row_vals  # plausible header
    return None, None, None

def _coerce_time_like(s):
    """Return hour 0-23 from various time representations, else None."""
    if s is None or s is pd.NA:
        return None
    s2 = _norm(s)
    if s2 == "":
        return None
    # try datetime.time in string form
    m = re.match(r"^(\d{1,2})(?::\d{1,2}(?::\d{1,2})?)?$", s2)
    if m:
        try:
            h = int(m.group(1))
            return h if 0 <= h <= 23 else None
        except Exception:
            pass
    # sometimes stored as number: 0..23 or 0.0..23.0
    try:
        v = float(s2)
        h = int(round(v))
        return h if 0 <= h <= 23 else None
    except Exception:
        return None

def _auto_detect_date_cols(df: pd.DataFrame):
    """
    If explicit aliases didn't match, infer columns by value patterns.
    Return dict mapping {canon_col: actual_col_name} or {} if not found.
    """
    candidates = {c: df[c].head(200) for c in df.columns}
    # Heuristics
    year_col = None
    month_col = None
    day_col = None
    time_col = None

    for c, s in candidates.items():
        try:
            vals = pd.to_numeric(s, errors="coerce")
            if (vals.dropna() >= 1900).mean() > 0.9 and (vals.dropna() <= 2100).mean() > 0.9:
                year_col = c if year_col is None else year_col
            if (vals.dropna() >= 1).mean() > 0.9 and (vals.dropna() <= 12).mean() > 0.9:
                month_col = c if month_col is None else month_col
            if (vals.dropna() >= 1).mean() > 0.9 and (vals.dropna() <= 31).mean() > 0.9:
                day_col = c if day_col is None else day_col
        except Exception:
            pass

    for c, s in candidates.items():
        hits = s.map(_coerce_time_like).dropna()
        if len(hits) >= max(5, int(len(s) * 0.2)):
            time_col = c
            break

    out = {}
    if year_col and month_col and day_col and time_col:
        out["year"] = year_col
        out["month"] = month_col
        out["day"] = day_col
        out["time_utc"] = time_col
    return out

# ------------------ Core parsing ------------------

def _read_one_xlsx(path: str, station: str) -> pd.DataFrame:
    xl = pd.ExcelFile(path, engine="openpyxl")
    sheet, header_row, _ = _find_header_and_sheet(xl)
    if sheet is None:
        print(f"[weather][WARN] No plausible header found in {os.path.basename(path)}; skipping.")
        return pd.DataFrame(columns=OUT_COLS)

    # Read with the detected header row
    df = pd.read_excel(xl, sheet_name=sheet, header=header_row, engine="openpyxl")

    # Normalize the read column names
    norm_cols = [_norm(c) for c in df.columns]
    raw_to_norm = {orig: nc for orig, nc in zip(df.columns, norm_cols)}
    df.columns = norm_cols

    # Map normalized headers to canonical names via aliases
    rename = {}
    for nc in norm_cols:
        if nc in REV_ALIAS:
            rename[nc] = REV_ALIAS[nc]
    df = df.rename(columns=rename)

    # If we still don't have all MIN_KEYS, try auto-detection
    if not MIN_KEYS.issubset(df.columns):
        detected = _auto_detect_date_cols(df)
        if detected:
            df = df.rename(columns=detected)

    # If still missing, give up on this file but don't crash DAG
    if not MIN_KEYS.issubset(df.columns):
        print(f"[weather][WARN] Missing required date columns in {os.path.basename(path)} "
              f"(have: {list(df.columns)}); skipping.")
        return pd.DataFrame(columns=OUT_COLS)

    # Restrict to likely columns; keep weather fields if present
    maybe_cols = {
        "year","month","day","time_utc",
        "t_air_c","t_min_c","t_max_c","wind_dir_deg","wind_ms","wind_gust_ms","precip_mm"
    }
    keep = [c for c in df.columns if c in maybe_cols]
    df = df[keep].copy()

    # Cap rows to avoid memory spikes
    if len(df) > MAX_ROWS_PER_FILE:
        df = df.iloc[:MAX_ROWS_PER_FILE].copy()

    # Build timestamp
    def _build_ts(y, m, d, t):
        try:
            yy = int(float(y)); mm = int(float(m)); dd = int(float(d))
            h = _coerce_time_like(t)
            if h is None:
                return None
            if not (1900 <= yy <= 2100):
                return None
            return datetime(yy, mm, dd, int(h))
        except Exception:
            return None

    df["ts"] = [
        _build_ts(y, m, d, t) for y, m, d, t in
        zip(df.get("year"), df.get("month"), df.get("day"), df.get("time_utc"))
    ]
    df = df.dropna(subset=["ts"])

    # Numeric coercions (coerce garbage → NaN)
    for f in ["t_air_c","t_min_c","t_max_c","wind_ms","wind_gust_ms","precip_mm"]:
        if f in df.columns:
            df[f] = pd.to_numeric(df[f], errors="coerce")

    if "wind_dir_deg" in df.columns:
        df["wind_dir_deg"] = _clean_wind_dir_int(df["wind_dir_deg"])

    # Station & final shape
    df["station"] = station

    # Ensure all expected columns exist and order them
    for c in OUT_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[OUT_COLS]

    # Plain Python scalars only
    df = _to_python_scalars(df)

    return df

_table_truncated = False

def _insert_chunk(client, rows):
    global _table_truncated
    if not rows:
        return
    if not _table_truncated:
        client.command("TRUNCATE TABLE bronze.weather_hourly")
        _table_truncated = True
    client.insert("bronze.weather_hourly", rows, column_names=OUT_COLS)

def load_weather(**context):
    data_dir = context["params"].get("data_dir", DEFAULT_DATA_DIR)
    client = _client()
    files = sorted(glob.glob(os.path.join(data_dir, "*.xlsx")))
    if not files:
        print(f"No .xlsx files in {data_dir}")
        return

    total_rows = 0
    for fp in files:
        station = os.path.basename(fp).split("-")[0]
        print(f"[weather] Reading {fp} → sheet auto-detect → station={station}")
        df = _read_one_xlsx(fp, station)
        n = len(df)
        if n == 0:
            print(f"[weather][WARN] 0 rows parsed from {os.path.basename(fp)}; skipped.")
            continue
        print(f"[weather] Parsed rows: {n}  (mem ~{df.memory_usage(deep=True).sum()/1e6:.1f} MB)")

        # Chunked insert
        for start in range(0, n, CHUNK_ROWS):
            end = min(start + CHUNK_ROWS, n)
            piece = df.iloc[start:end]

            batch = []

            for row in piece.itertuples(index=False, name=None):
                clean_row = []
                for col_name, val in zip(OUT_COLS, row):
                    if col_name == "wind_dir_deg":
                        if val is None or (isinstance(val, float) and np.isnan(val)):
                            clean_row.append(None)
                        else:
                            clean_row.append(int(val))
                    else:
                        clean_row.append(val)
                batch.append(clean_row)

                if len(batch) >= BATCH_SZ:
                    _insert_chunk(client, batch)
                    total_rows += len(batch)
                    batch = []
            if batch:
                _insert_chunk(client, batch)
                total_rows += len(batch)

        print(f"[weather] Inserted rows for {station}: {n}")

    print(f"[weather] All files done. Total rows inserted: {total_rows}")

def dq_check():
    client = _client()
    bad_future = client.query(
        "SELECT count() FROM bronze.weather_hourly WHERE ts > now()"
    ).result_rows[0][0]
    bad_temp = client.query(
        "SELECT count() FROM bronze.weather_hourly WHERE t_air_c < -70 OR t_air_c > 70"
    ).result_rows[0][0]
    if bad_future or bad_temp:
        raise ValueError(f"DQ failed: future={bad_future}, temp_out_of_range={bad_temp}")
    print("Weather DQ passed ✅")

# ------------------ DAG ------------------

with DAG(
    dag_id="bronze_weather_ingest",
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    tags=["bronze","weather"],
    default_args={"owner": "you"},
    params={
        "data_dir": "/opt/airflow/datasets/weather",
    },
):
    t1 = PythonOperator(task_id="load_weather", python_callable=load_weather)
    t2 = PythonOperator(task_id="dq_check", python_callable=dq_check)
    t1 >> t2
