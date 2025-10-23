# Tervisekassa medications (monthly) → ClickHouse bronze.medications_monthly

from datetime import datetime, date
from airflow import DAG
from airflow.operators.python import PythonOperator
import os, glob, re, unicodedata
import pandas as pd
import numpy as np

DATA_DIR = "/opt/airflow/datasets/medications"
BATCH_SZ = 2_000  # smaller batches to avoid memory spikes

OUT_COLS = [
    "year","month","month_date","ingredient","package",
    "persons","prescriptions","packages_count",
    "total_amount","hk_amount","over_ref_price","copay_no_trh","copay_with_trh"
]

# Exact headers (as in your workbook)
COL_MAP_EXACT = {
    "Aasta": "year",
    "Kuu": "month",
    "Toimeaine": "ingredient",
    "Pakend": "package",
    "Isikute arv": "persons",
    "Retseptide arv": "prescriptions",
    "Pakendite arv": "packages_count",
    "Retsepti kogusumma": "total_amount",
    "Tervisekassa kompenseeritud summa (ilma TRH)": "hk_amount",
    "Üle piirhinna": "over_ref_price",
    "Patsiendi omaosaluse summa (ilma TRH)": "copay_no_trh",
    "Patsiendi omaosaluse summa (sh TRH)": "copay_with_trh",
}

def _client():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host="clickhouse", port=8123,
        username="default", password="mysecret"
    )

# ---------- utils ----------

def _norm(s):
    if s is None:
        return ""
    s = str(s).replace("\xa0"," ").replace("\u2009"," ")
    s = re.sub(r"\s+"," ", s.strip())
    return s

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def _to_float(v):
    if v is None or v is pd.NA:
        return None
    try:
        s = _norm(v).replace("\u00A0","").replace(" ", "").replace(",", ".")
        if s == "" or s.lower() in {"nan","none","null"}:
            return None
        x = float(s)
        return float(x) if np.isfinite(x) else None
    except Exception:
        return None

def _to_int(v):
    f = _to_float(v)
    if f is None:
        return None
    try:
        i = int(round(f))
        return i if i >= 0 else None
    except Exception:
        return None

# ---------- robust month parsing ----------

_ROMAN = {"i":1,"ii":2,"iii":3,"iv":4,"v":5,"vi":6,"vii":7,"viii":8,"ix":9,"x":10,"xi":11,"xii":12}
_MONTH_NAME_MAP = {
    # Estonian
    "jaanuar":1, "jaan":1,
    "veebruar":2, "veebr":2, "vebr":2,
    "märts":3, "marts":3, "mrts":3,
    "aprill":4, "apr":4,
    "mai":5,
    "juuni":6, "juni":6, "jun":6,
    "juuli":7, "juli":7, "jul":7,
    "august":8, "aug":8,
    "september":9, "sept":9, "sep":9,
    "oktoober":10, "okt":10,
    "november":11, "nov":11,
    "detsember":12, "dets":12, "dec":12,
    # English safety net
    "january":1, "jan":1,
    "february":2, "feb":2,
    "march":3, "mar":3,
    "april":4, "apr":4,
    "may":5,
    "june":6, "jun":6,
    "july":7, "jul":7,
    "august":8, "aug":8,
    "september":9, "sep":9, "sept":9,
    "october":10, "oct":10,
    "november":11, "nov":11,
    "december":12, "dec":12,
}

def _month_num(m):
    if m is None or m is pd.NA:
        return None
    s = _norm(m)
    if s == "":
        return None
    # numeric like "1", "01", "1.0"
    try:
        val = float(s)
        iv = int(round(val))
        if 1 <= iv <= 12:
            return iv
    except Exception:
        pass
    # roman numerals
    s2 = s.lower().replace(".", "")
    if s2 in _ROMAN:
        return _ROMAN[s2]
    # names (accent-insensitive)
    s3 = _strip_accents(s2)
    if s3 in _MONTH_NAME_MAP:
        return _MONTH_NAME_MAP[s3]
    # fallback: trailing number (e.g., "2024-12")
    m2 = re.search(r"(\d{1,2})$", s3)
    if m2:
        iv = int(m2.group(1))
        if 1 <= iv <= 12:
            return iv
    return None

def _month_date(y, m):
    try:
        yy = int(float(y))
        mm = _month_num(m)
        if mm is None:
            return None
        if 1900 <= yy <= 2100:
            return date(yy, mm, 1)
    except Exception:
        pass
    return None

# ---------- parsing ----------

def _read_one_xlsx(path: str) -> pd.DataFrame:
    # first sheet, header row 0 (confirmed)
    df = pd.read_excel(path, sheet_name=0, header=0, engine="openpyxl")
    print(f"[meds] Raw columns: {list(df.columns)}")
    if df.empty:
        print("[meds][WARN] DataFrame empty after read_excel")
        return pd.DataFrame(columns=OUT_COLS)

    # rename to canonical
    norm_to_canon = { _norm(k): v for k, v in COL_MAP_EXACT.items() }
    rename = {}
    for c in df.columns:
        nc = _norm(c)
        if c in COL_MAP_EXACT:
            rename[c] = COL_MAP_EXACT[c]
        elif nc in norm_to_canon:
            rename[c] = norm_to_canon[nc]
    df = df.rename(columns=rename)
    print(f"[meds] After rename: {list(df.columns)}")

    required = {"year","month","ingredient","package"}
    if not required.issubset(df.columns):
        print(f"[meds][WARN] Missing required columns. Have: {list(df.columns)}")
        return pd.DataFrame(columns=OUT_COLS)

    # keep relevant columns
    keep = list(required | {
        "persons","prescriptions","packages_count",
        "total_amount","hk_amount","over_ref_price","copay_no_trh","copay_with_trh"
    })
    keep = [c for c in keep if c in df.columns]
    df = df[keep].copy()

    # drop obvious empties
    before = len(df)
    df = df.dropna(subset=["year","month"])
    print(f"[meds] Rows before dropna(year,month)={before}, after={len(df)}")

    # build month_date and make month numeric (UInt8 safe)
    df["month_date"] = [_month_date(y, m) for y, m in zip(df["year"], df["month"])]
    df["month_num"] = [ _month_num(m) for m in df["month"] ]
    df = df[df["month_num"].notna()].copy()
    df["month"] = df["month_num"].astype(int)
    df = df.drop(columns=["month_num"])

    before_md = len(df)
    df = df.dropna(subset=["month_date"])
    print(f"[meds] Rows before dropna(month_date)={before_md}, after={len(df)}")

    # numeric coercions
    if "persons" in df.columns:        df["persons"]        = df["persons"].map(_to_int)
    if "prescriptions" in df.columns:  df["prescriptions"]  = df["prescriptions"].map(_to_int)
    if "packages_count" in df.columns: df["packages_count"] = df["packages_count"].map(_to_float)
    for f in ["total_amount","hk_amount","over_ref_price","copay_no_trh","copay_with_trh"]:
        if f in df.columns:
            df[f] = df[f].map(_to_float)

    # strings
    df["ingredient"] = df["ingredient"].map(lambda x: None if _norm(x)=="" else str(x))
    df["package"]    = df["package"].map(lambda x: None if _norm(x)=="" else str(x))

    # ensure all output cols exist & order them
    for c in OUT_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[OUT_COLS]

    print(f"[meds] Final sample:\n{df.head(3)}")
    return df

def _insert_chunk(client, rows):
    if not rows:
        return
    client.insert("bronze.medications_monthly", rows, column_names=OUT_COLS)

def load_medications():
    client = _client()
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.xlsx")))
    if not files:
        print(f"[meds] No .xlsx files in {DATA_DIR}")
        return

    total = 0
    for fp in files:
        print(f"[meds] Reading {os.path.basename(fp)} (sheet=0, header=0)")
        df = _read_one_xlsx(fp)
        n = len(df)
        print(f"[meds] Parsed rows: {n}")
        if n == 0:
            print(f"[meds][WARN] 0 rows parsed from {os.path.basename(fp)}; skipping.")
            continue

        batch = []
        for row in df.itertuples(index=False, name=None):
            batch.append(list(row))
            if len(batch) >= BATCH_SZ:
                _insert_chunk(client, batch)
                total += len(batch)
                batch = []
        if batch:
            _insert_chunk(client, batch)
            total += len(batch)

        print(f"[meds] Inserted rows from {os.path.basename(fp)}: {n}")

    print(f"[meds] All files done. Total rows inserted: {total}")

def dq_check():
    client = _client()
    bad = client.query("""
        SELECT count()
        FROM bronze.medications_monthly
        WHERE (year < 2000 OR year > 2100)
           OR (month < 1 OR month > 12)
           OR month_date IS NULL
    """).result_rows[0][0]
    if bad:
        raise ValueError(f"Medications DQ failed: invalid rows={bad}")
    print("Medications DQ passed ✅")

with DAG(
    dag_id="bronze_medication_ingest",  # match your filename / trigger
    start_date=datetime(2025, 10, 1),   # <-- FIX: datetime, not date
    schedule=None,
    catchup=False,
    tags=["bronze","medications"],
    default_args={"owner": "you"},
):
    t1 = PythonOperator(task_id="load_medications", python_callable=load_medications)
    t2 = PythonOperator(task_id="dq_check", python_callable=dq_check)
    t1 >> t2
