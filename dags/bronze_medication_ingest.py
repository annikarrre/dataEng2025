# Tervisekassa medications (monthly) → Iceberg bronze.medications_monthly

from datetime import datetime, date
from airflow import DAG
from airflow.operators.python import PythonOperator
import os, glob, re, unicodedata
import pandas as pd
import numpy as np
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, IntegerType, StringType, DateType, DoubleType
)
import pyarrow as pa

DEFAULT_DATA_DIR = "/opt/airflow/datasets/medications"
BATCH_SZ = 2_000

OUT_COLS = [
    "year","month","month_date","ingredient","package",
    "persons","prescriptions","packages_count",
    "total_amount","hk_amount","over_ref_price","copay_no_trh","copay_with_trh"
]

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

# ---------- Iceberg catalog connection ----------

def _get_catalog():
    """Connect to REST catalog"""
    catalog = load_catalog(
        "rest",
        **{
            "uri": "http://iceberg-rest:8181",
            "warehouse": "warehouse",
        }
    )
    return catalog

def _get_or_create_table(catalog, truncate=True):
    """Get or create the medications Iceberg table"""
    namespace = "bronze"
    table_name = "medications_monthly"
    full_name = f"{namespace}.{table_name}"
    
    # Create namespace if it doesn't exist
    try:
        catalog.create_namespace(namespace)
        print(f"[iceberg] Created namespace: {namespace}")
    except Exception as e:
        print(f"[iceberg] Namespace {namespace} already exists")
    
    # Define schema
    schema = Schema(
        NestedField(1, "year", IntegerType(), required=False),
        NestedField(2, "month", IntegerType(), required=False),
        NestedField(3, "month_date", DateType(), required=False),
        NestedField(4, "ingredient", StringType(), required=False),
        NestedField(5, "package", StringType(), required=False),
        NestedField(6, "persons", IntegerType(), required=False),
        NestedField(7, "prescriptions", IntegerType(), required=False),
        NestedField(8, "packages_count", DoubleType(), required=False),
        NestedField(9, "total_amount", DoubleType(), required=False),
        NestedField(10, "hk_amount", DoubleType(), required=False),
        NestedField(11, "over_ref_price", DoubleType(), required=False),
        NestedField(12, "copay_no_trh", DoubleType(), required=False),
        NestedField(13, "copay_with_trh", DoubleType(), required=False),
    )
    
    # Try to load existing table
    table_exists = False
    try:
        table = catalog.load_table(full_name)
        table_exists = True
        print(f"[iceberg] Loaded existing table: {full_name}")
        
        # Truncate if requested
        if truncate:
            print(f"[iceberg] Truncating table: {full_name}")
            # Delete all data files by overwriting with empty dataset
            table.overwrite(table.scan().to_arrow().slice(0, 0))
            print(f"[iceberg] Table truncated successfully")
            
    except Exception as e:
        print(f"[iceberg] Table {full_name} doesn't exist, creating new table...")
    
    # Create table if it doesn't exist
    if not table_exists:
        try:
            table = catalog.create_table(
                identifier=full_name,
                schema=schema
            )
            print(f"[iceberg] Created table: {full_name}")
        except Exception as e:
            # If creation fails, try loading again (race condition)
            print(f"[iceberg] Creation failed, trying to load existing table: {e}")
            table = catalog.load_table(full_name)
    
    return table

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
    try:
        val = float(s)
        iv = int(round(val))
        if 1 <= iv <= 12:
            return iv
    except Exception:
        pass
    s2 = s.lower().replace(".", "")
    if s2 in _ROMAN:
        return _ROMAN[s2]
    s3 = _strip_accents(s2)
    if s3 in _MONTH_NAME_MAP:
        return _MONTH_NAME_MAP[s3]
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
    df = pd.read_excel(path, sheet_name=0, header=0, engine="openpyxl")
    print(f"[meds] Raw columns: {list(df.columns)}")
    if df.empty:
        print("[meds][WARN] DataFrame empty after read_excel")
        return pd.DataFrame(columns=OUT_COLS)

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

    keep = list(required | {
        "persons","prescriptions","packages_count",
        "total_amount","hk_amount","over_ref_price","copay_no_trh","copay_with_trh"
    })
    keep = [c for c in keep if c in df.columns]
    df = df[keep].copy()

    before = len(df)
    df = df.dropna(subset=["year","month"])
    print(f"[meds] Rows before dropna(year,month)={before}, after={len(df)}")

    df["month_date"] = [_month_date(y, m) for y, m in zip(df["year"], df["month"])]
    df["month_num"] = [ _month_num(m) for m in df["month"] ]
    df = df[df["month_num"].notna()].copy()
    df["month"] = df["month_num"].astype(int)
    df = df.drop(columns=["month_num"])

    before_md = len(df)
    df = df.dropna(subset=["month_date"])
    print(f"[meds] Rows before dropna(month_date)={before_md}, after={len(df)}")

    if "persons" in df.columns:        df["persons"]        = df["persons"].map(_to_int)
    if "prescriptions" in df.columns:  df["prescriptions"]  = df["prescriptions"].map(_to_int)
    if "packages_count" in df.columns: df["packages_count"] = df["packages_count"].map(_to_float)
    for f in ["total_amount","hk_amount","over_ref_price","copay_no_trh","copay_with_trh"]:
        if f in df.columns:
            df[f] = df[f].map(_to_float)

    df["ingredient"] = df["ingredient"].map(lambda x: None if _norm(x)=="" else str(x))
    df["package"]    = df["package"].map(lambda x: None if _norm(x)=="" else str(x))

    for c in OUT_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[OUT_COLS]

    print(f"[meds] Final sample:\n{df.head(3)}")
    return df

# ---------- Iceberg write ----------

def _write_to_iceberg(table, df_batch):
    """Write a DataFrame batch to Iceberg table"""
    if df_batch.empty:
        return
    
    # Convert int64 columns to int32 to match Iceberg IntegerType schema
    int_cols = ['year', 'month', 'persons', 'prescriptions']
    for col in int_cols:
        if col in df_batch.columns and df_batch[col].notna().any():
            df_batch[col] = df_batch[col].astype('Int32')  # nullable int32
    
    # Convert DataFrame to PyArrow Table
    arrow_table = pa.Table.from_pandas(df_batch, preserve_index=False)
    
    # Append to Iceberg table
    table.append(arrow_table)
    print(f"[iceberg] Appended {len(df_batch)} rows to table")

# ---------- main load function ----------

def load_medications(**context):
    data_dir = context["params"].get("data_dir", DEFAULT_DATA_DIR)
    
    # Get Iceberg catalog and table
    catalog = _get_catalog()
    table = _get_or_create_table(catalog, truncate=True)
    
    files = sorted(glob.glob(os.path.join(data_dir, "*.xlsx")))
    if not files:
        print(f"[meds] No .xlsx files in {data_dir}")
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

        # Write in batches to Iceberg
        for start_idx in range(0, len(df), BATCH_SZ):
            end_idx = min(start_idx + BATCH_SZ, len(df))
            batch_df = df.iloc[start_idx:end_idx]
            _write_to_iceberg(table, batch_df)
            total += len(batch_df)

        print(f"[meds] Inserted rows from {os.path.basename(fp)}: {n}")

    print(f"[meds] All files done. Total rows written to Iceberg: {total}")

def dq_check():
    """Data quality check - now reads from Iceberg"""
    catalog = _get_catalog()
    table = catalog.load_table("bronze.medications_monthly")
    
    # Scan table and convert to pandas for checking
    scan = table.scan()
    arrow_table = scan.to_arrow()
    df = arrow_table.to_pandas()
    
    # Check data quality
    bad = len(df[
        (df['year'] < 2000) | (df['year'] > 2100) |
        (df['month'] < 1) | (df['month'] > 12) |
        (df['month_date'].isna())
    ])
    
    if bad:
        raise ValueError(f"Medications DQ failed: invalid rows={bad}")
    print(f"Medications DQ passed ✅ (checked {len(df)} rows)")


with DAG(
    dag_id="bronze_medication_ingest",
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    tags=["bronze","medications","iceberg"],
    default_args={"owner": "you"},
    params={
        "data_dir": "/opt/airflow/datasets/medications",
    },
):
    t1 = PythonOperator(task_id="load_medications", python_callable=load_medications)
    t2 = PythonOperator(task_id="dq_check", python_callable=dq_check)
    t1 >> t2
