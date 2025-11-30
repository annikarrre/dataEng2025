from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os, glob

DEFAULT_DATA_DIR = "/opt/airflow/datasets/sunshine"

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

def _get_spark_session():
    """Create Spark session configured for Iceberg"""
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("sunshine_iceberg_ingest") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg_catalog.type", "rest") \
        .config("spark.sql.catalog.iceberg_catalog.uri", "http://iceberg-rest:8181") \
        .config("spark.sql.catalog.iceberg_catalog.warehouse", "warehouse") \
        .config("spark.sql.catalog.iceberg_catalog.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
        .getOrCreate()
    
    return spark

def _get_clickhouse_client():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host="clickhouse", 
        port=8123, 
        username="default", 
        password="mysecret"
    )

def create_iceberg_tables(**context):
    """Create Iceberg tables if they don't exist using Spark SQL"""
    spark = _get_spark_session()
    
    # Use the catalog explicitly for all operations
    spark.sql("USE iceberg_catalog")
    
    # Create namespace if it doesn't exist
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")
        print("Created/verified namespace: bronze")
    except Exception as e:
        print(f"Namespace creation note: {e}")
    
    # Create sources table
    try:
        spark.sql("""
            CREATE TABLE IF NOT EXISTS bronze.sunshine_sources_iceberg (
                staid INT,
                souid INT,
                station_name STRING,
                country STRING,
                lat DOUBLE,
                lon DOUBLE,
                elevation INT,
                begin INT,
                end INT,
                parname STRING
            )
            USING iceberg
        """)
        print("Created/verified table: bronze.sunshine_sources_iceberg")
    except Exception as e:
        print(f"Sources table note: {e}")
    
    # Create daily table with partitioning by year
    try:
        spark.sql("""
            CREATE TABLE IF NOT EXISTS bronze.sunshine_daily_iceberg (
                staid INT,
                souid INT,
                date DATE,
                ss_raw INT,
                q_ss INT
            )
            USING iceberg
            PARTITIONED BY (years(date))
        """)
        print("Created/verified table: bronze.sunshine_daily_iceberg")
    except Exception as e:
        print(f"Daily table note: {e}")
    
    spark.stop()

def load_sources_to_iceberg(**context):
    """Load sources.txt into Iceberg table using PySpark"""
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
    
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
            
            # Skip if required fields are missing
            if staid is None or souid is None:
                continue
            
            station_name = parts[2] if parts[2] else None
            country = (parts[3] or "EE")[:2]
            lat = _to_float(parts[4])
            lon = _to_float(parts[5])
            elevation = _to_int(parts[6])
            begin = _to_int(parts[8])
            end = _to_int(parts[9])
            parname = parts[11] if parts[11] else None
            
            rows.append((staid, souid, station_name, country, lat, lon, elevation, begin, end, parname))
    
    if not rows:
        print("No data to load")
        return
    
    # Create Spark DataFrame
    spark = _get_spark_session()
    
    schema = StructType([
        StructField("staid", IntegerType(), False),
        StructField("souid", IntegerType(), False),
        StructField("station_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("elevation", IntegerType(), True),
        StructField("begin", IntegerType(), True),
        StructField("end", IntegerType(), True),
        StructField("parname", StringType(), True),
    ])
    
    df = spark.createDataFrame(rows, schema)
    
    # Use the catalog explicitly
    spark.sql("USE iceberg_catalog")
    
    # Check if table exists
    try:
        spark.sql("DESCRIBE TABLE bronze.sunshine_sources_iceberg")
        table_exists = True
    except:
        table_exists = False
    
    if table_exists:
        # Table exists - overwrite using SQL
        df.createOrReplaceTempView("temp_sources")
        spark.sql("INSERT OVERWRITE TABLE bronze.sunshine_sources_iceberg SELECT * FROM temp_sources")
    else:
        # Table doesn't exist - use SQL to create and insert
        df.createOrReplaceTempView("temp_sources")
        spark.sql("""
            CREATE TABLE bronze.sunshine_sources_iceberg
            USING iceberg
            AS SELECT * FROM temp_sources
        """)
    
    print(f"Loaded {len(rows)} rows into Iceberg sunshine_sources_iceberg")
    
    spark.stop()

def load_daily_to_iceberg(**context):
    """Load SS_SOUID*.txt files into Iceberg table using PySpark with upsert"""
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, IntegerType, DateType
    from datetime import datetime as dt
    
    data_dir = context["params"].get("data_dir", DEFAULT_DATA_DIR)
    files = glob.glob(os.path.join(data_dir, "SS_SOUID*.txt"))
    
    if not files:
        print("no SS_SOUID*.txt files found in", data_dir)
        return
    
    rows = []
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
                
                # Skip if required fields are missing
                if staid is None or souid is None or not d:
                    continue
                
                # Convert to date
                try:
                    date_obj = dt.strptime(d, "%Y%m%d").date()
                    if date_obj.year < 1900:
                        continue
                except:
                    continue
                
                ss_raw = _to_int(parts[3])
                q_ss = _to_int(parts[4])
                
                # Handle missing values
                if ss_raw == -9999:
                    ss_raw = None
                if q_ss == -9 or q_ss is None:
                    q_ss = None
                
                rows.append((staid, souid, date_obj, ss_raw, q_ss))
    
    if not rows:
        print("No data to load")
        return
    
    # Create Spark DataFrame
    spark = _get_spark_session()
    
    schema = StructType([
        StructField("staid", IntegerType(), False),
        StructField("souid", IntegerType(), False),
        StructField("date", DateType(), False),
        StructField("ss_raw", IntegerType(), True),
        StructField("q_ss", IntegerType(), True),
    ])
    
    new_df = spark.createDataFrame(rows, schema)
    
    # Use the catalog explicitly
    spark.sql("USE iceberg_catalog")
    
    # Check if table has data
    try:
        existing_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze.sunshine_daily_iceberg").collect()[0]['cnt']
        table_exists = True
    except:
        existing_count = 0
        table_exists = False
    
    if not table_exists:
        # First time - create table using SQL
        new_df.createOrReplaceTempView("temp_daily")
        spark.sql("""
            CREATE TABLE bronze.sunshine_daily_iceberg
            USING iceberg
            PARTITIONED BY (years(date))
            AS SELECT * FROM temp_daily
        """)
        print(f"Created table and inserted {len(rows)} rows into Iceberg sunshine_daily_iceberg (first load)")
    elif existing_count > 0:
        # Perform upsert using MERGE INTO
        new_df.createOrReplaceTempView("new_sunshine_data")
        
        spark.sql("""
            MERGE INTO bronze.sunshine_daily_iceberg t
            USING new_sunshine_data s
            ON t.staid = s.staid AND t.souid = s.souid AND t.date = s.date
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        print(f"Upserted {len(rows)} rows into Iceberg sunshine_daily_iceberg")
    else:
        # Table exists but empty - just insert
        new_df.createOrReplaceTempView("temp_daily")
        spark.sql("INSERT INTO TABLE bronze.sunshine_daily_iceberg SELECT * FROM temp_daily")
        print(f"Appended {len(rows)} rows into Iceberg sunshine_daily_iceberg")
    
    spark.stop()

def load_sources_to_clickhouse(**context):
    """Read from Iceberg using PyIceberg and load into ClickHouse bronze.sunshine_sources"""
    from pyiceberg.catalog import load_catalog
    import pandas as pd

    catalog = load_catalog(
        "default",
        **{
            "type": "rest",
            "uri": "http://iceberg-rest:8181",
            "warehouse": "warehouse",
        }
    )
    
    iceberg_table = catalog.load_table("bronze.sunshine_sources_iceberg")
    
    # Read data from Iceberg
    df = iceberg_table.scan().to_pandas()
    
    if df.empty:
        print("No data in Iceberg table")
        return

    # NUCLEAR OPTION: manually build each row ensuring correct types
    rows = []
    for _, row in df.iterrows():
        clean_row = [
            int(row['staid']) if pd.notna(row['staid']) else 0,
            int(row['souid']) if pd.notna(row['souid']) else 0,
            str(row['station_name']) if pd.notna(row['station_name']) else "",
            str(row['country']) if pd.notna(row['country']) else "",
            float(row['lat']) if pd.notna(row['lat']) else None,
            float(row['lon']) if pd.notna(row['lon']) else None,
            int(row['elevation']) if pd.notna(row['elevation']) else None,
            int(row['begin']) if pd.notna(row['begin']) else None,
            int(row['end']) if pd.notna(row['end']) else None,
            str(row['parname']) if pd.notna(row['parname']) else "",
        ]
        rows.append(clean_row)
    
    # Insert into ClickHouse - truncate first since sources is small
    client = _get_clickhouse_client()
    client.command("TRUNCATE TABLE IF EXISTS bronze.sunshine_sources")
    client.insert(
        "bronze.sunshine_sources",
        rows,
        column_names=["staid", "souid", "station_name", "country", 
                     "lat", "lon", "elevation", "begin", "end", "parname"]
    )
    
    print(f"Loaded {len(rows)} rows into ClickHouse bronze.sunshine_sources")

def load_daily_to_clickhouse(**context):
    """Read from Iceberg using PyIceberg and reload into ClickHouse bronze.sunshine_daily"""
    from pyiceberg.catalog import load_catalog
    import pandas as pd
    
    catalog = load_catalog(
        "default",
        **{
            "type": "rest",
            "uri": "http://iceberg-rest:8181",
            "warehouse": "warehouse",
        }
    )
    
    iceberg_table = catalog.load_table("bronze.sunshine_daily_iceberg")
    
    # Read data from Iceberg
    df = iceberg_table.scan().to_pandas()
    
    if df.empty:
        print("No data in Iceberg table")
        return
    
    client = _get_clickhouse_client()
    
    # Truncate and reload all from Iceberg (Iceberg is source of truth)
    client.command("TRUNCATE TABLE IF EXISTS bronze.sunshine_daily")
    
    # NUCLEAR OPTION: manually build each row ensuring correct types
    batch_size = 50000
    total_rows = 0
    batch_rows = []
    
    for _, row in df.iterrows():
        clean_row = [
            int(row['staid']) if pd.notna(row['staid']) else 0,
            int(row['souid']) if pd.notna(row['souid']) else 0,
            row['date'] if pd.notna(row['date']) else None,
            int(row['ss_raw']) if pd.notna(row['ss_raw']) else None,
            int(row['q_ss']) if pd.notna(row['q_ss']) else None,
        ]
        batch_rows.append(clean_row)
        
        # Insert in batches
        if len(batch_rows) >= batch_size:
            client.insert(
                "bronze.sunshine_daily",
                batch_rows,
                column_names=["staid", "souid", "date", "ss_raw", "q_ss"]
            )
            total_rows += len(batch_rows)
            print(f"Inserted batch: {len(batch_rows)} rows (total: {total_rows})")
            batch_rows = []
    
    # Insert remaining rows
    if batch_rows:
        client.insert(
            "bronze.sunshine_daily",
            batch_rows,
            column_names=["staid", "souid", "date", "ss_raw", "q_ss"]
        )
        total_rows += len(batch_rows)
    
    print(f"Reloaded {total_rows} total rows from Iceberg into ClickHouse bronze.sunshine_daily")

def dq_check():
    """Data quality checks on ClickHouse bronze tables"""
    client = _get_clickhouse_client()
    
    bad_q = client.query(
        "SELECT count() FROM bronze.sunshine_daily WHERE q_ss NOT IN (0,1,9) AND q_ss IS NOT NULL"
    ).result_rows[0][0]
    
    bad_future = client.query(
        "SELECT count() FROM bronze.sunshine_daily WHERE date > today()"
    ).result_rows[0][0]
    
    total_rows = client.query("SELECT count() FROM bronze.sunshine_daily").result_rows[0][0]
    
    if bad_q > 0 or bad_future > 0:
        raise ValueError(f"DQ failed: invalid_q={bad_q}, future_dates={bad_future}")
    
    print(f"Sunshine DQ passed - Total rows: {total_rows}")

# DAG definition
with DAG(
    dag_id="bronze_sunshine_ingest_iceberg",
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "sunshine", "iceberg"],
    default_args={"owner": "you"},
    params={
        "data_dir": "/opt/airflow/datasets/sunshine",
    },
) as dag:
    
    # Step 1: Create Iceberg tables
    create_tables = PythonOperator(
        task_id="create_iceberg_tables",
        python_callable=create_iceberg_tables
    )
    
    # Step 2: Load raw data into Iceberg using PySpark
    load_sources_iceberg = PythonOperator(
        task_id="load_sources_to_iceberg",
        python_callable=load_sources_to_iceberg
    )
    
    load_daily_iceberg = PythonOperator(
        task_id="load_daily_to_iceberg",
        python_callable=load_daily_to_iceberg
    )
    
    # Step 3: Load from Iceberg to ClickHouse bronze (using PyIceberg for reading)
    load_sources_ch = PythonOperator(
        task_id="load_sources_to_clickhouse",
        python_callable=load_sources_to_clickhouse
    )
    
    load_daily_ch = PythonOperator(
        task_id="load_daily_to_clickhouse",
        python_callable=load_daily_to_clickhouse
    )
    
    # Step 4: Data quality checks
    dq = PythonOperator(
        task_id="dq_check",
        python_callable=dq_check
    )
    
    # Task dependencies
    create_tables >> [load_sources_iceberg, load_daily_iceberg]
    load_sources_iceberg >> load_sources_ch
    load_daily_iceberg >> load_daily_ch
    [load_sources_ch, load_daily_ch] >> dq
    t1 = PythonOperator(task_id="load_sources", python_callable=load_sources)
    t2 = PythonOperator(task_id="load_daily", python_callable=load_daily)
    t3 = PythonOperator(task_id="dq_check", python_callable=dq_check)
    t1 >> t2 >> t3
