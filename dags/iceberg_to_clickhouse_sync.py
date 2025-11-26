from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyiceberg.catalog import load_catalog
import clickhouse_connect
import pyarrow as pa

# Define dataset for triggering downstream DAGs
medications_clickhouse_dataset = Dataset("clickhouse://bronze/medications_monthly")

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'iceberg_to_clickhouse_sync',
    default_args=default_args,
    description='Sync Iceberg bronze layer to ClickHouse',
    schedule_interval=None,  # Manual or triggered by upstream
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bronze', 'iceberg', 'clickhouse'],
) as dag:

    @task
    def create_clickhouse_database():
        """Create ClickHouse bronze database if it doesn't exist"""
        client = clickhouse_connect.get_client(
            host='clickhouse',
            port=8123,
            username='default',
            password='mysecret'
        )
        
        try:
            client.command('CREATE DATABASE IF NOT EXISTS bronze')
            print("✅ Database 'bronze' created or already exists")
        except Exception as e:
            print(f"❌ Error creating database: {str(e)}")
            raise
        finally:
            client.close()
        
        return True

    @task(outlets=[medications_clickhouse_dataset])
    def create_clickhouse_table():
        """Create ClickHouse table if it doesn't exist"""
        client = clickhouse_connect.get_client(
            host='clickhouse',
            port=8123,
            username='default',
            password='mysecret',
            database='bronze'
        )
        
        try:
            # Drop table if exists (for clean reloads)
            client.command('DROP TABLE IF EXISTS bronze.medications_monthly')
            
            # Create the table
            create_table_sql = """
            CREATE TABLE bronze.medications_monthly
            (
                year Int32,
                month UInt8,
                month_date Date,
                ingredient String,
                package String,
                persons Int32,
                prescriptions Int32,
                packages_count Float64,
                total_amount Float64,
                hk_amount Float64,
                over_ref_price Float64,
                copay_no_trh Float64,
                copay_with_trh Float64
            )
            ENGINE = MergeTree()
            ORDER BY (year, month, ingredient)
            """
            
            client.command(create_table_sql)
            print("✅ ClickHouse table created successfully")
        except Exception as e:
            print(f"❌ Error creating table: {str(e)}")
            raise
        finally:
            client.close()
        
        return True

    @task(outlets=[medications_clickhouse_dataset])
    def load_iceberg_to_clickhouse():
        """Load data from Iceberg to ClickHouse"""
        
        try:
            # Initialize Iceberg catalog using REST catalog
            catalog = load_catalog(
                name="rest",
                **{
                    "uri": "http://iceberg-rest:8181",
                    "warehouse": "warehouse",
                }
            )
            
            # Load the Iceberg table
            table = catalog.load_table("bronze.medications_monthly")
            
            # Read data as PyArrow table
            print("📖 Reading data from Iceberg table...")
            arrow_table = table.scan().to_arrow()
            print(f"✅ Read {len(arrow_table)} rows from Iceberg")
            
            # Convert to pandas for ClickHouse insert
            df = arrow_table.to_pandas()
            
            # Connect to ClickHouse
            client = clickhouse_connect.get_client(
                host='clickhouse',
                port=8123,
                username='default',
                password='mysecret',
                database='bronze'
            )
            
            # Insert data in batches
            print("💾 Loading data to ClickHouse...")
            client.insert_df(
                table='medications_monthly',
                df=df
            )
            
            # Verify the load
            count = client.command('SELECT COUNT(*) FROM bronze.medications_monthly')
            print(f"✅ Successfully loaded {count} rows to ClickHouse")
            
            client.close()
            
            return {
                'rows_loaded': count,
                'table': 'bronze.medications_monthly'
            }
        except Exception as e:
            print(f"❌ Error loading data: {str(e)}")
            raise

    @task
    def verify_data():
        """Verify data quality in ClickHouse"""
        client = clickhouse_connect.get_client(
            host='clickhouse',
            port=8123,
            username='default',
            password='mysecret',
            database='bronze'
        )
        
        try:
            # Run verification queries
            total_count = client.command('SELECT COUNT(*) FROM bronze.medications_monthly')
            distinct_years = client.command('SELECT COUNT(DISTINCT year) FROM bronze.medications_monthly')
            distinct_months = client.command('SELECT COUNT(DISTINCT month) FROM bronze.medications_monthly')
            
            print(f"""
            📊 Data Verification Results:
            ============================
            Total rows: {total_count}
            Distinct years: {distinct_years}
            Distinct months: {distinct_months}
            """)
            
            # Sample data
            sample = client.query('SELECT * FROM bronze.medications_monthly LIMIT 5')
            print("\n📋 Sample data:")
            print(sample.result_rows)
            
            return {
                'total_count': total_count,
                'distinct_years': distinct_years,
                'distinct_months': distinct_months
            }
        except Exception as e:
            print(f"❌ Error verifying data: {str(e)}")
            raise
        finally:
            client.close()

    # Task dependencies
    create_db = create_clickhouse_database()
    create_table = create_clickhouse_table()
    load_data = load_iceberg_to_clickhouse()
    verify = verify_data()
    
    create_db >> create_table >> load_data >> verify