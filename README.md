# dataEng2025
Project 1: Data Architecture & Modeling

Group 4: Annika Remmelgas, Agnes Kala, Imbi Jaks, Liis Andresen

Business Brief

We analyze the relationship between weather conditions and medication sales in Estonia to support healthcare planning.

KPIs: Medications sold/month, Avg. temperature, Sunny days

Business Questions: Seasonal spikes, weather effects, forecasting demand

Data & Tools

Data: Medication sales (Tervisekassa), Weather data (Ilmateenistus/ECAD)

Tools: PostgreSQL, dbt, Airflow, Superset, Docker, Python

Schema

Star schema with central fact_medication_sales table + dimensions:

dim_date, dim_weather, dim_medication, dim_diagnosis

Repo Contents

sql/ddl_create_tables.sql → schema (DDL)

sql/dml_insert_sample_data.sql → sample inserts (DML)

sql/demo_queries.sql → example queries

Run Instructions

psql -h localhost -U postgres -f sql/ddl_create_tables.sql

psql -h localhost -U postgres -f sql/dml_insert_sample_data.sql

psql -h localhost -U postgres -f sql/demo_queries.sql

Notes

..to be added..
psql -h localhost -U postgres -f sql/dml_insert_sample_data.sql
psql -h localhost -U postgres -f sql/demo_queries.sql
