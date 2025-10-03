# dataEng2025
Project 1: Data Architecture & Modeling

Group 4: Annika Remmelgas, Agnes Kala, Imbi Jaks, Liis Andresen

Business Brief

Objective: To analyze the relationships between weather conditions and medications sold in order to support planning in the healthcare sector. 

KPIs: Medications sold/month, Avg. temperature, Sunny days

Data & Tools

Data: Medication sales (Tervisekassa, https://tervisekassa.ee/muudud-ravimid-diagnoosi-ja-arsti-eriala-loikes )

  Weather data (Ilmateenistus/ECAD, https://www.ilmateenistus.ee/kliima/ajaloolised-ilmaandmed/  )

Tools: Docker, PostgreSQL, dbt, Airflow, Superset,  Python

Schema

Star schema with central fact_medication_sales table + dimensions:

dim_date, dim_weather, dim_medication, dim_diagnosis

Repo Contents

sql/ddl_create_tables.sql - schema (DDL)

sql/demo_queries.sql - example queries

Run Instructions

psql -h localhost -U postgres -f sql/ddl_create_tables.sql

psql -h localhost -U postgres -f sql/demo_queries.sql

Notes

..to be added..
