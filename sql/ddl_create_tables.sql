-- Dimension: Date
CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE NOT NULL,
    day INT,
    month INT,
    year INT,
    season VARCHAR(20)
);

-- Dimension: Weather
CREATE TABLE dim_weather (
    weather_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES dim_date(date_id),
    region VARCHAR(100),
    avg_temp NUMERIC,
    precipitation NUMERIC,
    wind_speed NUMERIC,
    humidity NUMERIC,
    sunlight_hours NUMERIC
);

-- Dimension: Medication
CREATE TABLE dim_medication (
    medication_id SERIAL PRIMARY KEY,
    atc_code VARCHAR(10),
    medication_name VARCHAR(255),
    medication_type VARCHAR(100),
    active_substance VARCHAR(255)
);

-- Dimension: Diagnosis
CREATE TABLE dim_diagnosis (
    diagnosis_id SERIAL PRIMARY KEY,
    diagnosis_code VARCHAR(10),
    diagnosis_name VARCHAR(255)
);

-- Fact Table
CREATE TABLE fact_medication_sales (
    sale_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES dim_date(date_id),
    weather_id INT REFERENCES dim_weather(weather_id),
    medication_id INT REFERENCES dim_medication(medication_id),
    diagnosis_id INT REFERENCES dim_diagnosis(diagnosis_id),
    quantity_sold INT,
    prescription_counts INT,
    total_cost NUMERIC
);
