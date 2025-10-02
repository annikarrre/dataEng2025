-- Dates
INSERT INTO dim_date (full_date, day, month, year, season) VALUES
('2025-01-15', 15, 1, 2025, 'Winter'),
('2025-06-20', 20, 6, 2025, 'Summer');

-- Weather
INSERT INTO dim_weather (date_id, region, avg_temp, precipitation, wind_speed, humidity, sunlight_hours) VALUES
(1, 'Tallinn', -5.0, 12.0, 15.0, 85.0, 2.0),
(2, 'Tartu', 20.0, 5.0, 8.0, 60.0, 12.0);

-- Medications
INSERT INTO dim_medication (atc_code, medication_name, medication_type, active_substance) VALUES
('R05', 'Cough Syrup', 'Cold/Flu', 'Dextromethorphan'),
('N06', 'Antidepressant A', 'Antidepressant', 'Sertraline');

-- Diagnoses
INSERT INTO dim_diagnosis (diagnosis_code, diagnosis_name) VALUES
('J00', 'Acute nasopharyngitis [common cold]'),
('F32', 'Depressive episode');

-- Fact Table
INSERT INTO fact_medication_sales (date_id, weather_id, medication_id, diagnosis_id, quantity_sold, prescription_counts, total_cost) VALUES
(1, 1, 1, 1, 500, 400, 2500.00),
(2, 2, 2, 2, 300, 280, 4200.00);
