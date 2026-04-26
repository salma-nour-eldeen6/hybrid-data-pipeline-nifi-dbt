-- create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- bronze table (raw data)
CREATE TABLE IF NOT EXISTS bronze.insurance_data (
    id SERIAL PRIMARY KEY,
    age INT,
    gender VARCHAR(10),
    annual_income FLOAT,
    marital_status VARCHAR(20),
    number_of_dependents INT,
    education_level VARCHAR(50),
    occupation VARCHAR(50),
    health_score FLOAT,
    location VARCHAR(50),
    policy_type VARCHAR(50),
    previous_claims INT,
    vehicle_age INT,
    credit_score INT,
    insurance_duration INT,
    policy_start_date DATE,
    customer_feedback TEXT,
    smoking_status VARCHAR(10),
    exercise_frequency VARCHAR(20),
    property_type VARCHAR(50)
);