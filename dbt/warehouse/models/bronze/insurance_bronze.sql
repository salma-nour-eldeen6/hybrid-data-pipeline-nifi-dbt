--- models/bronze/insurance_bronze.sql

--- This model is responsible for cleaning and transforming the raw insurance data from the bronze layer 
--- before it is loaded into the silver layer. 

with base as (
    --- CTE: IS A COMMON TABLE EXPRESSION, WHICH IS A TEMPORARY RESULT SET THAT CAN BE REFERENCED WITHIN A SELECT, INSERT, UPDATE, OR DELETE STATEMENT.

--- This CTE selects all the raw data from the bronze layer's insurance_data table.

    select *
    from {{ source('bronze', 'insurance_data') }}

),
--- The clean CTE performs various transformations to clean and standardize the data, such as:
--- - Casting data types to the appropriate formats (e.g., age to INT, annual_income to NUMERIC).
--- - Converting text to lowercase for consistency (e.g gender, marital_status, location, etc.).
--- - Handling NULL values for numeric fields by using NULLIF and casting to INT.

clean as (

    select

        id,
        age,
        gender,
        annual_income,
        marital_status,
        number_of_dependents,
        education_level,
        occupation,
        health_score,
        location,
        policy_type,
        previous_claims,
        vehicle_age,
        credit_score,
        insurance_duration,
        -- policy_start_date,
        customer_feedback,
        smoking_status,
        exercise_frequency,
        property_type

    from base

)

select

    id,

    CAST(age AS INT) as age,

    lower(gender) as gender,

    CAST(annual_income AS NUMERIC) as annual_income,

    lower(marital_status) as marital_status,

    CAST(NULLIF(CAST(number_of_dependents AS VARCHAR), '') AS INT) as number_of_dependents,

    education_level,

    nullif(trim(occupation), '') as occupation,

    CAST(health_score AS NUMERIC) as health_score,

    lower(location) as location,

    lower(policy_type) as policy_type,

    CAST(NULLIF(CAST(previous_claims AS VARCHAR), '') AS INT) as previous_claims,

    CAST(vehicle_age AS INT) as vehicle_age,

    CAST(NULLIF(TRIM(CAST(credit_score AS TEXT)), '') AS INT) AS credit_score,

    CAST(insurance_duration AS INT) as insurance_duration,

    lower(customer_feedback) as customer_feedback,

    lower(smoking_status) as smoking_status,

    lower(exercise_frequency) as exercise_frequency,

    lower(property_type) as property_type

from clean

--- After that we will got a cleaned and standardized dataset that can be loaded into the silver layer for further analysis and modeling.
--- saved in raw_bronze.insurance_bronze