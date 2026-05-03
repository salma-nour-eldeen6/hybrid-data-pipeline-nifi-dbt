-- models/bronze/insurance_bronze.sql

-- This model cleans and transforms raw insurance data from the bronze layer.
-- It handles NULL values, standardization, and type casting before loading into the silver layer.

with base as (

    select *
    from {{ source('bronze', 'insurance_data') }}

),

-- CTE for cleaning the data
clean as (

    -- Select only the required columns from the raw source
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
        customer_feedback,
        smoking_status,
        exercise_frequency,
        property_type

    from base

),

-- ============================================================
-- STEP 1: Compute statistics for numeric imputation
-- Using aggregate functions (AVG / MEDIAN)
-- ============================================================

stats as (

    select
        -- Average for continuous numerical fields
        AVG(CAST(age AS NUMERIC)) as avg_age,
        AVG(CAST(annual_income AS NUMERIC))  as avg_annual_income,

        -- Median for credit_score (more robust to outliers)
        PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY CAST(NULLIF(TRIM(CAST(credit_score AS TEXT)), '') AS INT)
        ) as median_credit_score

    from clean
    where age           is not null
      and annual_income is not null

)

select

    c.id,

    -- age: impute NULL values using average age (rounded)
    COALESCE(
        CAST(c.age AS INT),
        CAST(ROUND(s.avg_age) AS INT)
    ) as age,

        --  age_nulls | 
        -- -----------+ 
        --       4545 |   
        

    -- gender: normalize to lowercase
    lower(c.gender) as gender,

    -- annual_income: impute NULL values using average income
    COALESCE(
        CAST(c.annual_income AS NUMERIC),
        CAST(ROUND(s.avg_annual_income) AS NUMERIC)
    ) as annual_income,

        --  income_nulls | 
        -- --------------+ 
        --       11257   | 

 
    -- marital_status: handle NULLs and empty strings
    COALESCE(
        NULLIF(lower(trim(c.marital_status)), ''),
        'unknown'
    ) as marital_status,

 
    -- number_of_dependents: default NULLs to 0
    COALESCE(
        CAST(NULLIF(CAST(c.number_of_dependents AS VARCHAR), '') AS INT),
        0
    ) as number_of_dependents,

    -- Keep as-is (already clean categorical)
    c.education_level,

    -- occupation: replace NULL or empty with 'unknown'
    COALESCE(
        NULLIF(trim(c.occupation), ''),
        'unknown'
    ) as occupation,

    -- Cast to numeric (no imputation applied)
    CAST(c.health_score AS NUMERIC) as health_score,

    -- Normalize categorical columns
    lower(c.location) as location,
    lower(c.policy_type) as policy_type,

    -- Cast previous_claims, handle empty strings
    CAST(NULLIF(CAST(c.previous_claims AS VARCHAR), '') AS INT) as previous_claims,

    -- Cast numeric fields
    CAST(c.vehicle_age AS INT) as vehicle_age,


    -- credit_score: impute NULL values using median
    COALESCE(
        CAST(NULLIF(TRIM(CAST(c.credit_score AS TEXT)), '') AS INT),
        CAST(ROUND(s.median_credit_score) AS INT)
    ) as credit_score,

    CAST(c.insurance_duration AS INT) as insurance_duration,

    -- Normalize remaining categorical fields
    lower(c.customer_feedback) as customer_feedback,
    lower(c.smoking_status) as smoking_status,
    lower(c.exercise_frequency) as exercise_frequency,
    lower(c.property_type) as property_type,


-- Important
    -- ================================================
    -- Imputation Flags
    -- Track which fields were originally NULL or empty
    -- Useful for downstream analysis and ML features
    -- ================================================
    case when c.age is null then 1 else 0 end as is_age_imputed,

    case when c.annual_income is null then 1 else 0 end as is_income_imputed,

    case when c.marital_status is null
          or trim(c.marital_status) = ''
         then 1 else 0 end as is_marital_imputed,

    case when CAST(NULLIF(CAST(c.number_of_dependents AS VARCHAR), '') AS INT)
              is null
         then 1 else 0 end as is_dependents_imputed,

    case when NULLIF(trim(c.occupation), '') is null
         then 1 else 0 end as is_occupation_imputed,

    case when CAST(NULLIF(TRIM(CAST(c.credit_score AS TEXT)), '') AS INT)
              is null
         then 1 else 0 end as is_credit_score_imputed

from clean c
cross join stats s

-- Final result:
-- Cleaned dataset with standardized values, NULL handling,
-- and imputation flags for data quality tracking
-- Materialized as: raw_bronze.insurance_bronze