-- models/gold/dim_customer.sql
-- Dimension: Customer Demographics
-- Grain: 1 row per customer
-- Source: bronze.insurance_silver

with source as (

    select * from {{ ref('insurance_silver') }}

)

select

    -- ==============================
    -- Surrogate Key
    -- ==============================
    {{ dbt_utils.generate_surrogate_key(['id']) }} as customer_sk,

    -- ==============================
    -- Natural Key
    -- ==============================
    id as customer_id,

    -- ==============================
    -- Demographics
    -- ==============================
    age,
    age_group,
    gender,
    marital_status,
    number_of_dependents,
    education_level,
    occupation,
    location,
    property_type,

    -- ==============================
    -- Health & Lifestyle
    -- ==============================
    health_score,
    smoking_status,
    exercise_frequency,
    lifestyle_profile,

    -- ==============================
    -- Data Quality
    -- ==============================
    is_age_imputed,
    is_marital_imputed,
    is_occupation_imputed,
    is_dependents_imputed,
    imputed_fields_count,
    data_quality_tier

from source