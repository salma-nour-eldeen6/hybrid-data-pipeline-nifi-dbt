-- models/silver/insurance_silver.sql

with base as (

    select *
    from {{ ref('insurance_bronze') }}

)

select

    -- =========================
    -- Core Identifiers
    -- =========================
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
    property_type,

    -- -- =========================
    -- -- Cleaned Date (fixed)
    -- -- =========================
    -- case
    --     when REGEXP_LIKE(policy_start_date, '^\d{2}:\d{2}\.\d$')
    --     then null
    --     else policy_start_date
    -- end as policy_start_date,

    -- =========================
    -- Age Segmentation
    -- =========================
    case
        when age < 25 then 'young'
        when age between 25 and 45 then 'adult'
        else 'senior'
    end as age_group,

    -- =========================
    -- Income Segmentation
    -- =========================
    case
        when annual_income < 20000 then 'low'
        when annual_income between 20000 and 70000 then 'medium'
        else 'high'
    end as income_segment,

    -- =========================
    -- Risk Category
    -- =========================
    case
        when credit_score < 500 then 'high_risk'
        when credit_score between 500 and 700 then 'medium_risk'
        else 'low_risk'
    end as risk_category,

    -- =========================
    -- Claim Behavior
    -- =========================
    case
        when previous_claims = 0 then 'no_claims'
        when previous_claims <= 2 then 'low_claims'
        else 'high_claims'
    end as claim_profile,

    -- =========================
    -- Lifestyle Profile
    -- =========================
    case
        when smoking_status = 'yes'
         and exercise_frequency = 'rarely'
        then 'unhealthy'
        else 'healthy'
    end as lifestyle_profile,

    -- =========================
    -- High Risk Flag
    -- =========================
    case
        when credit_score < 450
         and previous_claims > 2
        then 1 else 0
    end as is_high_risk_customer

from base