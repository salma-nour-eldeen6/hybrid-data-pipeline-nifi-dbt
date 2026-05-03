-- models/silver/insurance_silver.sql

with base as (

    select *
    from {{ ref('insurance_bronze') }}

)

select

   
    -- Core Identifiers
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

    -- Imputation Flags from Bronze
    is_age_imputed,
    is_income_imputed,
    is_marital_imputed,
    is_dependents_imputed,
    is_occupation_imputed,
    is_credit_score_imputed,

   
    -- Age Segmentation
    case
        when age < 25 then 'young'
        when age between 25 and 45 then 'adult'
        else 'senior'
    end as age_group,

 
    -- Income Segmentation
    case
        when annual_income < 20000 then 'low'
        when annual_income between 20000 and 70000 then 'medium'
        else 'high'
    end as income_segment,

    
    -- Risk Category
    case
        when credit_score < 500 then 'high_risk'
        when credit_score between 500 and 700 then 'medium_risk'
        else 'low_risk'
    end as risk_category,


    -- Claim Behavior
    -- previous_claims: categorize into 'no_claims', 'low_claims', 'high_claims'
    case
        when previous_claims is null then 'unknown'
        when previous_claims = 0 then 'no_claims'
        when previous_claims <= 2 then 'low_claims'
        else 'high_claims'
    end as claim_profile,

 
    -- Lifestyle Profile
    -- smoking_status و exercise_frequency مش فيهم nulls بس نحتاط
    case
        when smoking_status = 'yes'
         and exercise_frequency = 'rarely' then 'unhealthy'
        when smoking_status is null
          or exercise_frequency is null then 'unknown'
        else 'healthy'
    end as lifestyle_profile,

    
    -- High Risk Flag
    case
        when credit_score < 450
         and previous_claims > 2 then 1
        else 0
    end as is_high_risk_customer,

 
    -- Data Quality Score
    (
        is_age_imputed
      + is_income_imputed
      + is_marital_imputed
      + is_dependents_imputed
      + is_occupation_imputed
      + is_credit_score_imputed
    ) as imputed_fields_count,

    case
        when (is_age_imputed + is_income_imputed + is_marital_imputed
            + is_dependents_imputed + is_occupation_imputed + is_credit_score_imputed) = 0
            then 'complete'
        when (is_age_imputed + is_income_imputed + is_marital_imputed
            + is_dependents_imputed + is_occupation_imputed + is_credit_score_imputed) <= 2
            then 'mostly_complete'
        else 'high_missing'
    end as data_quality_tier

from base