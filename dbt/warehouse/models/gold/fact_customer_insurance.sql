-- models/gold/fact_customer_insurance.sql
-- Fact Table: Customer Insurance
-- Grain: 1 row per customer
-- Source: bronze.insurance_silver + Gold Dimensions
-- 
-- Star Schema:
--   fact_customer_insurance
--       ├── dim_customer (customer_sk)
--       ├── dim_policy (policy_sk)
--       └── dim_risk (risk_sk)

with silver as (

    select * from {{ ref('insurance_silver') }}

),

dim_customer as (

    select customer_sk, customer_id from {{ ref('dim_customer') }}

),

dim_policy as (

    select policy_sk, customer_id from {{ ref('dim_policy') }}

),

dim_risk as (

    select risk_sk, customer_id, composite_risk_score, risk_tier from {{ ref('dim_risk') }}

)

select

    -- ==============================
    -- Fact Surrogate Key
    -- ==============================
    {{ dbt_utils.generate_surrogate_key(['s.id']) }} as fact_sk,

    -- ==============================
    -- Foreign Keys → Dimensions
    -- ==============================
    dc.customer_sk,
    dp.policy_sk,
    dr.risk_sk,

    -- ==============================
    -- Natural Key
    -- ==============================
    s.id as customer_id,

    -- ==============================
    -- Measures  
    -- ==============================

    -- Financial
    s.annual_income,
    s.credit_score,
    s.vehicle_age,
    s.insurance_duration,

    -- Claims
    s.previous_claims,
    coalesce(s.previous_claims, 0) as previous_claims_clean,

    -- Health
    s.health_score,

    -- Risk Score 
    dr.composite_risk_score,

    -- ==============================
    -- Derived Measures
    -- ==============================

    -- estimated monthly premium bucket based on risk tier
    case
        when dr.risk_tier = 'high_risk' then round(s.annual_income * 0.05 / 12, 2)
        when dr.risk_tier = 'medium_risk' then round(s.annual_income * 0.03 / 12, 2)
        else round(s.annual_income * 0.015 / 12, 2)
    end as estimated_monthly_premium,

    -- Customer Lifetime Value Proxy
    round(
        s.annual_income
        * s.insurance_duration
        * case
            when s.customer_feedback = 'good'    then 1.2
            when s.customer_feedback = 'average' then 1.0
            else 0.8
          end
    , 2) as customer_ltv_proxy,

    -- ==============================
    -- Flags
    -- ==============================
    s.is_high_risk_customer,
    s.is_age_imputed,
    s.is_income_imputed,
    s.is_credit_score_imputed,
    s.imputed_fields_count,
    s.data_quality_tier,

    -- ==============================
    -- Descriptive Segments  
    -- ==============================
    s.age_group,
    s.income_segment,
    s.risk_category,
    dr.risk_tier,
    s.claim_profile,
    s.lifestyle_profile,
    s.policy_type,
    s.gender,
    s.location,
    s.education_level,
    s.occupation,
    s.marital_status,
    s.property_type,
    s.smoking_status,
    s.exercise_frequency,
    s.customer_feedback

from silver s
left join dim_customer dc on s.id = dc.customer_id
left join dim_policy dp on s.id = dp.customer_id
left join dim_risk dr on s.id = dr.customer_id