-- models/gold/dim_risk.sql
-- Dimension: Risk & Financial Profile
-- Grain: 1 row per customer
-- Source: insurance_silver (dbt model)

with source as (

    select *
    from {{ ref('insurance_silver') }}

)

select

    -- ==============================
    -- Surrogate Key
    -- ==============================
    {{ dbt_utils.generate_surrogate_key(['id']) }} as risk_sk,

    -- ==============================
    -- Natural Key
    -- ==============================
    id as customer_id,

    -- ==============================
    -- Financial Attributes
    -- ==============================
    annual_income,
    income_segment,
    credit_score,
    vehicle_age,

    -- ==============================
    -- Risk Indicators
    -- ==============================
    previous_claims,
    claim_profile,
    risk_category,
    is_high_risk_customer,

    -- ==============================
    -- Composite Risk Score (0-100)
    -- Weighted scoring model:
    -- - credit_score (higher = lower risk) → 40%
    -- - previous_claims (lower = lower risk) → 35%
    -- - annual_income (higher = lower risk) → 25%
    -- ==============================
    round(
        (
            -- Credit Score Component: normalized (300–849 → 0–40)
            (least(greatest(credit_score, 300), 849) - 300) / 549.0 * 40

            -- Claims Component: penalizes claim history (max 35 points)
            + greatest(35 - (coalesce(previous_claims, 0) * 7), 0)

            -- Income Component: normalized (0–150,000 → 0–25)
            + least(annual_income, 150000) / 150000.0 * 25
        )
    , 2) as composite_risk_score,

    -- ==============================
    -- Risk Tier Classification
    -- Based on composite risk score
    -- ==============================
    case
        when round(
            (least(greatest(credit_score, 300), 849) - 300) / 549.0 * 40
            + greatest(35 - (coalesce(previous_claims, 0) * 7), 0)
            + least(annual_income, 150000) / 150000.0 * 25
        , 2) >= 70 then 'low_risk'

        when round(
            (least(greatest(credit_score, 300), 849) - 300) / 549.0 * 40
            + greatest(35 - (coalesce(previous_claims, 0) * 7), 0)
            + least(annual_income, 150000) / 150000.0 * 25
        , 2) >= 40 then 'medium_risk'

        else 'high_risk'
    end as risk_tier,

    -- ==============================
    -- Data Quality Indicators
    -- ==============================
    is_credit_score_imputed,
    is_income_imputed

from source