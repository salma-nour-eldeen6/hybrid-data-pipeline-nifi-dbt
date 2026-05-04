-- snapshots/insurance_silver_snapshot.sql
-- ============================================================
-- Snapshot: SCD Type 2 on insurance_silver
-- Tracks changes in customer risk profile, segments, and flags
-- 
-- dbt will automatically add:
--   dbt_scd_id       — surrogate key for each snapshot row
--   dbt_updated_at   — when the snapshot was last updated
--   dbt_valid_from   — when this version became active
--   dbt_valid_to     — when this version expired (null = current)
-- ============================================================

{% snapshot insurance_silver_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='id',
        strategy='check',
        check_cols=[
            'credit_score',
            'risk_category',
            'is_high_risk_customer',
            'composite_risk_score',
            'previous_claims',
            'claim_profile',
            'income_segment',
            'age_group',
            'lifestyle_profile',
            'policy_type',
            'insurance_duration',
            'customer_feedback',
            'data_quality_tier',
            'imputed_fields_count'
        ]
    )
}}

select
    id,
    age,
    age_group,
    gender,
    annual_income,
    income_segment,
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

    -- Derived segments
    risk_category,
    claim_profile,
    lifestyle_profile,
    is_high_risk_customer,
    data_quality_tier,
    imputed_fields_count,

    -- Imputation flags
    is_age_imputed,
    is_income_imputed,
    is_marital_imputed,
    is_dependents_imputed,
    is_occupation_imputed,
    is_credit_score_imputed,

    -- Snapshot timestamp (required for 'check' strategy)
    now() as updated_at

from {{ ref('insurance_silver') }}

{% endsnapshot %}