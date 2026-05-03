with base as (
    select * from {{ ref('fact_customer_insurance') }}
)

-- ── 1. Imputed vs clean customers — key metrics comparison ────
select
    case when imputed_fields_count = 0 then 'clean' else 'has_imputation' end as data_type,
    data_quality_tier,
    count(*) as customers,
    round(avg(annual_income), 0) as avg_income,
    round(avg(credit_score), 0) as avg_credit_score,
    round(avg(composite_risk_score), 2) as avg_risk_score,
    round(avg(health_score), 2) as avg_health_score,
    round(avg(previous_claims_clean), 2) as avg_claims,
    sum(is_high_risk_customer) as high_risk_count,
    round(100.0 * sum(is_high_risk_customer) / count(*), 2) as high_risk_pct
from base
group by 1, 2
order by data_type, data_quality_tier

union all

-- ── 2. Per-field imputation effect ────────────────────────────
select
    'credit_score_imputed' as data_type,
    cast(is_credit_score_imputed as varchar) as data_quality_tier,
    count(*),
    round(avg(annual_income), 0),
    round(avg(credit_score), 0),
    round(avg(composite_risk_score), 2),
    round(avg(health_score), 2),
    round(avg(previous_claims_clean), 2),
    sum(is_high_risk_customer),
    round(100.0 * sum(is_high_risk_customer) / count(*), 2)
from base
group by is_credit_score_imputed

union all

select
    'income_imputed',
    cast(is_income_imputed as varchar),
    count(*),
    round(avg(annual_income), 0),
    round(avg(credit_score), 0),
    round(avg(composite_risk_score), 2),
    round(avg(health_score), 2),
    round(avg(previous_claims_clean), 2),
    sum(is_high_risk_customer),
    round(100.0 * sum(is_high_risk_customer) / count(*), 2)
from base
group by is_income_imputed

order by data_type, data_quality_tier