with base as (
    select * from {{ ref('fact_customer_insurance') }}
)

-- ── 1. Overall risk distribution ──────────────────────────────
select
    'overall' as breakdown,
    risk_tier as segment,
    count(*)  as customers,
    round(100.0 * count(*) / sum(count(*)) over(), 2) as pct,
    round(avg(composite_risk_score), 2) as avg_risk_score,
    round(avg(annual_income), 0) as avg_income,
    round(avg(credit_score), 0) as avg_credit_score,
    sum(previous_claims_clean) as total_claims
from base
group by risk_tier

union all

-- ── 2. Risk by policy type ─────────────────────────────────────
select
    'by_policy_type'  as breakdown,
    concat(policy_type, ' | ', risk_tier) as segment,
    count(*),
    round(100.0 * count(*) / sum(count(*)) over(partition by policy_type), 2),
    round(avg(composite_risk_score), 2),
    round(avg(annual_income), 0),
    round(avg(credit_score), 0),
    sum(previous_claims_clean)
from base
group by policy_type, risk_tier

union all

-- ── 3. Risk by location ────────────────────────────────────────
select
    'by_location'     as breakdown,
    concat(location, ' | ', risk_tier) as segment,
    count(*),
    round(100.0 * count(*) / sum(count(*)) over(partition by location), 2),
    round(avg(composite_risk_score), 2),
    round(avg(annual_income), 0),
    round(avg(credit_score), 0),
    sum(previous_claims_clean)
from base
group by location, risk_tier

order by breakdown, customers desc