with base as (
    select * from {{ ref('fact_customer_insurance') }}
),

-- ── Segment-level aggregation ─────────────────────────────────
segment_summary as (
    select
        age_group,
        income_segment,
        risk_tier,
        policy_type,
        lifestyle_profile,
        gender,
        location,

        count(*) as customer_count,

        -- LTV
        round(avg(customer_ltv_proxy), 0) as avg_ltv,
        round(min(customer_ltv_proxy), 0) as min_ltv,
        round(max(customer_ltv_proxy), 0) as max_ltv,
        round(sum(customer_ltv_proxy), 0) as total_ltv,

        -- Premium
        round(avg(estimated_monthly_premium), 2) as avg_monthly_premium,
        round(sum(estimated_monthly_premium), 2) as total_monthly_premium,
        round(sum(estimated_monthly_premium) * 12, 2) as projected_annual_premium,

        -- Risk
        round(avg(composite_risk_score), 2) as avg_risk_score,
        sum(is_high_risk_customer) as high_risk_count,

        -- Claims
        round(avg(previous_claims_clean), 2) as avg_claims,
        sum(previous_claims_clean) as total_claims

    from base
    group by
        age_group, income_segment, risk_tier,
        policy_type, lifestyle_profile, gender, location
)

select
    *,
    -- Revenue efficiency: LTV per claim
    round(
        {{ safe_divide('avg_ltv', 'nullif(avg_claims, 0)') }}
    , 0) as ltv_per_claim,

    -- LTV as % of total
    round(
        100.0 * total_ltv / sum(total_ltv) over()
    , 4) as ltv_share_pct

from segment_summary
order by total_ltv desc