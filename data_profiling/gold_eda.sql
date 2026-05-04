-- ============================================================
-- 1. Row Count Across Gold Tables
-- ============================================================
select 'fact_customer_insurance' as table_name, count(*) as row_count
from raw_gold.fact_customer_insurance

union all

select 'dim_customer', count(*)
from raw_gold.dim_customer

union all

select 'dim_policy', count(*)
from raw_gold.dim_policy

union all

select 'dim_risk', count(*)
from raw_gold.dim_risk

order by table_name;

--        table_name        | row_count
-- -------------------------+-----------
--  dim_customer            |    300000
--  dim_policy              |    300000
--  dim_risk                |    300000
--  fact_customer_insurance |    300000
-- (4 rows)

-- because the grain of the fact table is 1 row per customer, we expect the fact and dimension tables to have the same row count. 
-- This confirms that all customers have corresponding dimension records and there are no duplicates or missing keys in the dimensions.


-- ============================================================
-- 2. Risk Tier Summary (Core Business View)
-- ============================================================
select
    risk_tier,
    count(*) as customers,
    round(avg(composite_risk_score), 2) as avg_risk_score,
    round(avg(credit_score), 0) as avg_credit,
    round(avg(annual_income), 0) as avg_income,
    sum(previous_claims_clean) as total_claims,
    round(avg(customer_ltv_proxy), 0) as avg_ltv,
    round(avg(estimated_monthly_premium), 2) as avg_premium,
    round(100.0 * sum(is_high_risk_customer) / count(*), 2) as high_risk_pct
from raw_gold.fact_customer_insurance
group by risk_tier
order by avg_risk_score desc;

--   risk_tier  | customers | avg_risk_score | avg_credit | avg_income | total_claims | avg_ltv | avg_premium | high_risk_pct
-- -------------+-----------+----------------+------------+------------+--------------+---------+-------------+---------------
--  low_risk    |     44842 |          75.43 |        765 |      46638 |         7815 |  229704 |       58.30 |          0.00
--  medium_risk |    229535 |          55.87 |        578 |      31391 |       152449 |  154891 |       78.48 |          0.06
--  high_risk   |     25623 |          34.11 |        424 |      20829 |        49001 |  103643 |       86.79 |          8.77
-- (3 rows)


-- ============================================================
-- 3. Composite Risk Distribution
-- ============================================================
select
    case
        when composite_risk_score < 20 then 'very_high_risk'
        when composite_risk_score < 40 then 'high_risk'
        when composite_risk_score < 60 then 'medium_risk'
        when composite_risk_score < 80 then 'low_risk'
        else 'very_low_risk'
    end as risk_bucket,

    count(*) as customers,
    round(100.0 * count(*) / sum(count(*)) over(), 2) as pct,
    round(avg(credit_score), 0) as avg_credit,
    round(avg(annual_income), 0) as avg_income,
    round(avg(previous_claims_clean), 2) as avg_claims

from raw_gold.fact_customer_insurance
group by 1
order by min(composite_risk_score);

--   risk_bucket   | customers |  pct  | avg_credit | avg_income | avg_claims
-- ----------------+-----------+-------+------------+------------+------------
--  very_high_risk |       611 |  0.20 |        367 |      16345 |       3.88
--  high_risk      |     25012 |  8.34 |        425 |      20939 |       1.86
--  medium_risk    |    152234 | 50.74 |        535 |      29344 |       0.79
--  low_risk       |    116107 | 38.70 |        696 |      36861 |       0.33
--  very_low_risk  |      6036 |  2.01 |        783 |      91064 |       0.14
-- (5 rows)

-- ============================================================
-- 4. LTV Distribution
-- LTV is a key metric for customer valuation and segmentation. Understanding its distribution helps identify high-value customers and tail segments.
-- ============================================================
select
    case
        when customer_ltv_proxy < 50000 then 'low_ltv'
        when customer_ltv_proxy < 150000 then 'mid_ltv'
        when customer_ltv_proxy < 300000 then 'high_ltv'
        else 'very_high_ltv'
    end as ltv_bucket,

    count(*) as customers,
    round(100.0 * count(*) / sum(count(*)) over(), 2) as pct,
    round(avg(customer_ltv_proxy), 0) as avg_ltv,
    round(avg(annual_income), 0) as avg_income

from raw_gold.fact_customer_insurance
group by 1
order by min(customer_ltv_proxy);

--   ltv_bucket   | customers |  pct  | avg_ltv | avg_income
-- ---------------+-----------+-------+---------+------------
--  low_ltv       |    105556 | 35.19 |   20634 |       9525
--  mid_ltv       |     86495 | 28.83 |   94004 |      28729
--  high_ltv      |     59995 | 20.00 |  214612 |      42726
--  very_high_ltv |     47954 | 15.98 |  528094 |      78756
-- (4 rows)

-- ============================================================
-- 5. Premium Distribution
-- ============================================================
select
    case
        when estimated_monthly_premium < 50 then 'low'
        when estimated_monthly_premium < 150 then 'medium'
        when estimated_monthly_premium < 300 then 'high'
        else 'very_high'
    end as premium_bucket,

    count(*) as customers,
    round(100.0 * count(*) / sum(count(*)) over(), 2) as pct,
    round(avg(estimated_monthly_premium), 2) as avg_premium

from raw_gold.fact_customer_insurance
group by 1
order by min(estimated_monthly_premium);

--  premium_bucket | customers |  pct  | avg_premium
-- ----------------+-----------+-------+-------------
--  low            |    142540 | 47.51 |       21.70
--  medium         |    114810 | 38.27 |       88.89
--  high           |     36169 | 12.06 |      203.46
--  very_high      |      6481 |  2.16 |      338.48
-- (4 rows)
 


-- ============================================================
-- 6. Top Segment Combinations (Revenue View)
-- ============================================================
select
    age_group,
    income_segment,
    policy_type,
    risk_tier,
    count(*) as customers,
    round(avg(customer_ltv_proxy), 0) as avg_ltv,
    round(sum(customer_ltv_proxy), 0) as total_ltv,
    round(sum(estimated_monthly_premium) * 12, 0) as annual_revenue
from raw_gold.fact_customer_insurance
group by age_group, income_segment, policy_type, risk_tier
order by total_ltv desc
fetch first 20 rows only;

--  age_group | income_segment |  policy_type  |  risk_tier  | customers | avg_ltv | total_ltv  | annual_revenue
-- -----------+----------------+---------------+-------------+-----------+---------+------------+----------------
--  adult     | medium         | premium       | medium_risk |     14983 |  185897 | 2785297650 |       16959898
--  adult     | medium         | comprehensive | medium_risk |     14873 |  186941 | 2780366059 |       16766704
--  adult     | medium         | basic         | medium_risk |     14981 |  185328 | 2776401878 |       16856851
--  senior    | medium         | premium       | medium_risk |     13389 |  185883 | 2488787746 |       15051456
--  senior    | medium         | comprehensive | medium_risk |     13088 |  186981 | 2447203322 |       14769435
--  senior    | medium         | basic         | medium_risk |     13057 |  185450 | 2421421819 |       14715786
--  adult     | high           | premium       | medium_risk |      4149 |  483550 | 2006248909 |       12158519
--  adult     | high           | comprehensive | medium_risk |      4069 |  481297 | 1958396581 |       11974411
--  adult     | high           | basic         | medium_risk |      4039 |  478505 | 1932680826 |       11860629
--  senior    | high           | basic         | medium_risk |      3546 |  484843 | 1719254993 |       10477420
--  senior    | high           | premium       | medium_risk |      3568 |  477475 | 1703630931 |       10460226
--  senior    | high           | comprehensive | medium_risk |      3540 |  477628 | 1690803828 |       10415305
--  young     | medium         | premium       | medium_risk |      4871 |  182815 |  890492690 |        5468629
--  young     | medium         | basic         | medium_risk |      4645 |  186992 |  868578122 |        5237471
--  adult     | high           | comprehensive | low_risk    |      1587 |  540732 |  858141689 |        2572542
--  young     | medium         | comprehensive | medium_risk |      4639 |  184669 |  856680925 |        5258864
--  adult     | high           | basic         | low_risk    |      1545 |  520270 |  803817080 |        2491684
--  adult     | high           | premium       | low_risk    |      1529 |  511209 |  781639163 |        2431960
--  senior    | high           | comprehensive | low_risk    |      1449 |  516836 |  748895720 |        2274709
--  senior    | high           | premium       | low_risk    |      1429 |  520452 |  743725670 |        2287759
-- (20 rows)

 
-- ============================================================
-- 7. Geographic Overview
-- ============================================================
select
    location,
    count(*) as customers,
    round(avg(composite_risk_score), 2) as avg_risk,
    round(avg(annual_income), 0) as avg_income,
    round(avg(customer_ltv_proxy), 0) as avg_ltv,
    round(100.0 * sum(is_high_risk_customer) / count(*), 2) as high_risk_pct
from raw_gold.fact_customer_insurance
group by location
order by customers desc;

--  location | customers | avg_risk | avg_income | avg_ltv | high_risk_pct
-- ----------+-----------+----------+------------+---------+---------------
--  rural    |    100668 |    56.97 |      32693 |  161260 |          0.79
--  suburban |     99726 |    56.96 |      32869 |  162442 |          0.78
--  urban    |     99606 |    56.88 |      32743 |  161390 |          0.81
-- (3 rows)

-- ============================================================
-- 8. Data Quality Impact
-- ============================================================
select
    data_quality_tier,
    count(*) as customers,
    round(avg(composite_risk_score), 2) as avg_risk,
    round(avg(annual_income), 0) as avg_income,
    round(avg(credit_score), 0) as avg_credit,
    round(avg(customer_ltv_proxy), 0) as avg_ltv,
    round(100.0 * sum(is_high_risk_customer) / count(*), 2) as high_risk_pct
from raw_gold.fact_customer_insurance
group by data_quality_tier
order by data_quality_tier;

--  data_quality_tier | customers | avg_risk | avg_income | avg_credit | avg_ltv | high_risk_pct
-- -------------------+-----------+----------+------------+------------+---------+---------------
--  complete          |    157687 |    57.18 |      33284 |        595 |  164263 |          0.88
--  high_missing      |      2280 |    55.95 |      31158 |        586 |  150561 |          0.53
--  mostly_complete   |    140033 |    56.68 |      32212 |        591 |  158987 |          0.70
-- (3 rows)