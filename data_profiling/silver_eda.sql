-- ============================================================
-- 1. Row Count & Uniqueness
-- ============================================================
select
    count(*) as total_rows,
    count(distinct id) as unique_customers,
    count(*) - count(distinct id) as duplicate_ids
from raw_silver.insurance_silver;

--  total_rows | unique_customers | duplicate_ids
-- ------------+------------------+---------------
--      300000 |           300000 |             0
-- (1 row)


-- ============================================================
-- 2. Numerical Summary (Core Metrics)
-- ============================================================
select
    -- Age
    min(age) as min_age,
    max(age) as max_age,
    round(avg(age), 2) as avg_age,

    -- Income
    min(annual_income) as min_income,
    max(annual_income) as max_income,
    round(avg(annual_income), 2) as avg_income,

    -- Credit Score
    min(credit_score) as min_credit_score,
    max(credit_score) as max_credit_score,
    round(avg(credit_score), 2) as avg_credit_score,

    -- Health Score
    min(health_score) as min_health_score,
    max(health_score) as max_health_score,
    round(avg(health_score), 2) as avg_health_score,

    -- Vehicle & Duration
    min(vehicle_age) as min_vehicle_age,
    max(vehicle_age) as max_vehicle_age,
    round(avg(vehicle_age), 2) as avg_vehicle_age,

    min(insurance_duration) as min_duration,
    max(insurance_duration) as max_duration,
    round(avg(insurance_duration), 2) as avg_duration

from raw_silver.insurance_silver;

--  min_age | max_age | avg_age | min_income | max_income | avg_income | min_credit_score | max_credit_score | avg_credit_score | min_health_score | max_health_score | avg_health_score | min_vehicle_age | max_vehicle_age | avg_vehicle_age | min_duration | max_duration | avg_duration
-- ---------+---------+---------+------------+------------+------------+------------------+------------------+------------------+------------------+------------------+------------------+-----------------+-----------------+-----------------+--------------+--------------+--------------
--       18 |      64 |   41.14 |          2 |     149997 |   32767.88 |              300 |              849 |           593.09 |      2.068451579 | 58.5696892033348 |            25.60 |               0 |              19 |            9.56 |            1 |            9 |         5.02
-- (1 row)

-- ============================================================
-- 3. Null Check (Data Quality After Imputation)
-- ============================================================
select
    sum(case when age is null then 1 else 0 end) as age_nulls,
    sum(case when annual_income is null then 1 else 0 end) as income_nulls,
    sum(case when credit_score is null then 1 else 0 end) as credit_nulls,
    sum(case when occupation is null or occupation = '' then 1 else 0 end) as occupation_nulls,
    sum(case when marital_status is null or marital_status = '' then 1 else 0 end) as marital_nulls,
    sum(case when location is null or location = '' then 1 else 0 end) as location_nulls,
    sum(case when previous_claims is null then 1 else 0 end) as claims_nulls
from raw_silver.insurance_silver;

--  age_nulls | income_nulls | credit_nulls | occupation_nulls | marital_nulls | location_nulls | claims_nulls
-- -----------+--------------+--------------+------------------+---------------+----------------+--------------
--          0 |            0 |            0 |                0 |             0 |              0 |        91008
-- (1 row)


-- ============================================================
-- 4. Segment Distribution (Core Business Fields)
-- ============================================================
select
    'age_group' as field, age_group as value, count(*) as cnt
from raw_silver.insurance_silver
group by age_group

union all

select
    'income_segment', income_segment, count(*)
from raw_silver.insurance_silver
group by income_segment

union all

select
    'risk_category', risk_category, count(*)
from raw_silver.insurance_silver
group by risk_category

union all

select
    'policy_type', policy_type, count(*)
from raw_silver.insurance_silver
group by policy_type

union all

select
    'lifestyle_profile', lifestyle_profile, count(*)
from raw_silver.insurance_silver
group by lifestyle_profile

order by field, cnt desc;

--        field       |     value     |  cnt
-- -------------------+---------------+--------
--  age_group         | adult         | 136611
--  age_group         | senior        | 120315
--  age_group         | young         |  43074
--  income_segment    | low           | 131859
--  income_segment    | medium        | 129784
--  income_segment    | high          |  38357
--  lifestyle_profile | healthy       | 262295
--  lifestyle_profile | unhealthy     |  37705
--  policy_type       | premium       | 100781
--  policy_type       | comprehensive |  99714
--  policy_type       | basic         |  99505
--  risk_category     | medium_risk   | 138429
--  risk_category     | high_risk     |  83907
--  risk_category     | low_risk      |  77664
-- (14 rows)

-- ============================================================
-- 5. Categorical Distribution 
-- ============================================================
select 'gender' as field, gender as value, count(*) as cnt
from raw_silver.insurance_silver group by gender

union all
select 'marital_status', marital_status, count(*)
from raw_silver.insurance_silver group by marital_status

union all
select 'location', location, count(*)
from raw_silver.insurance_silver group by location

union all
select 'occupation', occupation, count(*)
from raw_silver.insurance_silver group by occupation

order by field, cnt desc;

--      field      |     value     |  cnt
-- ----------------+---------------+--------
--  gender         | male          | 150613
--  gender         | female        | 149387
--  location       | rural         | 100668
--  location       | suburban      |  99726
--  location       | urban         |  99606
--  marital_status | single        |  98768
--  marital_status | married       |  98767
--  marital_status | divorced      |  97825
--  marital_status | unknown       |   4640
--  occupation     | unknown       |  89451
--  occupation     | Employed      |  70830
--  occupation     | Self-Employed |  70573
--  occupation     | Unemployed    |  69146
-- (13 rows)

-- ============================================================
-- 6. High Risk Profile Summary
-- ============================================================
select
    count(*) as total,
    sum(is_high_risk_customer) as high_risk,
    round(100.0 * sum(is_high_risk_customer) / count(*), 2) as high_risk_pct,
    round(avg(credit_score), 2) as avg_credit,
    round(avg(annual_income), 2) as avg_income,
    round(avg(previous_claims), 2) as avg_claims
from raw_silver.insurance_silver;

--  total  | high_risk | high_risk_pct | avg_credit | avg_income | avg_claims
-- --------+-----------+---------------+------------+------------+------------
--  300000 |      2379 |          0.79 |     593.09 |   32767.88 |       1.00
-- (1 row)

-- ============================================================
-- 7. Cross Analysis: Policy vs Risk
-- ============================================================
select
    policy_type,
    risk_category,
    count(*) as cnt,
    round(100.0 * count(*) / sum(count(*)) over(partition by policy_type), 2) as pct,
    round(avg(credit_score), 0) as avg_credit,
    round(avg(annual_income), 0) as avg_income
from raw_silver.insurance_silver
group by policy_type, risk_category
order by policy_type, cnt desc;

--   policy_type  | risk_category |  cnt  |  pct  | avg_credit | avg_income
-- ---------------+---------------+-------+-------+------------+------------
--  basic         | medium_risk   | 45781 | 46.01 |        601 |      28555
--  basic         | high_risk     | 27835 | 27.97 |        413 |      43638
--  basic         | low_risk      | 25889 | 26.02 |        772 |      28431
--  comprehensive | medium_risk   | 45982 | 46.11 |        602 |      28643
--  comprehensive | high_risk     | 27907 | 27.99 |        413 |      43440
--  comprehensive | low_risk      | 25825 | 25.90 |        772 |      28666
--  premium       | medium_risk   | 46666 | 46.30 |        602 |      28424
--  premium       | high_risk     | 28165 | 27.95 |        413 |      43766
--  premium       | low_risk      | 25950 | 25.75 |        773 |      28657
-- (9 rows)

-- ============================================================
-- 8. Imputation Summary
-- ============================================================
select
    sum(is_age_imputed) as age_imputed,
    sum(is_income_imputed) as income_imputed,
    sum(is_credit_score_imputed) as credit_imputed,
    sum(is_occupation_imputed) as occupation_imputed,
    sum(case when imputed_fields_count > 0 then 1 else 0 end) as affected_rows,
    round(100.0 * sum(case when imputed_fields_count > 0 then 1 else 0 end) / count(*), 2) as imputation_rate
from raw_silver.insurance_silver;

--  age_imputed | income_imputed | credit_imputed | occupation_imputed | affected_rows | imputation_rate
-- -------------+----------------+----------------+--------------------+---------------+-----------------
--         4545 |          11257 |          34516 |              89451 |        142313 |           47.44
-- (1 row)
