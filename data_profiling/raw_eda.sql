-- ============================================================
-- 1. Row Count & Uniqueness Check
-- ============================================================
select
    count(*) as total_rows,
    count(distinct id) as unique_customers,
    count(*) - count(distinct id) as duplicate_ids,
    count(*) - count(id) as null_ids
from bronze.insurance_data;

--  total_rows | unique_customers | duplicate_ids | null_ids
-- ------------+------------------+---------------+----------
--      300000 |           300000 |             0 |        0
-- (1 row)


-- ============================================================
-- 2. Numerical Distribution — Extended (with median & stddev)
-- ============================================================
select
    -- =========================
    -- Age
    -- =========================
    min(age) as min_age,
    max(age) as max_age,
    round(avg(age), 2) as avg_age,
    percentile_cont(0.5) within group (order by age) as median_age,
    round(stddev(age), 2) as stddev_age,

    -- =========================
    -- Income
    -- =========================
    min(annual_income) as min_income,
    max(annual_income) as max_income,
    round(avg(annual_income), 2) as avg_income,
    percentile_cont(0.5) within group (order by annual_income) as median_income,
    round(stddev(annual_income), 2) as stddev_income,

    -- =========================
    -- Health Score
    -- =========================
    min(health_score) as min_health_score,
    max(health_score) as max_health_score,
    round(avg(health_score), 4) as avg_health_score,
    percentile_cont(0.5) within group (order by health_score) as median_health_score,

    -- =========================
    -- Vehicle Age
    -- =========================
    min(vehicle_age) as min_vehicle_age,
    max(vehicle_age) as max_vehicle_age,
    round(avg(vehicle_age), 2) as avg_vehicle_age,

    -- =========================
    -- Credit Score
    -- =========================
    min(credit_score) as min_credit_score,
    max(credit_score) as max_credit_score,
    round(avg(credit_score), 2) as avg_credit_score,
    percentile_cont(0.5) within group (order by credit_score) as median_credit_score,
    round(stddev(credit_score), 2) as stddev_credit_score

from bronze.insurance_data
where age is not null
  and annual_income is not null;

--  min_age | max_age | avg_age | median_age | stddev_age | min_income | max_income | avg_income | median_income | stddev_income | min_health_score | max_health_score  | avg_health_score | median_health_score | min_vehicle_age | max_vehicle_age | avg_vehicle_age | min_credit_score | max_credit_score | avg_credit_score | median_credit_score | stddev_credit_score
-- ---------+---------+---------+------------+------------+------------+------------+------------+---------------+---------------+------------------+-------------------+------------------+---------------------+-----------------+-----------------+-----------------+------------------+------------------+------------------+---------------------+---------------------
--       18 |      64 |   41.15 |         41 |      13.54 |          2 |     149997 |   32767.25 |         23939 |      32163.06 |      2.068451579 | 58.56968920333484 |          25.5592 |    24.5164654352762 |               0 |              19 |            9.56 |              300 |              849 |           595.51 |                 598 |              148.88
-- (1 row)

-- ============================================================
-- 3. Null / Missing Values — Full Coverage
-- ============================================================
select
    count(*) as total_rows,

    -- =========================
    -- Numeric fields (null counts)
    -- =========================
    sum(case when age is null then 1 else 0 end) as age_nulls,
    sum(case when annual_income is null then 1 else 0 end) as income_nulls,
    sum(case when credit_score is null then 1 else 0 end) as credit_score_nulls,
    sum(case when number_of_dependents is null then 1 else 0 end) as dependents_nulls,
    sum(case when health_score is null then 1 else 0 end) as health_score_nulls,
    sum(case when vehicle_age is null then 1 else 0 end) as vehicle_age_nulls,
    sum(case when insurance_duration is null then 1 else 0 end) as duration_nulls,
    sum(case when previous_claims is null then 1 else 0 end) as claims_nulls,

    -- =========================
    -- Categorical fields (null or empty)
    -- =========================
    sum(case when gender is null or gender = '' then 1 else 0 end) as gender_nulls,
    sum(case when marital_status is null or marital_status = '' then 1 else 0 end) as marital_status_nulls,
    sum(case when occupation is null or occupation = '' then 1 else 0 end) as occupation_nulls,
    sum(case when education_level is null or education_level = '' then 1 else 0 end) as education_nulls,
    sum(case when location is null or location = '' then 1 else 0 end) as location_nulls,
    sum(case when policy_type is null or policy_type = '' then 1 else 0 end) as policy_type_nulls,
    sum(case when smoking_status is null or smoking_status = '' then 1 else 0 end) as smoking_nulls,
    sum(case when exercise_frequency is null or exercise_frequency = '' then 1 else 0 end) as exercise_nulls,
    sum(case when property_type is null or property_type = '' then 1 else 0 end) as property_type_nulls,
    sum(case when customer_feedback is null or customer_feedback = '' then 1 else 0 end) as feedback_nulls

from bronze.insurance_data;

--  total_rows | age_nulls | income_nulls | credit_score_nulls | dependents_nulls | health_score_nulls | vehicle_age_nulls | duration_nulls | claims_nulls | gender_nulls | marital_status_nulls | occupation_nulls | education_nulls | location_nulls | policy_type_nulls | smoking_nulls | exercise_nulls | property_type_nulls | feedback_nulls
-- ------------+-----------+--------------+--------------------+------------------+--------------------+-------------------+----------------+--------------+--------------+----------------------+------------------+-----------------+----------------+-------------------+---------------+----------------+---------------------+----------------
--      300000 |      4545 |        11257 |              34516 |            27236 |              18526 |                 2 |              0 |        91008 |            0 |                 4640 |            89451 |               0 |              0 |                 0 |             0 |              0 |                   0 |          19403
-- (1 row)

-- ============================================================
-- 4. Categorical Cardinality — All Text Columns
-- ============================================================
select
    count(distinct gender) as gender_distinct,
    count(distinct marital_status) as marital_status_distinct,
    count(distinct policy_type) as policy_type_distinct,

    -- combined insight
    count(distinct education_level) + 
    count(distinct location) +
    count(distinct occupation) as total_work_context_diversity
from bronze.insurance_data;

--  gender_distinct | marital_status_distinct | policy_type_distinct | total_work_context_diversity
-- -----------------+-------------------------+----------------------+------------------------------
--                2 |                       3 |                    3 |                           10
-- (1 row)

-- ============================================================
-- 5. Categorical Value Distribution
-- ============================================================
select 'gender' as field, gender as value,
       count(*) as cnt,
       round(100.0 * count(*) / sum(count(*)) over(), 2) as pct
from bronze.insurance_data
group by gender

union all

select 'marital_status', marital_status,
       count(*),
       round(100.0 * count(*) / sum(count(*)) over(), 2)
from bronze.insurance_data
group by marital_status

union all

select 'policy_type', policy_type,
       count(*),
       round(100.0 * count(*) / sum(count(*)) over(), 2)
from bronze.insurance_data
group by policy_type;

--      field      |     value     |  cnt   |  pct
-- ----------------+---------------+--------+-------
--  gender         | Female        | 149387 | 49.80
--  gender         | Male          | 150613 | 50.20
--  marital_status | Divorced      |  97825 | 32.61
--  marital_status | Married       |  98767 | 32.92
--  marital_status | Single        |  98768 | 32.92
--  marital_status |               |   4640 |  1.55
--  policy_type    | Basic         |  99505 | 33.17
--  policy_type    | Comprehensive |  99714 | 33.24
--  policy_type    | Premium       | 100781 | 33.59
-- (9 rows)

-- ============================================================
-- 6. Outlier Detection — Values Outside Expected Ranges
-- ============================================================
select
    sum(case when age < 18 or age > 100 then 1 else 0 end) as invalid_age,
    sum(case when annual_income < 0 then 1 else 0 end) as negative_income,
    sum(case when annual_income > 1000000 then 1 else 0 end) as extreme_income,

    sum(case when credit_score < 300 then 1 else 0 end) as credit_below_min,
    sum(case when credit_score > 850 then 1 else 0 end) as credit_above_max,

    sum(case when health_score < 0 then 1 else 0 end) as invalid_health_score,

    sum(case when vehicle_age < 0 then 1 else 0 end) as invalid_vehicle_age,
    sum(case when vehicle_age > 30 then 1 else 0 end) as old_vehicle,

    sum(case when number_of_dependents < 0 then 1 else 0 end) as invalid_dependents,
    sum(case when number_of_dependents > 10 then 1 else 0 end) as high_dependents,

    sum(case when previous_claims < 0 then 1 else 0 end) as invalid_claims,
    sum(case when insurance_duration <= 0 then 1 else 0 end) as invalid_duration

from bronze.insurance_data;

--  invalid_age | negative_income | extreme_income | credit_below_min | credit_above_max | invalid_health_score | invalid_vehicle_age | old_vehicle | invalid_dependents | high_dependents | invalid_claims | invalid_duration
-- -------------+-----------------+----------------+------------------+------------------+----------------------+---------------------+-------------+--------------------+-----------------+----------------+------------------
--            0 |               0 |              0 |                0 |                0 |                    0 |                   0 |           0 |                  0 |               0 |              0 |                0
-- (1 row)

-- ============================================================
-- 7. Records Missing Multiple Fields (severity check)
-- ============================================================
select
    (
        case when age is null then 1 else 0 end +
        case when annual_income is null then 1 else 0 end +
        case when credit_score is null then 1 else 0 end +
        case when marital_status is null or marital_status = '' then 1 else 0 end +
        case when number_of_dependents is null then 1 else 0 end +
        case when occupation is null or occupation = '' then 1 else 0 end
    ) as missing_fields_count,

    count(*) as records,

    round(100.0 * count(*) / sum(count(*)) over(), 2) as pct

from bronze.insurance_data

group by missing_fields_count
order by missing_fields_count;

--  missing_fields_count | records |  pct
-- ----------------------+---------+-------
--                     0 |  157687 | 52.56
--                     1 |  115367 | 38.46
--                     2 |   24666 |  8.22
--                     3 |    2176 |  0.73
--                     4 |     102 |  0.03
--                     5 |       2 |  0.00
-- (6 rows)