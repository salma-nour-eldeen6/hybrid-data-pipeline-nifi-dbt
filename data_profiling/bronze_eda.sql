-- ============================================================
-- 1. Row Count & Uniqueness
-- ============================================================
select
    count(*) as total_rows,
    count(distinct id) as unique_customers,
    count(*) - count(distinct id) as duplicate_ids
from raw_bronze.insurance_bronze;

--  total_rows | unique_customers | duplicate_ids
-- ------------+------------------+---------------
--      300000 |           300000 |             0
-- (1 row)

-- ============================================================
-- 2. Numerical Summary 
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

    -- Health Score
    min(health_score) as min_health_score,
    max(health_score) as max_health_score,
    round(avg(health_score), 2) as avg_health_score,

    -- Vehicle Age
    min(vehicle_age) as min_vehicle_age,
    max(vehicle_age) as max_vehicle_age,
    round(avg(vehicle_age), 2) as avg_vehicle_age,

    -- Credit Score
    min(credit_score) as min_credit_score,
    max(credit_score) as max_credit_score,
    round(avg(credit_score), 2) as avg_credit_score

from raw_bronze.insurance_bronze;

--  min_age | max_age | avg_age | min_income | max_income | avg_income | min_health_score | max_health_score | avg_health_score | min_vehicle_age | max_vehicle_age | avg_vehicle_age | min_credit_score | max_credit_score | avg_credit_score
-- ---------+---------+---------+------------+------------+------------+------------------+------------------+------------------+-----------------+-----------------+-----------------+------------------+------------------+------------------
--       18 |      64 |   41.14 |          2 |     149997 |   32767.88 |      2.068451579 | 58.5696892033348 |            25.60 |               0 |              19 |            9.56 |              300 |              849 |           593.09
-- (1 row)

-- ============================================================
-- 3. Null Values Check 
-- ============================================================
select
    sum(case when age is null then 1 else 0 end) as age_nulls,
    sum(case when annual_income is null then 1 else 0 end) as income_nulls,
    sum(case when credit_score is null then 1 else 0 end) as credit_score_nulls,
    sum(case when number_of_dependents is null then 1 else 0 end) as dependents_nulls,

    sum(case when gender is null or gender = '' then 1 else 0 end) as gender_nulls,
    sum(case when marital_status is null or marital_status = '' then 1 else 0 end) as marital_nulls,
    sum(case when occupation is null or occupation = '' then 1 else 0 end) as occupation_nulls,
    sum(case when location is null or location = '' then 1 else 0 end) as location_nulls

from raw_bronze.insurance_bronze;

--  age_nulls | income_nulls | credit_score_nulls | dependents_nulls | gender_nulls | marital_nulls | occupation_nulls | location_nulls
-- -----------+--------------+--------------------+------------------+--------------+---------------+------------------+----------------
--          0 |            0 |                  0 |                0 |            0 |             0 |                0 |              0
-- (1 row)

-- ============================================================
-- 4. Categorical Uniqueness (Cardinality Check)
-- ============================================================
select
    count(distinct gender) as gender_distinct,
    count(distinct marital_status) as marital_status_distinct,
    count(distinct policy_type) as policy_type_distinct,
    count(distinct education_level) as education_level_distinct,
    count(distinct location) as location_distinct,
    count(distinct occupation) as occupation_distinct
from raw_bronze.insurance_bronze;


--  gender_distinct | marital_status_distinct | policy_type_distinct | education_level_distinct | location_distinct | occupation_distinct
-- -----------------+-------------------------+----------------------+--------------------------+-------------------+---------------------
--                2 |                       4 |                    3 |                        4 |                 3 |                   4
-- (1 row)

-- ============================================================
-- 5. Data Validity / Outliers Check
-- ============================================================
select
    sum(case when age < 18 or age > 100 then 1 else 0 end) as invalid_age,
    sum(case when annual_income < 0 then 1 else 0 end) as negative_income,
    sum(case when credit_score < 300 then 1 else 0 end) as low_credit_score,
    sum(case when credit_score > 850 then 1 else 0 end) as high_credit_score,
    sum(case when vehicle_age < 0 then 1 else 0 end) as invalid_vehicle_age,
    sum(case when number_of_dependents < 0 then 1 else 0 end) as invalid_dependents,
    sum(case when insurance_duration <= 0 then 1 else 0 end) as invalid_duration
from raw_bronze.insurance_bronze;

--  invalid_age | negative_income | low_credit_score | high_credit_score | invalid_vehicle_age | invalid_dependents | invalid_duration
-- -------------+-----------------+------------------+-------------------+---------------------+--------------------+------------------
--            0 |               0 |                0 |                 0 |                   0 |                  0 |                0
-- (1 row)

-- ============================================================
-- 6. Category Distributio 
-- ============================================================
select
    'gender' as field, gender as value, count(*) as cnt
from raw_bronze.insurance_bronze
group by gender

union all

select
    'marital_status', marital_status, count(*)
from raw_bronze.insurance_bronze
group by marital_status

union all

select
    'policy_type', policy_type, count(*)
from raw_bronze.insurance_bronze
group by policy_type

union all

select
    'location', location, count(*)
from raw_bronze.insurance_bronze
group by location

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
--  policy_type    | premium       | 100781
--  policy_type    | comprehensive |  99714
--  policy_type    | basic         |  99505
-- (12 rows)

-- insurance_dw=#