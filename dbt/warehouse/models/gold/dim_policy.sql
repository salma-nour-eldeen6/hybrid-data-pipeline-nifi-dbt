-- models/gold/dim_policy.sql
-- Dimension: Policy Details
-- Grain: 1 row per customer (each customer has one active policy)
-- Source: bronze.insurance_silver

with source as (

    select * from {{ ref('insurance_silver') }}

)

select

    -- ==============================
    -- Surrogate Key
    -- ==============================
    {{ dbt_utils.generate_surrogate_key(['id']) }} as policy_sk,

    -- ==============================
    -- Natural Key
    -- ==============================
    id as customer_id,

    -- ==============================
    -- Policy Attributes
    -- ==============================
    policy_type,
    insurance_duration,
    customer_feedback,

    -- ==============================
    -- Policy Duration Segmentation
    -- ==============================
    case
        when insurance_duration <= 2  then 'new'
        when insurance_duration <= 5  then 'established'
        else 'long_term'
    end as policy_tenure_segment,

    -- ==============================
    -- Customer Feedback Encoding
    -- ==============================
    case
        when customer_feedback = 'poor' then 1
        when customer_feedback = 'average' then 2
        when customer_feedback = 'good' then 3
        else null  
    end as feedback_score

from source