--- macros/insurance_macros.sql


-- 1. safe_divide
-- Division that returns null instead of crashing on zero
-- Usage: {{ safe_divide('numerator_col', 'denominator_col') }}
{% macro safe_divide(numerator, denominator) %}
    case
        when {{ denominator }} = 0 or {{ denominator }} is null
        then null
        else {{ numerator }}::numeric / {{ denominator }}::numeric
    end
{% endmacro %}
--- used in ltv_and_premium_analysis.sql for ltv_per_claim calculation


-- 2. composite_risk_score
-- Weighted risk score (0–100)
-- Usage: {{ composite_risk_score('credit_score', 'previous_claims', 'annual_income') }}
{% macro composite_risk_score(credit_col, claims_col, income_col) %}
    round(
        (least(greatest({{ credit_col }}, 300), 849) - 300) / 549.0 * 40
        + greatest(35 - (coalesce({{ claims_col }}, 0) * 7), 0)
        + least({{ income_col }}, 150000) / 150000.0 * 25
    , 2)
{% endmacro %}
--- used in dim_risk.sql to calculate composite risk score and risk tier classification

 