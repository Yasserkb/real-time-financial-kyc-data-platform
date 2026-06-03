{% macro normalize_risk_band(score_expression) %}
    case
        when {{ score_expression }} >= 90 then 'CRITICAL'
        when {{ score_expression }} >= 75 then 'HIGH'
        when {{ score_expression }} >= 40 then 'MEDIUM'
        else 'LOW'
    end
{% endmacro %}
