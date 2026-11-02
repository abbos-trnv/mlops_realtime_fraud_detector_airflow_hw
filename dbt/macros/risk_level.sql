{% macro risk_level(rate) -%}
    case
        when {{ rate }} >= 5 then 'HIGH'
        when {{ rate }} >= 1 then 'MEDIUM'
        else 'LOW'
    end
{%- endmacro %}
