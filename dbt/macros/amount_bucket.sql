{% macro amount_bucket(column) -%}
    case
        when {{ column }} < 100 then 'SMALL'
        when {{ column }} < 500 then 'MEDIUM'
        else 'LARGE'
    end
{%- endmacro %}
