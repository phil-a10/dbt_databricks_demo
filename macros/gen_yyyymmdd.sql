{% macro gen_yyyymmdd (date_column_name) %}
    DATEPART('YEAR', {{ date_column_name }}) * 10000 + DATEPART('MONTH', {{ date_column_name }}) * 100 + DAY({{ date_column_name }})
{% endmacro %}