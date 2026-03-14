{% macro generate_schema_name(custom_schema_name, node) -%}
    {# Override to use exact schema names — prevents dbt from prepending target schema #}
    {%- if custom_schema_name is none -%}
        {{ target.schema | upper }}
    {%- else -%}
        {{ custom_schema_name | upper }}
    {%- endif -%}
{%- endmacro %}
