{#
  By default dbt prefixes custom schemas with the target schema (analytics_staging).
  This override uses the custom schema name as-is, so models land in `staging` and `marts`.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
