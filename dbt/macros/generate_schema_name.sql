{#
  Use the model's custom schema as-is instead of prefixing it with the target
  schema. Both are "gold" here, and dbt's default would produce "gold_gold".
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
