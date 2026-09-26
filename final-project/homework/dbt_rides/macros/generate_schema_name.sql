{#-
  Без цього макросу dbt клеїть схему як `<target.schema>_<custom>`, тобто `public_silver`.
  Нам потрібні рівно `silver` / `gold` / `reference` — імена шарів є частиною контракту.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
