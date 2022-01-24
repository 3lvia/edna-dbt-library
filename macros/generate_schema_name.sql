{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- set dataproduct_group = node.path.split('/')[0] -%}
  {%- if node.path.split('/')[-1] == 'dataproduct' -%}
    {{ project_name }}_{{ dataproduct_group }}
  {%- else -%}
    {{ project_name }}_{{ dataproduct_group }}_curated
  {% endif %}
{%- endmacro %}
