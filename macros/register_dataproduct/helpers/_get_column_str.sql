{%- macro _get_column_str(bq_columns, model_definition_columns) -%}
  {%- set cols = [] -%}
  {%- for column in bq_columns-%}
    {%- set model_column = model_definition_columns.get(column.name) -%}
    {%- if model_column.description is defined -%}
      {%- set description = odel_column.description -%}
    {% else %}
      {%- set description = '' -%}
    {%- endif -%}
    {%- do cols.append('("{}", "{}", "{}")'.format(column.name, column.data_type, description)) -%}
  {%- endfor-%}
  {{ return('[{}]'.format(cols | join(', '))) }}
{%- endmacro -%}
