{%- macro _get_columns_and_check_for_column_deletion(is_registered, bq_project, identifier, compiled_sql) -%}
  {%- set tmp_relation = edna_dbt_lib._create_tmp_relation(bq_project, identifier, compiled_sql) -%}
  {%- set new_columns = adapter.get_columns_in_relation(tmp_relation) -%}
  {%- if is_registered_dataproduct -%}
    {%- set missing_columns = adapter.get_missing_columns(this, tmp_relation) -%}
    {%- do adapter.drop_relation(tmp_relation) -%}
    {%- if missing_columns | length > 0 -%}
      {#- Should we allow flag to force change of dataproduct? -#}
      {{ exceptions.raise_compiler_error("Schema of registered dataproduct can't be changed. Missing columns: " ~ missing_columns | map(attribute="name") |join(', ')) }}
    {%- endif -%}
  {%- endif -%}
  {{- return(new_columns) -}}
{%- endmacro -%}
