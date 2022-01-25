{%- macro _create_tmp_relation(bq_project, identifier, compiled_sql) -%}
  {%- set tmp_id = identifier + '_tmp' -%}
  {%- set tmp_view_location -%}
    `{{ bq_project }}`.`dataplatform_internal`.`{{ tmp_id }}`
  {%- endset -%}
  {%- set create_tmp_view_cmd -%}
    create or replace view {{ tmp_view_location }} as
    {{ compiled_sql }}
  {%- endset -%}
  {%- do run_query(create_tmp_view_cmd) -%}
  {%- set tmp_relation = api.Relation.create(
      database=database,
      schema = 'dataplatform_internal',
      identifier = tmp_id
  )-%}
  {{ return(tmp_relation) }}
{%- endmacro -%}
