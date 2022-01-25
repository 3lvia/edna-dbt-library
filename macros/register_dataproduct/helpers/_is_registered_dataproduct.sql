{%- macro _is_registered_dataproduct(product_id) -%}
  {%- set query -%}
    SELECT COUNT(1) FROM dataplatform_internal.dataproducts
    WHERE id = '{{ product_id }}'
  {%- endset -%}
  {%- set cnt = run_query(query).columns[0].values()[0] -%}
  {%- if cnt > 0 -%}
    {{ return(true) }}
  {%- else -%}
    {{ return(false) }}
  {% endif %}
{%- endmacro -%}
