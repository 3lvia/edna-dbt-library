{%- macro _dataproduct_shoulud_be_registered(product_id, dataprodconfig) -%}
  {%- if dataprodconfig is defined and dataprodconfig is not none -%}
    {{- return(true) -}}
  {%- else -%}
    {%- if _is_registered_dataproduct() -%}
      {{ exceptions.raise_compiler_error("Can't unregister dataproduct.") }}
    {%- endif -%}
    {{- return(false) -}}
  {%- endif -%}
{%- endmacro -%}
