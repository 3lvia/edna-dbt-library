{%- macro register_dataproduct() -%}
    {%- do enda_dbt_lib._validate_dataproduct() -%}
    {%- do enda_dbt_lib._register_dataproduct_metadata() -%}
{%- endmacro -%}
