{%- macro register_dataproduct() -%}
    {%- do edna_dbt_lib._validate_dataproduct() -%}
    {%- do edna_dbt_lib._register_dataproduct_metadata() -%}
{%- endmacro -%}
