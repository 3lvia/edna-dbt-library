{%- macro register_dataproduct() -%}
    {%- set is_validated_dataproduct = edna_dbt_lib._validate_dataproduct() -%}

    {% if is_validated_dataproduct %}
        {%- do edna_dbt_lib._register_dataproduct_metadata() -%}
    {% endif %}
{%- endmacro -%}
