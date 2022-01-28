{% macro _validate_dataproduct() %}
    {% if execute %}

        {% set dataprodconfig = config.get('dataproduct') %}
        {% set is_registered = edna_dbt_lib._is_registered_dataproduct(this) %}

        {% if is_registered and not edna_dbt_lib.is_defined(dataprodconfig) %}
            {{ exceptions.raise_compiler_error("Can't unregister dataproduct.") }}
        {% endif %}

        {% if edna_dbt_lib.is_defined(dataprodconfig) %}
            {% do edna_dbt_lib._validate_dataproductconfig(dataprodconfig) %}
            {% do edna_dbt_lib._validate_is_in_dataproduct_dataset(this) %}

            {%- if is_registered -%}
                {%- do edna_dbt_lib._check_for_column_deletion(model.compiled_sql, this) -%}
            {%- endif -%}
        {% endif %}

    {% endif %}
{% endmacro %}

{% macro _validate_dataproductconfig(dataprodconfig) %}
    {%- set owner = dataprodconfig.get('owner')-%}
    {%- if not is_defined(owner) -%}
        {{ exceptions.raise_compiler_error("Dataproduct owner must be set") }}
    {%- endif -%}
{% endmacro %}

{% macro _validate_is_in_dataproduct_dataset(target_relation) %}
    {%- if target_relation.schema.split('_')[-1] == 'curated' -%}
        {{ exceptions.raise_compiler_error(
            "Models for registered dataproducts must be in a subfolder called dataproduct under your dataproductgroup. e.g: models/example/dataproduct/mymodel.sql") }}
    {%- endif -%}
{% endmacro %}

{% macro _is_registered_dataproduct(target_relation) %}
    {% set query %}
        select count(1) FROM dataplatform_internal.dataproducts
        where bigquery = ('{{ target_relation.schema }}', '{{ target_relation.identifier }}')
    {% endset %}

    {% set cnt = run_query(query).columns[0].values()[0] %}

    {{ return(cnt > 0) }}
{% endmacro %}

{% macro _check_for_column_deletion(compiled_sql, target_relation) %}
    {% set tmp_relation = edna_dbt_lib.create_tmp_relation(compiled_sql, target_relation) %}
    {% set missing_columns = adapter.get_missing_columns(target_relation, tmp_relation) %}
    {% do adapter.drop_relation(tmp_relation) %}
    {% if missing_columns | length > 0 %}
        {{ exceptions.raise_compiler_error("Schema of registered dataproduct can't be changed. Missing columns: " 
                                                ~ missing_columns
                                                | map(attribute="name")
                                                | join(', ')) }}
    {%- endif -%}
{%- endmacro -%}
