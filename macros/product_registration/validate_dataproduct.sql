{% macro validate_dataproduct() %}
    {% if execute %}

        {% set dataprodconfig = config.get('dataproduct') %}
        {% set is_registered = edna_dbt_lib._is_registered_dataproduct(this) %}

        {% if is_registered and not edna_dbt_lib.is_defined(dataprodconfig) %}
            {{ exceptions.raise_compiler_error("Can't unregister dataproduct.") }}
        {% endif %}

        {% if edna_dbt_lib.is_defined(dataprodconfig) %}
            {% do edna_dbt_lib._validate_dataproductconfig(dataprodconfig) %}
            {% do edna_dbt_lib._validate_is_in_dataproduct_dataset(this) %}

            {% if not edna_dbt_lib.is_defined(model.description) %}
                {{ exceptions.raise_compiler_error("Dataproducts must have a description") }}
            {% endif %}

            {%- do edna_dbt_lib._check_for_column_deletion_and_descriptions(model.compiled_sql, this, is_registered) -%}

        {% endif %}

    {% endif %}
{% endmacro %}

{% macro _validate_dataproductconfig(dataprodconfig) %}
    {%- set owner = dataprodconfig.get('owner')-%}
    {%- if not edna_dbt_lib.is_defined(owner) -%}
        {{ exceptions.raise_compiler_error("Dataproduct owner must be set") }}
    {%- endif -%}
    {%- set preview_limit = dataprodconfig.get('previewLimit') -%}
    {%- if edna_dbt_lib.is_defined(preview_limit) -%}
        {%- do edna_dbt_lib._validate_whereclause(preview_limit) -%}
    {%- endif -%}
{% endmacro %}

{% macro _validate_preview_limit(preview_limit) %}
    {%- set query = 'select * from ({}) where {}'.format(model.compiled_sql, preview_limit) -%}
    {%- set tmp_relation = edna_dbt_lib.create_tmp_relation(query, this) -%}
    {%- do adapter.drop_relation(tmp_relation) -%}
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

{% macro _check_for_column_deletion_and_descriptions(compiled_sql, target_relation, is_registered) %}
    {% set tmp_relation = edna_dbt_lib.create_tmp_relation(compiled_sql, target_relation) %}

    {% set all_columns = adapter.get_columns_in_relation(tmp_relation) %}
    {% set missing_columns = adapter.get_missing_columns(target_relation, tmp_relation) %}
    
    {% do adapter.drop_relation(tmp_relation) %}
    
    {% if is_registered  and missing_columns | length > 0 %}
        {{ exceptions.raise_compiler_error("Schema of registered dataproduct can't be changed. Missing columns: " 
                                                ~ missing_columns
                                                | map(attribute="name")
                                                | join(', ')) }}
    {%- endif -%}

    {% set model_definition_columns = config.model.columns if edna_dbt_lib.is_defined(config.model.columns) else {} %}
    {% for column in all_columns %}
        {% set model_column = model_definition_columns.get(column.name) %}
        {% if not edna_dbt_lib.is_defined(model_column.description) %}
            {{ exceptions.raise_compiler_error("Dataproduct columns must have a description, missing description for {}".format(column.name)) }}
        {% endif %}
    {% endfor %}

{%- endmacro -%}
