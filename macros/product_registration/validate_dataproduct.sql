{% macro validate_dataproduct() %}
    {% if execute %}
        {% set is_registered = edna_dbt_lib._is_registered_dataproduct(this) %}

        {%- set dataproduct_config = config.get('dataproduct') -%}
        {%- set is_dataproduct = edna_dbt_lib.is_defined(dataproduct_config) or config.get('datacatalog', False) -%}

        {% if is_registered and not is_dataproduct %}
            {{ exceptions.raise_compiler_error("Can't unregister dataproduct.") }}
        {% endif %}

        {%- if is_dataproduct -%}
            {%- set dataproduct_owner = (dataproduct_config.get('owner') if edna_dbt_lib.is_defined(dataproduct_config) else config.get('group')) -%}

            {%- if not edna_dbt_lib.is_defined(dataproduct_owner) -%}
                {{ exceptions.raise_compiler_error("Dataproduct owner must be set") }}
            {%- endif -%}

            {%- set preview_where_clause = dataproduct_config.get('previewWhereClause') if edna_dbt_lib.is_defined(dataproduct_config) else config.get('previewWhereClause') -%}
            {%- if edna_dbt_lib.is_defined(preview_where_clause) -%}
                {%- do edna_dbt_lib._validate_preview_where_clause(preview_where_clause) -%}
            {%- endif -%}

            {%- set version = dataproduct_config.get('version') if edna_dbt_lib.is_defined(dataproduct_config) -%}
            {%- if edna_dbt_lib.is_defined(version) -%}
                {%- do edna_dbt_lib._validate_semantic_versioning(version) -%}
            {%- endif -%}

            {% do edna_dbt_lib._validate_is_in_dataproduct_dataset(this) %}
            {% if not edna_dbt_lib.is_defined(model.description) %}
                {{ exceptions.raise_compiler_error("Dataproducts must have a description") }}
            {% endif %}

            {%- do edna_dbt_lib._check_for_column_deletion_and_descriptions(model.compiled_sql, this, is_registered) -%}
        {%- endif -%}
    {% endif %}
{% endmacro %}

{% macro _validate_preview_where_clause(preview_where_clause) %}
    {%- set query = 'select * from ({}) where {}'.format(model.compiled_sql, preview_where_clause) -%}
    {%- set tmp_relation = edna_dbt_lib.create_tmp_relation(query, this) -%}
    {%- do adapter.drop_relation(tmp_relation) -%}
{% endmacro %}

{% macro _validate_is_in_dataproduct_dataset(target_relation) %}
    {% set deployed_relation = edna_dbt_lib.get_deployed_relation(target_relation) %}

    {%- if deployed_relation.schema.split('_')[-1] == 'curated' -%}
        {{ exceptions.raise_compiler_error(
            "Models for registered dataproducts must be in a subfolder called dataproduct under your dataproductgroup. e.g: models/example/dataproduct/mymodel.sql") }}
    {%- endif -%}
{% endmacro %}

{% macro _is_registered_dataproduct(target_relation) %}
    {% set deployed_relation = edna_dbt_lib.get_deployed_relation(target_relation) %}
    {% set query %}
        select count(1) FROM dataplatform_internal.dataproducts
        where bigquery = ('{{ deployed_relation.schema }}', '{{ deployed_relation.identifier }}')
    {% endset %}

    {% set cnt = run_query(query).columns[0].values()[0] %}

    {{ return(cnt > 0) }}
{% endmacro %}

{% macro _check_for_column_deletion_and_descriptions(compiled_sql, target_relation, is_registered) %}
    {% set deployed_relation = edna_dbt_lib.get_deployed_relation(target_relation) %}
    {% set tmp_relation = edna_dbt_lib.create_tmp_relation(compiled_sql, target_relation) %}

    {% set new_columns = edna_dbt_lib._get_columns_from_relation(tmp_relation) %}
    {% do adapter.drop_relation(tmp_relation) %}

    {% set old_columns = edna_dbt_lib._get_columns_from_relation(deployed_relation) %}

    {% set missing_columns = edna_dbt_lib._get_missing_columns(old_columns, new_columns) %}
    
    {% if is_registered and missing_columns | length > 0 %}
        {{ exceptions.raise_compiler_error("Schema of registered dataproduct can't be changed. Missing columns: " 
                                                ~ missing_columns
                                                | map(attribute="name")
                                                | join(', ')) }}
    {%- endif -%}

    {% set model_definition_columns = config.model.columns if edna_dbt_lib.is_defined(config.model.columns) else {} %}
    {% for column in new_columns %}
        {% set model_column = model_definition_columns.get(column.name) %}
        {% if not edna_dbt_lib.is_defined(model_column.description) %}
            {{ exceptions.raise_compiler_error("Dataproduct columns must have a description, missing description for {}".format(column.name)) }}
        {% endif %}
    {% endfor %}
{%- endmacro -%}

{% macro _get_missing_columns(target_columns, new_columns) %}
    {% set missing_columns = [] %}
    {% for column in target_columns %}
        {% if not column in new_columns and not ('RECORD' in column.dtype or 'STRUCT' in column.dtype) %}
            {% do missing_columns.append(column) %}
        {% endif %}
    {% endfor %}
    {{ return(missing_columns) }}
{% endmacro %}

{% macro _get_columns_from_relation(relation) %}
    {% set query %}
        select field_path, data_type
        from {{ relation.schema }}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
        where table_name = '{{ relation.identifier }}'
    {% endset %}

    {% set results = run_query(query) %}

    {% set columns = [] %}
    {% for row in results %}
        {% do columns.append(api.Column(row['field_path'], row['data_type'])) %}
    {% endfor %}

    {{ return(columns) }}
{% endmacro %}

{% macro _validate_semantic_versioning(v) %}
    {% set v = v | string %}
    {% set parts = v.split(".") %}
    {% if parts | length > 4 or parts | length < 2 %}
        {{ exceptions.raise_compiler_error("Version string portion was too short or too long. Use format: major.minor.(build).(revision)") }}
    {% endif %}
    {% for part in parts %}
        {% if part | int(-1) < 0 %}
            {{ exceptions.raise_compiler_error("each part of version must be a number. '" ~ part ~ "' is not a number") }}
        {% endif %}
    {% endfor %}
{% endmacro %}
