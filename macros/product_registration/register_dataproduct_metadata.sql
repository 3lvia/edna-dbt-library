{% macro register_dataproduct_metadata() %}
    {% if execute %}
        {% set dataprodconfig = config.get('dataproduct') %}
        {% if edna_dbt_lib.is_defined(dataprodconfig) %}

            {% set description = edna_dbt_lib.quote_replace(model.description) %}
            {% set domain = project_name %}
            {% set dataproduct_group = config.model.path.split('/')[0] %}
            {% set bq_dataset = this.schema %}
            {% set bq_tablename = this.identifier %}
            {% set dbt_id = model.unique_id %}
            {% set owner = dataprodconfig.get('owner') %}
            {% set displayName = dataprodconfig.get('displayName') %}

            {% set columns = edna_dbt_lib._get_formated_columns(this) %}
            {% set labels = edna_dbt_lib._get_formated_labels(config.get('labels', default={})) %}

            {% set size_info = edna_dbt_lib._get_sizeinfo(this) %}

            {% if not edna_dbt_lib.is_defined(displayName) %}
                {% set displayName = model.name %}
            {% endif %}

            {% set preview_where_clause = dataprodconfig.get('previewWhereClause') %}
            {% set version = dataprodconfig.get('version') %}
            {% set versionDescription = dataprodconfig.get('versionDescription') %}

            {% do edna_dbt_lib._upsert_dataproduct_entry(description, displayName, domain, dataproduct_group,
                                bq_dataset, bq_tablename, dbt_id, owner, columns, labels, size_info, preview_where_clause, version, versionDescription, model.name) %}
            
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro _get_sizeinfo(target_relation) %}
    {% set query %}
        select row_count, size_bytes, type
        from `{{ target_relation.schema }}.__TABLES__`
        where table_id = '{{ target_relation.identifier }}'
    {% endset %}

    {% set rows = run_query(query).rows %}

    {% if rows | count > 0 %}
        {% set info = { 'row_count': rows[0]['row_count'] | int, 'size_bytes': rows[0]['size_bytes'] | int } %}
    {% endif %}

    {{ return( info | default({ 'row_count': 'NULL', 'size_bytes': 'NULL' })) }}
{% endmacro %}

{% macro _get_formated_columns(target_relation) %}
    {% set query %}
        select field_path, data_type, description
        from {{ target_relation.schema }}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
        where table_name = '{{ target_relation.identifier }}'
    {% endset %}

    {% set results = run_query(query) %}

    {% set model_definition_columns = config.model.columns if edna_dbt_lib.is_defined(config.model.columns) else {} %}
    {% set columns = [] %}
    {% for row in results.rows %}
        {% set model_column = model_definition_columns.get(row['field_path']) %}
        {% if edna_dbt_lib.is_defined(model_column.description) %}
            {% set description = model_column.description %}
        {% else %}
            {% set description = '' %}
        {% endif %}

        {% do columns.append("('{}', '{}', '{}')".format(row['field_path'], row['data_type'], edna_dbt_lib.quote_replace(description))) %}
    {% endfor %}

    {{ return('[{}]'.format(columns | join(', '))) }}
{% endmacro %}

{% macro _get_formated_labels(label_dict) %}
    {% set labels = [] %}
    {% for key, value in label_dict.items() %}
        {%- do labels.append('("{}", "{}")'.format(key, value)) -%}
    {% endfor %}

    {{ return('[{}]'.format(labels | join(', '))) }}
{% endmacro %}

{% macro _upsert_dataproduct_entry(
            description, display_name, domain, dataproduct_group, bq_dataset, bq_tablename, dbt_id, owner,
            columns, labels, size_info, preview_where_clause, version, versionDescription, name) %}

    {% set query %}
        merge dataplatform_internal.dataproducts T
        using (select '{{ bq_dataset }}' as datasetId, '{{ bq_tablename }}' as table_name) S
        on T.bigquery.datasetId = S.datasetId and T.bigquery.tableId = S.table_name
        when matched then
            update set 
                id = (to_hex(md5('{{ "{}-{}".format(bq_dataset, name) }}')),
                description = '{{ description }}', 
                name = '{{ display_name }}',
                domain = '{{ domain }}',
                dataproductGroup = '{{ dataproduct_group }}',
                dbtId = '{{ dbt_id }}',
                owner = '{{ owner }}',
                lastUpdateTime = current_timestamp(),
                columns = {{ columns }},
                labels = {{ labels }},
                rowCount = {{ size_info.get('row_count') }},
                sizeInBytes = {{ size_info.get('size_bytes')}},
                previewWhereClause = {{ edna_dbt_lib._string_or_null(preview_where_clause) }},
                version = {{ edna_dbt_lib._string_or_null(version | string ) }},
                versionDescription = {{ edna_dbt_lib._string_or_null(versionDescription) }}
        when not matched then
            insert (id, description, name, domain, dataproductGroup, bigquery, dbtId,
                    owner, registeredTime, lastUpdateTime, columns, labels, rowCount, sizeInBytes,
                    previewWhereClause, version, versionDescription)
            values 
                (to_hex(md5('{{ "{}-{}".format(bq_dataset, name) }}')),
                '{{ description }}',
                '{{ display_name }}',
                '{{ domain }}', 
                '{{ dataproduct_group }}',
                ( '{{ bq_dataset }}', '{{ bq_tablename }}'),
                '{{ dbt_id }}',
                '{{ owner }}',
                current_timestamp(),
                current_timestamp(),
                {{ columns }},
                {{ labels }},
                {{ size_info.get('row_count') }},
                {{ size_info.get('size_bytes')}},
                {{ edna_dbt_lib._string_or_null(preview_where_clause) }},
                {{ edna_dbt_lib._string_or_null(version | string) }},
                {{ edna_dbt_lib._string_or_null(versionDescription) }} )
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}
