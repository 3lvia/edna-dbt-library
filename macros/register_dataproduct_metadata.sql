{% macro _register_dataproduct_metadata() %}
    {% if execute %}
        {% set dataprodconfig = config.get('dataproduct') %}

        {% set description = model.description %}
        {% set domain = project_name %}
        {% set dataproduct_group = config.model.path.split('/')[0] %}
        {% set bq_dataset = this.schema %}
        {% set bq_tablename = this.identifier %}
        {% set dbt_id = model.unique_id %}
        {% set owner = dataprodconfig.get('owner') %}

        {% set columns = edna_dbt_lib._get_formated_columns(model.compiled_sql, this) %}
        {% set labels = edna_dbt_lib._get_formated_labels(config.get('labels', default={})) %}

        {% do edna_dbt_lib._upsert_dataproduct_entry(description, domain, dataproduct_group,
                                        bq_dataset, bq_tablename, dbt_id, owner, columns, labels) %}
    {% endif %}
{% endmacro %}

{% macro _get_formated_columns(compiled_sql, target_relation) %}
    {% set tmp_relation = edna_dbt_lib.create_tmp_relation(compiled_sql, target_relation) %}

    {% set query %}
        select field_path, data_type, description
        from {{ tmp_relation.schema }}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
        where table_name = '{{ tmp_relation.identifier }}'
    {% endset %}

    {% set results = run_query(query) %}

    {% set columns = [] %}
    {% for row in results.rows %}
        {% do columns.append('({}, {}, {})'.format(row['field_path'], row['data_type'], row['description'])) %}
    {% endfor %}

    {% do adapter.drop_relation(tmp_relation) %}

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
            description, domain, dataproduct_group, bq_dataset, bq_tablename, dbt_id, owner, columns, labels) %}

    {% set query %}
        merge dataplatform_internal.dataproducts T
        using (select '{{ bq_dataset }}' as datasetId, '{{ bq_tablename }}' as table_name) S
        on T.bigquery.datasetId = S.datasetId and T.bigquery.tableId = S.table_name
        when matched then
            update set description = '{{ description }}', domain = '{{ domain }}',
                       dataproductGroup = '{{ dataproduct_group }}', dbtId = '{{ dbt_id }}', owner = '{{ owner }}',
                       lastUpdateTime = current_timestamp(), columns = {{ columns }}, labels = {{ labels }}
        when not matched then
            insert (id, description, domain, dataproductGroup, bigquery, dbtId,
                                    owner, registeredTime, lastUpdateTime, columns, labels)
            values (to_hex(md5('{{ "{}-{}".format(bq_dataset, bq_tablename) }}')),
                                    '{{ description }}', '{{ domain }}', '{{ dataproductGroup }}',
                                    ( '{{ bq_dataset }}', '{{ bq_tablename }}'), '{{ dbt_id }}', '{{ owner }}',
                                    current_timestamp(), current_timestamp(), {{ columns }}, {{ labels }} )
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}
