{% materialization incremental_log, adapter='bigquery', supported_languages=['sql'] %}
    {% set model_run_started_time = modules.datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f UTC') %}

    {% set bq_ids             = edna_dbt_lib.bq_ids_for_relation(this) %}
    {% set log_table_id       = bq_ids['log_table_id'] %}
    {% set target_table_id    = bq_ids['table_id'] %}

    {# === Required config === #}
    {% set run_window_column = edna_dbt_lib.get_config_or_meta(config, 'run_window_column', 'insertTime') %}
    {% if not log_table_id %}
        {% do exceptions.raise_compiler_error("incremental_log: `log_table_id` (project.dataset.table) is required.") %}
    {% endif %}
    {% if not run_window_column %}
        {% do exceptions.raise_compiler_error("incremental_log: `run_window_column` is required and must appear in your SELECT.") %}
    {% endif %}
    {% set run_window_col_ts = "SAFE_CAST(" ~ run_window_column ~ " AS TIMESTAMP)" %}
    {% set max_history_load_days = edna_dbt_lib.get_config_or_meta(config, 'max_history_load_days', none) %}
    {% set max_history_load_days_dev_ci = edna_dbt_lib.get_config_or_meta(config, 'max_history_load_days_dev_ci', none) %}



    {# BigQuery/core-aligned knobs #}
    {% set raw_partition_by = config.get('partition_by', none) %}
    {% set partition_by     = adapter.parse_partition_by(raw_partition_by) %}
    {% set cluster_by       = config.get('cluster_by', none) %}
    {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}
    {% set grant_config     = config.get('grants') %}

    {{ run_hooks(pre_hooks) }}

    {% set target_relation   = this %}
    {% set existing_relation = load_relation(this) %}
    {% set full_refresh_mode = should_full_refresh() %}

    {% if partition_by and partition_by.copy_partitions is true %}
        {% do exceptions.raise_compiler_error(
        "incremental_log: `copy_partitions=true` is only valid with insert_overwrite/microbatch, which this materialization does not use."
        ) %}
    {% endif %}

    {# Lower bound for incrementals: previous successful run's runWindowEnd #}
    {% set prev_run_window_end = edna_dbt_lib.get_last_successful_run_window_end(log_table_id, target_table_id) %}

    {# === Establish the run window === #}
    {% set current_run_window_end = edna_dbt_lib.apply_history_load_limit(max_history_load_days, prev_run_window_end, run_started_at, max_history_load_days_dev_ci) %}

    {# Log the start of THIS run #}
    {%- call statement('log_model_run_started') -%}
        {{ edna_dbt_lib.log_model_event(log_table_id, target_relation, 'model_run_started',  prev_run_window_end, current_run_window_end, ids=bq_ids, event_ts=model_run_started_time) }}
    {%- endcall -%}

    {# Upper bound (applies to all builds) #}
    {% set upper_bound_clause -%}
        {{ run_window_col_ts }} <= TIMESTAMP('{{ current_run_window_end }}')
    {%- endset %}

    {# Lower bound for incrementals: previous run start (we fetched it BEFORE logging this run) #}
    {% set lower_bound_clause -%}
        {{ run_window_col_ts }} > TIMESTAMP('{{ prev_run_window_end }}')
    {%- endset %}

    {% set ctx = (env_var('DBT_CLOUD_INVOCATION_CONTEXT', '') or '') | lower %}
    {% set is_dev_ci = ctx in ['dev', 'ci'] %}

    {# Pre-build the filtered SQL we need in both branches #}
    {% set filtered_sql_full %}
        with __src as (
            {{ compiled_code }}
        )
        select *
        from __src
        where {{ upper_bound_clause }}
    {% endset %}
    {% set filtered_sql_incr %}
        with __src as (
            {{ compiled_code }}
        )
        select *
        from __src
        where {{ upper_bound_clause }}
            and {{ lower_bound_clause }}
    {% endset %}

    {# === Build paths === #}
    {% if (existing_relation is none) or (existing_relation.is_view) or full_refresh_mode %}

        {# Drop when needed (view always; table if not replaceable on FR) #}
        {% if existing_relation is not none and existing_relation.is_view %}
            {{ adapter.drop_relation(existing_relation) }}
        {% elif full_refresh_mode and (existing_relation is not none)
                and (not adapter.is_replaceable(existing_relation, partition_by, cluster_by)) %}
            {% do log("Hard refreshing " ~ existing_relation ~ " because it is not replaceable") %}
            {{ adapter.drop_relation(existing_relation) }}
        {% endif %}

        {# Fresh create (apply only the upper bound) #}
        {%- call statement('main') -%}
            {{ bq_create_table_as(
                partition_by,
                False,
                target_relation,
                (filtered_sql_incr if is_dev_ci and not full_refresh_mode else filtered_sql_full)) }}
        {%- endcall -%}

        {%- call statement('log_model_run_succeeded') -%}
            {{ edna_dbt_lib.log_model_event(log_table_id, target_relation, 'model_run_succeeded', prev_run_window_end, current_run_window_end, ids=bq_ids) }}
        {%- endcall -%}


    {% else %}

        {# === Incremental append: rows written in (prev_run_window_end, current_run_window_end] === #}

        {% if on_schema_change != 'ignore' %}
            {# Build a temp table so we can reconcile schema robustly #}
            {% set tmp_relation      = make_temp_relation(this) %}
            {%- call statement('create_tmp') -%}
                {{ bq_create_table_as(partition_by, True, tmp_relation, filtered_sql_incr) }}
            {%- endcall -%}
            {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
            {% if partition_by and partition_by.time_ingestion_partitioning %}
                {% set dest_columns = adapter.add_time_ingestion_partition_column(partition_by, dest_columns) %}
            {% endif %}
            {% set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute='name')) %}

            {% call statement('main') %}
                insert into {{ target_relation }} ({{ dest_cols_csv }})
                select {{ dest_cols_csv }} from {{ tmp_relation }}
            {% endcall %}

            {%- call statement('log_model_run_succeeded') -%}
                {{ edna_dbt_lib.log_model_event(log_table_id, target_relation, 'model_run_succeeded', prev_run_window_end, current_run_window_end, ids=bq_ids) }}
            {%- endcall -%}


            {{ adapter.drop_relation(tmp_relation) }}

        {% else %}
            {# No schema change handling needed: insert directly from filtered SQL, but use explicit columns #}
            {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
            {% if partition_by and partition_by.time_ingestion_partitioning %}
                {% set dest_columns = adapter.add_time_ingestion_partition_column(partition_by, dest_columns) %}
            {% endif %}
            {% set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute='name')) %}

            {% call statement('main') %}
                insert into {{ target_relation }} ({{ dest_cols_csv }})
                select {{ dest_cols_csv }} from (
                    {{ filtered_sql_incr }}
                ) as __x
            {% endcall %}

            {%- call statement('log_model_run_succeeded') -%}
                {{ edna_dbt_lib.log_model_event(log_table_id, target_relation, 'model_run_succeeded', prev_run_window_end, current_run_window_end, ids=bq_ids) }}
            {%- endcall -%}

        {% endif %}

    {% endif %}

    {{ run_hooks(post_hooks) }}

    {% set target_relation = this.incorporate(type='table') %}
    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
    {% do apply_grants(target_relation, grant_config, should_revoke) %}
    {% do persist_docs(target_relation, model) %}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
