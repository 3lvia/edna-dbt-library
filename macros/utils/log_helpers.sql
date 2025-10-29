{# --- Resolve BQ ids from a Relation --- #}
{% macro bq_ids_for_relation(relation) %}
    {%- set domain     = model.fqn[0] -%}
    {%- set project_id = relation.database -%}
    {%- set dataset_id = relation.schema -%}
    {%- set table_id   = project_id ~ '.' ~ dataset_id ~ '.' ~ relation.identifier -%}
    {%- set log_table_dataset_id = domain ~ '_dbt_raw' -%}
    {%- set log_table_name = domain ~ '_dbt_event_log' -%}
    {%- set log_table_id   = project_id ~ '.' ~ log_table_dataset_id ~ '.' ~ log_table_name -%}

    {{ return({
        'domain': domain,
        'project_id': project_id,
        'dataset_id': dataset_id,
        'table_id': table_id,
        'log_table_id': log_table_id
        }) }}
{% endmacro %}

{# Insert a model-scoped event row. #}
{% macro log_model_event(log_table_id, relation, event_type, window_start, window_end, ids=None, event_ts=None, message=None) %}
    {# Don't record CI/dev empty runs #}
    {% if flags.EMPTY %}
        {% do log("log_model_event: --empty detected; skipping log write for " ~ event_type ~ " on " ~ relation, info=True) %}
        {{ return("select 1 as empty_run_logging_skipped limit 0") }}
    {% endif %}
    
    {% set allowed = ['model_run_started','model_run_succeeded','model_run_failed'] %}
    {% if event_type not in allowed %}
        {% do exceptions.raise_compiler_error("log_model_event: invalid event_type '" ~ event_type ~ "'.") %}
    {% endif %}

    {% if not ids %}
        {% set ids = edna_dbt_lib.bq_ids_for_relation(relation) %}
    {% endif %}

    {# if you added this helper; otherwise keep your inline env_var handling #}
    {% set cloud = edna_dbt_lib.cloud_env_sql_values() %}

    {# use early-captured timestamp if provided; else now() #}
    {% set event_ts_sql = "TIMESTAMP('" ~ event_ts ~ "')" if event_ts else "CURRENT_TIMESTAMP()" %}

    {# message: escape single quotes for BigQuery string literal ('' inside '') #}
    {% set event_message_sql = "'" ~ (message | replace("'", "''")) ~ "'" if (message is defined and message) else 'NULL' %}

    insert into `{{ log_table_id }}`
    select
        GENERATE_UUID()                    as eventId,
        {{ event_ts_sql }}                 as eventTimestamp,
        '{{ event_type }}'                 as eventType,

        '{{ ids['project_id'] }}'          as bigQueryProjectId,
        '{{ ids['dataset_id'] }}'          as bigQueryDatasetId,
        '{{ ids['table_id'] }}'            as bigQueryTableId,

        '{{ model.name }}'                 as dbtModelName,
        '{{ model.unique_id }}'            as dbtNodeUniqueId,

        '{{ invocation_id }}'              as dbtInvocationId,
        {{ cloud.invocation_context }}     as dbtInvocationContext,
        {{ cloud.job_id }}                 as dbtJobId,
        {{ cloud.run_id }}                 as dbtRunId,
        {{ cloud.run_reason_cat }}         as dbtRunReasonCategory,
        {{ cloud.run_reason }}             as dbtRunReason,
        {{ cloud.git_sha }}                as dbtGitSha,

        {{ 'TIMESTAMP("' ~ window_start ~ '")' if window_start else 'NULL' }} as runWindowStart,
        {{ 'TIMESTAMP("' ~ window_end   ~ '")' if window_end   else 'NULL' }} as runWindowEnd,

        {{ event_message_sql }}            as eventMessage,

        CURRENT_TIMESTAMP()                as insertTime
    ;
{% endmacro %}

{# Latest successful run's window end time (stored as runWindowEnd for that run in the log table). #}
{% macro get_last_successful_run_window_end(log_table_id, table_id, default='0001-01-01 00:00:00 UTC') %}
    {% set ctx = (env_var('DBT_CLOUD_INVOCATION_CONTEXT', '') or '') | lower %}
    {% set is_dev_ci = ctx in ['dev', 'ci'] %}
    {% set source_dataset_id = config.get('source_dataset_id', none) %}
    {% set source_table_id = config.get('source_table_id', none) %}

    {% set parts = table_id.split('.') %}
    {% if parts | length != 3 %}
        {% do exceptions.raise_compiler_error("get_last_successful_run_window_end: table_id must be 'project.dataset.table' but was '" ~ table_id ~ "'") %}
    {% endif %}
    {% set project_id   = parts[0] %}
    {% set table_name   = parts[2] %}

    {% set f0 = model.fqn[0] %}
    {% set f1 = model.fqn[1] %}
    {% set f2 = model.fqn[2] %}

    {% set ds_suffix = '' if f2 == 'dataproduct' else ('_' ~ f2) %}
    {% set actual_dataset = f0 ~ '_' ~ f1 ~ ds_suffix %}

    {% set logged_table = (project_id ~ '.' ~ actual_dataset ~ '.' ~ table_name) if is_dev_ci else table_id %}

    {%- set q -%}
        select runWindowEnd
        from `{{ log_table_id }}`
        where bigQueryTableId = '{{ logged_table }}'
            and eventType = 'model_run_succeeded'
            and runWindowEnd is not null
        qualify row_number() over (order by runWindowEnd desc) = 1
    {%- endset -%}
    {%- set ts = dbt_utils.get_single_value(q) -%}
    {% if ts is none and is_dev_ci %}
        {% set dt = modules.datetime.datetime.utcnow() - modules.datetime.timedelta(hours=24) %}
        {{ return(dt.strftime('%Y-%m-%d %H:%M:%S.%f UTC')) }}
    {% elif ts is none and source_table_id is not none %}
        {{ return(get_earliest_partition_timestamp(project_id, source_dataset_id, source_table_id)) }}
    {% else %}
        {{ return(ts or default) }}
    {% endif %}
{% endmacro %}

{# Return SQL-safe values for optional dbt Cloud env vars ('value' or NULL) #}
{% macro cloud_env_sql_values() %}
    {% set cloud_inv_ctx  = env_var('DBT_CLOUD_INVOCATION_CONTEXT', '') %}
    {% set cloud_job_id   = env_var('DBT_CLOUD_JOB_ID', '') %}
    {% set cloud_run_id   = env_var('DBT_CLOUD_RUN_ID', '') %}
    {% set cloud_reason_c = env_var('DBT_CLOUD_RUN_REASON_CATEGORY', '') %}
    {% set cloud_reason   = env_var('DBT_CLOUD_RUN_REASON', '') %}
    {% set cloud_git_sha  = env_var('DBT_CLOUD_GIT_SHA', '') %}

    {{ return({
        'invocation_context': ("'" ~ cloud_inv_ctx  ~ "'") if cloud_inv_ctx  else 'NULL',
        'job_id':             ("'" ~ cloud_job_id   ~ "'") if cloud_job_id   else 'NULL',
        'run_id':             ("'" ~ cloud_run_id   ~ "'") if cloud_run_id   else 'NULL',
        'run_reason_cat':     ("'" ~ cloud_reason_c ~ "'") if cloud_reason_c else 'NULL',
        'run_reason':         ("'" ~ cloud_reason   ~ "'") if cloud_reason   else 'NULL',
        'git_sha':            ("'" ~ cloud_git_sha  ~ "'") if cloud_git_sha  else 'NULL'
    }) }}
{% endmacro %}

{# Wrapper macros for logging model events in pre/post hooks #}
{% macro log_model_run_started_pre_hook(relation=this, message=None, max_history_load_days=None) %}
    {% set started_ts = modules.datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f UTC') %}
    {% set ids = edna_dbt_lib.bq_ids_for_relation(relation) %}

    {% set window_start = edna_dbt_lib.get_last_successful_run_window_end(ids['log_table_id'], ids['table_id']) %}
    {% set window_end = edna_dbt_lib.apply_history_load_limit(max_history_load_days, window_start, run_started_at) %}

    {{ edna_dbt_lib.log_model_event(
        ids['log_table_id'],
        relation,
        'model_run_started',
        window_start,
        window_end,
        ids=ids,
        event_ts=started_ts,
        message=message
        )
    }}
{% endmacro %}

{% macro log_model_run_succeeded_post_hook(relation=this, message=None, max_history_load_days=None) %}
    {% set ids = edna_dbt_lib.bq_ids_for_relation(relation) %}

    {% set window_start = edna_dbt_lib.get_last_successful_run_window_end(ids['log_table_id'], ids['table_id']) %}
    {% set window_end = edna_dbt_lib.apply_history_load_limit(max_history_load_days, window_start, run_started_at) %}

    {{ edna_dbt_lib.log_model_event(
        ids['log_table_id'],
        relation,
        'model_run_succeeded',
        window_start,
        window_end,
        ids=ids,
        message=message)
    }}
{% endmacro %}

{# Apply history load limit to window_end if configured #}
{% macro apply_history_load_limit(max_history_load_days, window_start, window_end=run_started_at) %}
    {% if max_history_load_days %}
        {% set load_days = max_history_load_days | int %}
        {% if load_days > 0 and window_start %}
            {% set max_load_end = modules.datetime.datetime.strptime(window_start, '%Y-%m-%d %H:%M:%S.%f UTC') + modules.datetime.timedelta(days=load_days) %}
            {% if window_end is string %}
                {% set window_end_dt = modules.datetime.datetime.strptime(window_end, '%Y-%m-%d %H:%M:%S.%f UTC') %}
            {% else %}
                {% set window_end_dt = window_end.replace(tzinfo=None) if window_end.tzinfo else window_end %}
            {% endif %}
            {% if max_load_end < window_end_dt %}
                {% set window_end = max_load_end.strftime('%Y-%m-%d %H:%M:%S.%f UTC') %}
            {% endif %}
        {% endif %}
    {% endif %}
    {{ return(window_end) }}
{% endmacro %}

{# Get the earliest partition timestamp from INFORMATION_SCHEMA.PARTITIONS for a given table #}
{% macro get_earliest_partition_timestamp(project_id, dataset_id, table_name) %}
    {%- set q -%}
        select
            min(partition_id) AS earliest_partition
        from
            `{{ project_id }}.{{ dataset_id }}.INFORMATION_SCHEMA.PARTITIONS`
        where
            table_name = '{{ table_name }}'
            and partition_id is not null
            and partition_id != '__NULL__'
            and partition_id != '__UNPARTITIONED__'
    {%- endset -%}
    {%- set partition_id = dbt_utils.get_single_value(q) -%}
    {% if partition_id and partition_id | length == 8 %}
        {# Assume YYYYMMDD format for daily partitions, convert to timestamp #}
        {% set year = partition_id[0:4] %}
        {% set month = partition_id[4:6] %}
        {% set day = partition_id[6:8] %}
        {% set partition_str = year ~ '-' ~ month ~ '-' ~ day ~ ' 00:00:00.000000 UTC' %}
        {% set partition_date = modules.datetime.datetime.strptime(partition_str, '%Y-%m-%d %H:%M:%S.%f UTC') %}
        {# Return timestamp just before partition start to include data at partition boundary #}
        {% set adjusted_timestamp = partition_date - modules.datetime.timedelta(microseconds=1) %}
        {% set timestamp_str = adjusted_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f UTC') %}
        {{ return(timestamp_str) }}
    {% else %}
        {{ return(none) }}
    {% endif %}
{% endmacro %}