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
{% macro log_model_run_started_pre_hook(relation=this, message=None, backfill_interval_days=None) %}
    {% set started_ts = modules.datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f UTC') %}
    {% set ids = edna_dbt_lib.bq_ids_for_relation(relation) %}

    {% set window_start = edna_dbt_lib.get_last_successful_run_window_end(ids['log_table_id'], ids['table_id']) %}
    {% set window_end = edna_dbt_lib.apply_backfill_interval_limit(window_start, run_started_at, backfill_interval_days) %}

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

{% macro log_model_run_succeeded_post_hook(relation=this, message=None, backfill_interval_days=None) %}
    {% set ids = edna_dbt_lib.bq_ids_for_relation(relation) %}

    {% set window_start = edna_dbt_lib.get_last_successful_run_window_end(ids['log_table_id'], ids['table_id']) %}
    {% set window_end = edna_dbt_lib.apply_backfill_interval_limit(backfill_interval_days, window_start, run_started_at) %}

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

{# Apply backfill interval limit to window_end if configured #}
{% macro apply_backfill_interval_limit(backfill_interval_days, window_start, window_end=run_started_at) %}
    {% if backfill_interval_days %}
        {% set backfill_days = backfill_interval_days | int %}
        {% if backfill_days > 0 and window_start %}
            {% set max_backfill_end = modules.datetime.datetime.strptime(window_start, '%Y-%m-%d %H:%M:%S UTC') + modules.datetime.timedelta(days=backfill_days) %}
            {% set max_backfill_end_str = max_backfill_end.strftime('%Y-%m-%d %H:%M:%S UTC') %}
            {% if window_end > max_backfill_end_str %}
                {% set window_end = max_backfill_end_str %}
            {% endif %}
        {% endif %}
    {% endif %}
    {{ return(window_end) }}
{% endmacro %}