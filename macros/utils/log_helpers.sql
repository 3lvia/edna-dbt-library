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

{# Latest successful run's start time (stored as runWindowEnd for that run in the log table). #}
{% macro get_last_successful_run_start(log_table_id, table_id, default='0001-01-01 00:00:00 UTC') %}
    {%- set q -%}
        select runWindowEnd
        from `{{ log_table_id }}`
        where bigQueryTableId = '{{ table_id }}'
            and eventType = 'model_run_succeeded'
            and runWindowEnd is not null
        qualify row_number() over (order by runWindowEnd desc) = 1
    {%- endset -%}
    {%- set ts = dbt_utils.get_single_value(q) -%}
    {{ return(ts or default) }}
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
