{# --- Resolve BQ ids from a Relation --- #}
{% macro bq_ids_for_relation(relation) %}
    {%- set domain     = model.fqn[0] -%}
    {%- set project_id = relation.database -%}
    {%- set dataset_id = relation.schema -%}
    {%- set table_id   = project_id ~ '.' ~ dataset_id ~ '.' ~ relation.identifier -%}
    {%- set log_table_dataset_id = domain ~ '_dbt_raw' -%}
    {%- set log_table_table_name = domain ~ '_dbt_event_log' -%}
    {%- set log_table_table_id   = project_id ~ '.' ~ log_table_dataset_id ~ '.' ~ log_table_table_name -%}

    {{ return({
        'domain': domain,
        'project_id': project_id,
        'dataset_id': dataset_id,
        'table_id': table_id,
        'log_table_id': log_table_table_id
        }) }}
{% endmacro %}

{# --- Insert a 'Run started' log row with full context + window bounds --- #}
{% macro log_run_started(log_table_id, relation, event_ts, window_start, window_end, ids=None) %}
    {% if not ids %}
        {% set ids = bq_ids_for_relation(relation) %}
    {% endif %}

    {% set cloud_inv_ctx  = env_var('DBT_CLOUD_INVOCATION_CONTEXT', '') %}
    {% set cloud_job_id   = env_var('DBT_CLOUD_JOB_ID', '') %}
    {% set cloud_run_id   = env_var('DBT_CLOUD_RUN_ID', '') %}
    {% set cloud_reason_c = env_var('DBT_CLOUD_RUN_REASON_CATEGORY', '') %}
    {% set cloud_reason   = env_var('DBT_CLOUD_RUN_REASON', '') %}
    {% set cloud_git_sha  = env_var('DBT_CLOUD_GIT_SHA', '') %}

    insert into `{{ log_table_id }}`
    select
        '{{ ids['table_id'] }}'                         as bigQueryTableId,
        '{{ ids['project_id'] }}'                       as bigQueryProjectId,
        '{{ ids['dataset_id'] }}'                       as bigQueryDatasetId,
        '{{ model.name }}'                              as dbtModelName,
        '{{ model.unique_id }}'                         as dbtNodeUniqueId,
        '{{ invocation_id }}'                           as dbtInvocationId,
        {{ "'" ~ cloud_inv_ctx  ~ "'" if cloud_inv_ctx  else 'NULL' }} as dbtInvocationContext,
        {{ "'" ~ cloud_job_id   ~ "'" if cloud_job_id   else 'NULL' }} as dbtJobId,
        {{ "'" ~ cloud_run_id   ~ "'" if cloud_run_id   else 'NULL' }} as dbtRunId,
        {{ "'" ~ cloud_reason_c ~ "'" if cloud_reason_c else 'NULL' }} as dbtRunReasonCategory,
        {{ "'" ~ cloud_reason   ~ "'" if cloud_reason   else 'NULL' }} as dbtRunReason,
        {{ "'" ~ cloud_git_sha  ~ "'" if cloud_git_sha  else 'NULL' }} as dbtGitSha,
        timestamp('{{ event_ts }}')                      as eventTimestamp,
        'Run started'                                    as eventType,
        {{ 'timestamp("' ~ window_start ~ '")' if window_start else 'NULL' }} as runWindowStart,
        {{ 'timestamp("' ~ window_end   ~ '")' if window_end   else 'NULL' }} as runWindowEnd,
        CURRENT_TIMESTAMP() as insertTime
    ;
{% endmacro %}

{# --- Insert a 'Run success' log row with the same context & window bounds --- #}
{% macro log_run_success(log_table_id, relation, window_start, window_end, ids=None) %}
    {% if not ids %}
        {% set ids = bq_ids_for_relation(relation) %}
    {% endif %}

    {% set cloud_inv_ctx  = env_var('DBT_CLOUD_INVOCATION_CONTEXT', '') %}
    {% set cloud_job_id   = env_var('DBT_CLOUD_JOB_ID', '') %}
    {% set cloud_run_id   = env_var('DBT_CLOUD_RUN_ID', '') %}
    {% set cloud_reason_c = env_var('DBT_CLOUD_RUN_REASON_CATEGORY', '') %}
    {% set cloud_reason   = env_var('DBT_CLOUD_RUN_REASON', '') %}
    {% set cloud_git_sha  = env_var('DBT_CLOUD_GIT_SHA', '') %}

    insert into `{{ log_table_id }}`
    select
        '{{ ids['table_id'] }}'   as bigQueryTableId,
        '{{ ids['project_id'] }}' as bigQueryProjectId,
        '{{ ids['dataset_id'] }}' as bigQueryDatasetId,
        '{{ model.name }}'        as dbtModelName,
        '{{ model.unique_id }}'   as dbtNodeUniqueId,
        '{{ invocation_id }}'     as dbtInvocationId,
        {{ "'" ~ cloud_inv_ctx  ~ "'" if cloud_inv_ctx  else 'NULL' }} as dbtInvocationContext,
        {{ "'" ~ cloud_job_id   ~ "'" if cloud_job_id   else 'NULL' }} as dbtJobId,
        {{ "'" ~ cloud_run_id   ~ "'" if cloud_run_id   else 'NULL' }} as dbtRunId,
        {{ "'" ~ cloud_reason_c ~ "'" if cloud_reason_c else 'NULL' }} as dbtRunReasonCategory,
        {{ "'" ~ cloud_reason   ~ "'" if cloud_reason   else 'NULL' }} as dbtRunReason,
        {{ "'" ~ cloud_git_sha  ~ "'" if cloud_git_sha  else 'NULL' }} as dbtGitSha,
        CURRENT_TIMESTAMP()       as eventTimestamp,
        'Run success'             as eventType,
        {{ 'timestamp("' ~ window_start ~ '")' if window_start else 'NULL' }} as runWindowStart,
        {{ 'timestamp("' ~ window_end   ~ '")' if window_end   else 'NULL' }} as runWindowEnd,
        CURRENT_TIMESTAMP()       as insertTime
    ;
{% endmacro %}

{# Latest successful run's start time (stored as runWindowEnd for that run in the log table). #}
{% macro get_last_successful_run_start(log_table_id, table_id, default='0001-01-01 00:00:00 UTC') %}
    {%- set q -%}
        select runWindowEnd
        from `{{ log_table_id }}`
        where bigQueryTableId = '{{ table_id }}'
            and eventType = 'Run success'
            and runWindowEnd is not null
        qualify row_number() over (order by runWindowEnd desc) = 1
    {%- endset -%}
    {%- set ts = dbt_utils.get_single_value(q) -%}
    {{ return(ts or default) }}
{% endmacro %}
