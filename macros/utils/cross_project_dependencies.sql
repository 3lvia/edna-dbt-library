{% macro _get_dbt_project_environment() %}
    {% set raw_environment = env_var('DBT_PROJECT_ENVIRONMENT', none) %}

    {% if raw_environment is none or raw_environment | trim == '' %}
        {% do exceptions.raise_compiler_error(
            "cross_project_ref: env var 'DBT_PROJECT_ENVIRONMENT' must be set. "
            ~ "Expected one of DEV, Development, CI, TEST or PROD."
        ) %}
    {% endif %}

    {% set environment = raw_environment | trim | upper %}

    {% if environment not in ['DEV', 'DEVELOPMENT', 'CI', 'TEST', 'PROD'] %}
        {% do exceptions.raise_compiler_error(
            "cross_project_ref: unsupported DBT_PROJECT_ENVIRONMENT '" ~ raw_environment ~ "'. "
            ~ "Expected one of DEV, Development, CI, TEST or PROD."
        ) %}
    {% endif %}

    {{ return(environment) }}
{% endmacro %}

{% macro _validate_upstream_relation_source_args(source_name, resolved_table_name, environment) %}
    {% if source_name is not defined or source_name is none or source_name == '' %}
        {% do exceptions.raise_compiler_error(
            "cross_project_ref: source_name is required when DBT_PROJECT_ENVIRONMENT resolves to '" ~ environment ~ "'."
        ) %}
    {% endif %}

    {% if resolved_table_name is not defined or resolved_table_name is none or resolved_table_name == '' %}
        {% do exceptions.raise_compiler_error(
            "cross_project_ref: provide model_name or table_name to resolve the physical source table name when "
            ~ "DBT_PROJECT_ENVIRONMENT resolves to '" ~ environment ~ "'."
        ) %}
    {% endif %}
{% endmacro %}

{% macro _validate_upstream_relation_ref_args(project_name, model_name, environment) %}
    {% if project_name is not defined or project_name is none or project_name == '' or model_name is not defined or model_name is none or model_name == '' %}
        {% do exceptions.raise_compiler_error(
            "cross_project_ref: project_name and model_name are required when "
            ~ "DBT_PROJECT_ENVIRONMENT resolves to '" ~ environment ~ "'."
        ) %}
    {% endif %}
{% endmacro %}

{% macro cross_project_ref(project_name=none, model_name=none, source_name=none, table_name=none, version=none) %}
    {% set environment = edna_dbt_lib._get_dbt_project_environment() %}
    {% set resolved_table_name = table_name if table_name is defined and table_name is not none and table_name != '' else model_name %}
    {% set resolved_version = version if version is defined and version is not none and (version | string | trim) != '' else none %}

    {# DEV should resolve through explicit sources instead of cross-project refs. #}
    {% if environment == 'DEV' %}
        {% do edna_dbt_lib._validate_upstream_relation_source_args(source_name, resolved_table_name, environment) %}
        {{ return(source(source_name, resolved_table_name)) }}
    {% endif %}

    {% do edna_dbt_lib._validate_upstream_relation_ref_args(project_name, model_name, environment) %}

    {% if resolved_version is not none %}
        {{ return(ref(project_name, model_name, version=resolved_version)) }}
    {% endif %}

    {{ return(ref(project_name, model_name)) }}
{% endmacro %}
