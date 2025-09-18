{% macro generate_schema_name(custom_schema_name, node) %}

    {%- set default_schema = target.schema -%}

    {% if env_var('DBT_CLOUD_INVOCATION_CONTEXT', '') == 'ci'
        and env_var('DBT_PROJECT_IS_POC', 'false') == 'true' %}

        {{ default_schema }}

    {%- elif env_var('DBT_CLOUD_INVOCATION_CONTEXT', '') == 'dev'
        and env_var('DBT_PROJECT_IS_POC', 'false') == 'true' -%}

        dbt_cloud_user_{{ env_var('DBT_USER_ID', '') }}

    {%- else -%}

        {% set dataproduct_group = node.fqn[1] %}
        {% set layer = node.fqn[2] %}

        {% if layer == 'dataproduct' %}
            {{ project_name }}_{{ dataproduct_group }}
        {%- else -%}
            {{ project_name }}_{{ dataproduct_group }}_curated
        {% endif %}
    {% endif %}
{%- endmacro %}