{% macro generate_schema_name(custom_schema_name, node) %}
    
    {%- set default_schema = target.schema -%}
    
    {% if env_var('DBT_CLOUD_INVOCATION_CONTEXT', '') == 'ci'
        and env_var('DBT_PROJECT_IS_POC', 'false') == 'true' %}
        
        {{ default_schema }}

    {%- elif env_var('DBT_CLOUD_INVOCATION_CONTEXT', '') == 'dev'
        and env_var('DBT_PROJECT_IS_POC', 'false') == 'true' -%}
        
        dbt_user_{{ env_var('DBT_USER_ID', '') }}

    {%- else -%}

        {% set dataproduct_group = node.path.split('/')[0] %}
        {% if node.path.split('/')[1] == 'dataproduct' %}
            {{ project_name }}_{{ dataproduct_group }}
        {%- else -%}
            {{ project_name }}_{{ dataproduct_group }}_curated
        {% endif %}
    {% endif %}
{%- endmacro %}
