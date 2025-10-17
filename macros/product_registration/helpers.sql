{% macro is_defined(item) %}
    {{ return(item is defined and item is not none and item != '') }}
{% endmacro %}

{% macro create_tmp_relation(compiled_sql, target_relation) %}
    {% set tmp_identifier = target_relation.identifier ~ '__edna_tmp' %}
    {% set tmp_relation = api.Relation.create(
        identifier=tmp_identifier,
        schema=target_relation.schema,
        database=target_relation.database,
        type='view') -%}

    {% set cmd = create_view_as(tmp_relation, compiled_sql) %}
    {% do run_query(cmd) %}
    {{ return(tmp_relation) }}
{% endmacro %}

{% macro _string_or_null(stringvalue) %}
    {% if edna_dbt_lib.is_defined(stringvalue) %}
        {{ return("'{}'".format(stringvalue)) }}
    {% else %}
        {{ return("null") }}
    {% endif %}
{% endmacro %}

{% macro get_deployed_relation(target_relation) %}
    {% if env_var('DBT_CLOUD_INVOCATION_CONTEXT', '') not in ['dev', 'ci'] %}
        {{ return(target_relation) }}
    {% endif %}
    
    {% set fqn = model.fqn %}
    {% set deploy_schema = target_relation.schema %}
    
    {% if fqn | length >= 3 %}
        {% set domain = fqn[0] | lower %}
        {% set group  = fqn[1] | lower %}
        {% set layer  = fqn[2] | lower %}

        {% if layer == 'curated' %}
            {% set deploy_schema = domain ~ '_' ~ group ~ '_curated' %}
        {% elif layer == 'dataproduct' %}
            {% set deploy_schema = domain ~ '_' ~ group %}
        {% endif %}
    {% endif %}
    
    {% set deployed_relation = api.Relation.create(
        identifier=target_relation.identifier,
        schema=deploy_schema,
        database=target_relation.database,
        type=target_relation.type) %}

    {{ return(deployed_relation) }}
{% endmacro %}
