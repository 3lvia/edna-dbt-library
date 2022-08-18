{% macro is_defined(item) %}
    {{ log(item is defined, true) }}
    {{ log(item is not none, true) }}
    {{ log(item != '', true) }}
    {{ return(item is defined and item is not none and item != '') }}
{% endmacro %}

{% macro create_tmp_relation(compiled_sql, target_relation) %}
    {% set tmp_identifier = target_relation.identifier ~ '__edna_tmp' %}
    {% set tmp_relation = api.Relation.create(identifier=tmp_identifier,
                                                  schema=target_relation.schema,
                                                  database=none,
                                                  type='view') -%}

    {% set cmd = create_view_as(tmp_relation, compiled_sql) %}
    {% do run_query(cmd) %}
    {{ return(tmp_relation) }}
{% endmacro %}
