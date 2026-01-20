{# Resolve config values from meta or top-level config without triggering warnings. #}
{% macro get_config_or_meta(config_obj, key, default=none) %}
    {% if config_obj.meta_get is defined %}
        {% set meta_value = config_obj.meta_get(key) %}
    {% else %}
        {% set meta_dict = config_obj.get('meta') or {} %}
        {% set meta_value = meta_dict.get(key) %}
    {% endif %}

    {% if meta_value is not none %}
        {{ return(meta_value) }}
    {% endif %}

    {{ return(config_obj.get(key, default)) }}
{% endmacro %}
