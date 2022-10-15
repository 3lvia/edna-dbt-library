{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- set dataprodconfig = node.config.get('dataproduct') -%}

    {%- if edna_dbt_lib.is_defined(dataprodconfig) and edna_dbt_lib.is_defined(dataprodconfig.get('version')) -%}
        {%- set v = "_v" ~ (dataprodconfig.get('version') | trim('.0') | replace(".", "-")) -%}
        {% if v == "_v1" %}
            {% set v = "" %}
        {% endif %}
    {%- endif -%}

    {%- if custom_alias_name is none -%}

        {{ node.name ~ v }}

    {%- else -%}

        {{ (custom_alias_name ~ v) | trim }}

    {%- endif -%}

{%- endmacro %}
