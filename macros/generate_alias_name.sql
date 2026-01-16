{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- set meta_config = node.config.get('meta') or {} -%}
    {%- set dataprodconfig = node.config.get('dataproduct', meta_config.get('dataproduct')) -%}

    {%- if edna_dbt_lib.is_defined(dataprodconfig) and edna_dbt_lib.is_defined(dataprodconfig.get('version')) -%}
        {%- set v = (dataprodconfig.get('version') | trim('.0')) -%}
        
        {% if v == "1" %}
            {% set v = "" %}
        {% endif %}

    {%- elif node.version -%}
        {%- set v = node.version -%}
    {%- endif -%}

    {%- if custom_alias_name -%}
        {{ custom_alias_name | trim }}

    {%- elif edna_dbt_lib.is_defined(v) -%}
        {{ return(node.name ~ "_v" ~ (v | replace(".", "-"))) }}

    {%- else -%}
        {{ node.name }}

    {%- endif -%}

{%- endmacro %}
