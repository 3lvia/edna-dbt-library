{%- macro quote_replace(string) -%}
    {{- string | replace("'", "\'") | replace('"', '\"') -}}
{%- endmacro %}
