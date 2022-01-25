{%- macro _get_label_str(labels) -%}
  {%- set labs = [] -%}
  {% for key, value in labels.items() %}
    {%- do labs.append('("{}", "{}")'.format(key, value)) -%}
  {% endfor %}
  {{ return('[{}]'.format(label_strs | join(', '))) }}
{%- endmacro -%}
