{% macro get_partitions_literal_for_merge(tmp_relation, partition_field) %}
    {# 
        Returns a comma-separated list of quoted DATE literals, like:
        '2025-10-30','2025-10-31'

        It inspects the rows that were materialized into tmp_relation in this run,
        finds which target partitions (by DATE(partition_field)) are affected,
        and returns them as a static literal list suitable for:
        
        DATE(T.<partition_field>) in (<that list>)

        This is required because:
        - The target table has require_partition_filter=true
        - BigQuery refuses MERGE without a static partition filter on the target
        - We do *not* want to scan all partitions, only the ones touched this run
    #}

    {% set q %}
        select distinct DATE({{ partition_field }}) as part_date
        from {{ tmp_relation }}
    {% endset %}

    {% set res = run_query(q) %}

    {% set vals = [] %}
    {% for row in res %}
        {# wrap each date in single quotes for a SQL literal #}
        {% do vals.append("'" ~ row['part_date'] ~ "'") %}
    {% endfor %}

    {{ return(vals | join(",")) }}
{% endmacro %}
