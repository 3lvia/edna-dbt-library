{% materialization incremental_partition_merge, adapter='bigquery', supported_languages=['sql'] %}

    {# ----------------------------------------------------------------------------------
        0. Context / relations
    ---------------------------------------------------------------------------------- #}

    {% set target_relation   = this %}
    {% set existing_relation = load_relation(this) %}
    {% set full_refresh_mode = should_full_refresh() %}

    {% set grant_config = config.get('grants') %}
    {% set cluster_config   = config.get('cluster_by') %}

    {# ----------------------------------------------------------------------------------
        1. Read and validate configs
        - partition_by: required, must be DAY-granularity
        - unique_key: required (string or list)
        - event_time: optional (dbt standard config to indicate row recency)
        - merge_update_columns / merge_exclude_columns: optional (dbt semantics)
    ---------------------------------------------------------------------------------- #}

    {% set raw_partition_by = config.get('partition_by') %}
    {% if not raw_partition_by %}
        {% do exceptions.raise_compiler_error(
            "incremental_partition_merge: model must define partition_by."
        ) %}
    {% endif %}

    {% set partition_config    = adapter.parse_partition_by(raw_partition_by) %}
    {% set partition_field = partition_config.field %}
    {% set partition_grain = partition_config.granularity %}

    {% if partition_grain | lower != 'day' %}
        {% do exceptions.raise_compiler_error(
            "incremental_partition_merge: only DAY partition granularity is supported (got '" ~ partition_grain ~ "')."
        ) %}
    {% endif %}

    {# unique_key: same contract as dbt incremental merge #}
    {% set unique_key_cfg = config.get('unique_key') %}
    {% if unique_key_cfg is string %}
        {% set unique_key_cols = [ unique_key_cfg ] %}
    {% else %}
        {% set unique_key_cols = unique_key_cfg %}
    {% endif %}

    {% if not unique_key_cols or (unique_key_cols | length == 0) %}
        {% do exceptions.raise_compiler_error(
            "incremental_partition_merge: config.unique_key is required and cannot be empty."
        ) %}
    {% endif %}

    {# event_time: dbt-standard config.
        If provided, we only update an existing row if the new row has strictly newer event_time.
        If not provided, we update unconditionally on match. #}
    {% set event_time_col = config.get('event_time', none) %}

    {# Optional dbt configs for controlling which columns are updated:
        - merge_update_columns: whitelist of columns to update/insert
        - merge_exclude_columns: blacklist of columns to exclude
        We support them to behave like dbt's incremental/merge. #}
    {% set merge_update_columns  = config.get('merge_update_columns', none) %}
    {% set merge_exclude_columns = config.get('merge_exclude_columns', none) %}

    {{ run_hooks(pre_hooks) }}

    {# ----------------------------------------------------------------------------------
        2. Create temp relation (the batch for this run)
        Mirrors dbt incremental/merge flow: build an intermediate relation that we'll MERGE from.
        We apply the same partitioning and clustering as the target table to optimize the MERGE
        operation by ensuring both tables have compatible physical layouts.
    ---------------------------------------------------------------------------------- #}

    {% set tmp_relation = make_temp_relation(this) %}
    {% set model_sql = sql %}

    {%- call statement('create_tmp') -%}
        create or replace table {{ tmp_relation }}
        {{ partition_by(partition_config) }}
        {{ cluster_by(cluster_config) }}
        as
        {{ model_sql }}
    {%- endcall -%}

    {# ----------------------------------------------------------------------------------
        3. Build list of affected partitions from tmp_relation (BigQuery-specific optimization)
        We'll inline this list in the MERGE predicate to satisfy require_partition_filter
        and avoid scanning the whole table.
    ---------------------------------------------------------------------------------- #}

    {% set partitions_literal = edna_dbt_lib.get_partitions_literal_for_merge(tmp_relation, partition_field) %}

    {# partitions_literal is something like:
        '2025-10-30','2025-10-31'
        If tmp_relation was empty, partitions_literal will be "" (empty string). #}

    {# ----------------------------------------------------------------------------------
        4. Build key_match_sql for MERGE ON
        dbt logic: unique_key → "T.col1 = S.col1 and T.col2 = S.col2 ..."
    ---------------------------------------------------------------------------------- #}

    {% set key_match_clauses = [] %}
    {% for col in unique_key_cols %}
        {% do key_match_clauses.append("T." ~ col ~ " = S." ~ col) %}
    {% endfor %}
    {% set key_match_sql = key_match_clauses | join(" and ") %}

    {# ----------------------------------------------------------------------------------
        5. Build merge_update_predicates (WHEN MATCHED extra conditions)
        We mimic dbt's idea of "merge_update_predicates": a list of AND'ed conditions.
        We'll support event_time as recency rule.
    ---------------------------------------------------------------------------------- #}

    {% set merge_update_predicates = [] %}

    {% if event_time_col %}
        {# Only update if the incoming row is strictly "newer" in terms of event_time #}
        {% do merge_update_predicates.append("S." ~ event_time_col ~ " > T." ~ event_time_col) %}
    {% endif %}

    {# Render them as either nothing (no extra predicate) or:
        and <pred1>
        and <pred2>
        etc.
    #}
    {% if merge_update_predicates | length > 0 %}
        {% set matched_predicate_sql = "and " ~ (merge_update_predicates | join("\nand ")) %}
    {% else %}
        {% set matched_predicate_sql = "" %}
    {% endif %}

    {# ----------------------------------------------------------------------------------
        6. Build column sets for UPDATE and INSERT
        We do what dbt does:
            - Find intersection of columns in tmp_relation and existing_relation
            - Apply merge_update_columns (whitelist) if provided
            - Apply merge_exclude_columns (blacklist) if provided
            - Build:
                UPDATE SET col = S.col, ...
                INSERT (col1, col2, ...) VALUES (S.col1, S.col2, ...)
        On first run / full refresh, we won't MERGE anyway, so we only need this if target exists.
    ---------------------------------------------------------------------------------- #}

    {% if (existing_relation is not none) and (not existing_relation.is_view) and (not full_refresh_mode) %}
        {% set tmp_columns = adapter.get_columns_in_relation(tmp_relation) %}
        {% set tgt_columns = adapter.get_columns_in_relation(existing_relation) %}

        {% set tmp_colnames = tmp_columns | map(attribute='name') | list %}
        {% set tgt_colnames = tgt_columns | map(attribute='name') | list %}

        {% set common_colnames = [] %}
        {% for c in tmp_colnames %}
            {% if c in tgt_colnames %}
                {% do common_colnames.append(c) %}
            {% endif %}
        {% endfor %}

        {# Apply merge_update_columns (whitelist) #}
        {% if merge_update_columns is not none %}
            {% set filtered_common = [] %}
            {% for c in common_colnames %}
                {% if c in merge_update_columns %}
                    {% do filtered_common.append(c) %}
                {% endif %}
            {% endfor %}
            {% set common_colnames = filtered_common %}
        {% endif %}

        {# Apply merge_exclude_columns (blacklist) #}
        {% if merge_exclude_columns is not none %}
            {% set filtered_common = [] %}
            {% for c in common_colnames %}
                {% if c not in merge_exclude_columns %}
                    {% do filtered_common.append(c) %}
                {% endif %}
            {% endfor %}
            {% set common_colnames = filtered_common %}
        {% endif %}

        {% if common_colnames | length == 0 %}
            {% do exceptions.raise_compiler_error(
                "incremental_partition_merge: no overlapping columns between "
                ~ tmp_relation ~ " and " ~ target_relation
                ~ " after applying merge_update_columns / merge_exclude_columns."
            ) %}
        {% endif %}

        {# UPDATE assignments: col = S.col #}
        {% set update_assignments = [] %}
        {% for c in common_colnames %}
            {% do update_assignments.append(c ~ " = S." ~ c) %}
        {% endfor %}
        {% set update_sql = update_assignments | join(",\n                    ") %}

        {# INSERT column list and VALUES list #}
        {% set insert_cols_csv  = common_colnames | join(", ") %}
        {% set insert_vals_list = [] %}
        {% for c in common_colnames %}
            {% do insert_vals_list.append("S." ~ c) %}
        {% endfor %}
        {% set insert_vals_csv  = insert_vals_list | join(", ") %}
    {% endif %}

    {# ----------------------------------------------------------------------------------
        7. Execute write to target
        Branching:
            - If partitions_literal is empty => tmp_relation has 0 rows ⇒ skip
            - Else:
            * If first run / full_refresh / table is a view:
                create or replace table target as select * from tmp_relation
            * Else (incremental merge):
                MERGE into target using tmp_relation with partition pruning
    ---------------------------------------------------------------------------------- #}

    {% if partitions_literal | length > 0 %}

                {# First run / full refresh / replacing a view #}
        {% if (existing_relation is none) or (existing_relation.is_view) or full_refresh_mode %}

            {# Drop non-replaceable relation if needed, consistent with dbt patterns #}
            {% if existing_relation is not none and existing_relation.is_view %}
                {{ adapter.drop_relation(existing_relation) }}
            {% elif full_refresh_mode and (existing_relation is not none)
                    and (not adapter.is_replaceable(existing_relation, partition_config, cluster_config)) %}
                {{ adapter.drop_relation(existing_relation) }}
            {% endif %}

            {%- call statement('main') -%}
                create or replace table {{ target_relation }} as
                select *
                from {{ tmp_relation }}
            {%- endcall -%}

        {% else %}

            {# Incremental MERGE path with partition pruning.
                Key point: The ON clause does two things:
                1) Restrict target partitions to the set of touched dates:
                    DATE(T.<partition_field>) in (<partitions_literal>)
                    This satisfies require_partition_filter and avoids scanning the full table.
                2) Match rows by unique_key (dbt standard).
            #}

            {%- call statement('main') -%}
                merge into {{ target_relation }} as T
                using {{ tmp_relation }} as S
                on
                    DATE(T.{{ partition_field }}) in ({{ partitions_literal }})
                    and {{ key_match_sql }}

                when matched
                    {{ matched_predicate_sql }}
                then update set
                    {{ update_sql }}

                when not matched then insert (
                    {{ insert_cols_csv }}
                )
                values (
                    {{ insert_vals_csv }}
                )
            {%- endcall -%}

        {% endif %}

    {% else %}

        {# No rows in tmp_relation, so there are no partitions to touch. #}
        {% if existing_relation is none %}
            {# First run with no data: create empty target table #}
            {% do log(
                "incremental_partition_merge: no rows to merge for " ~ target_relation ~ ", creating empty table",
                info=True
            ) %}

            {%- call statement('main') -%}
                create or replace table {{ target_relation }} as
                select *
                from {{ tmp_relation }}
            {%- endcall -%}
        {% else %}
            {# Incremental run with no data: insert 0 rows to satisfy dbt's main statement requirement #}
            {% do log(
                "incremental_partition_merge: no rows to merge for " ~ target_relation ~ ", inserting 0 rows",
                info=True
            ) %}

            {%- call statement('main') -%}
                insert into {{ target_relation }}
                select * from {{ tmp_relation }}
            {%- endcall -%}
        {% endif %}

    {% endif %}

    {# ----------------------------------------------------------------------------------
        8. Cleanup temp relation
        ---------------------------------------------------------------------------------- #}

    {{ adapter.drop_relation(tmp_relation) }}

    {# ----------------------------------------------------------------------------------
        9. Apply grants/docs (dbt convention) and return
    ---------------------------------------------------------------------------------- #}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
    {% do apply_grants(target_relation, grant_config, should_revoke) %}
    {% do persist_docs(target_relation, model) %}

    {{ run_hooks(post_hooks) }}

    {{ return({
        'relations': [target_relation]
    }) }}

{% endmaterialization %}
