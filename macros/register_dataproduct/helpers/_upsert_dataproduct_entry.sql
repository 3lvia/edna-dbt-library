{%- macro _upsert_dataproduct_entry(product_id, description, domain, dataproduct_group, bq_project, bq_dataset, identifier, dbt_id, owner, bq_columns, model_definition_columns, labels) -%}
  {%- set column_str = edna_dbt_lib._get_column_str(bq_columns, model_definition_columns)-%}
  {%- set label_str = edna_dbt_lib._get_label_str(labels) -%}

  {%- set query -%}
    MERGE `{{ bq_project }}.dataplatform_internal.dataproducts` T
    USING (SELECT '{{ product_id }}' as id) S
    ON T.id = S.id
    WHEN MATCHED THEN
      UPDATE SET domain = '{{ domain }}', description= '{{ description }}', dataproductGroup = '{{ dataproduct_group }}', owner = '{{ owner }}', lastUpdateTime = CURRENT_TIMESTAMP(), columns = {{ column_str }}, labels = {{ label_str }}
    WHEN NOT MATCHED THEN
      INSERT (id, description, domain, dataproductGroup, bigquery, dbtId, owner, registeredTime, lastUpdateTime, columns, labels)
      VALUES('{{ product_id }}', '{{ description }}', '{{ domain }}', '{{ dataproduct_group }}', ('{{ bq_dataset }}', '{{ identifier }}'), '{{ dbt_id }}', '{{ owner }}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), {{ column_str }}, {{ label_str }} )
  {%- endset -%}

  {%- do run_query(query) -%}
{%- endmacro -%}
