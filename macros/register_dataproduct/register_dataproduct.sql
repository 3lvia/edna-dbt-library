{%- macro register_dataproduct() -%}
  {%- if execute -%}
    {%- set product_id = '{}.{}'.format(schema, this.identifier) -%}
    {%- set dataprodconfig = config.get('dataproduct') -%}
    {%- if edna_dbt_lib._dataproduct_shoulud_be_registered(product_id, dataprodconfig) -%}
      {%- set owner = dataprodconfig.get('owner')-%}
      {%- if owner is undefined -%}
        {{ exceptions.raise_compiler_error("Dataproduct owner must be set") }}
      {%- endif -%}
      {%- set bq_dataset = schema -%}
      {%- if bq_dataset.split('_')[-1] == 'curated' -%}
        {{ exceptions.raise_compiler_error("Models for registered dataproducts must be in a subfolder called dataproduct under your dataproductgroup. e.g: models/example/dataproduct") }}
      {%- endif -%}

      {%- set is_registered = edna_dbt_lib._is_registered_dataproduct(product_id) -%}
      {%- set bq_project = model.database -%}
      {%- set identifier = this.identifier -%}
      {%- set compiled_sql = model.compiled_sql -%}
      {%- set domain = project_name -%}
      {%- set dbt_id = model.unique_id -%}
      {%- set model_definition_columns = config.model.columns if config.model.columns is defined or config.model.columns is none else {} -%}
      {%- set labels = config.get('labels', default={}) -%}
      {%- set dataproduct_group = config.model.path.split('/')[0] -%}

      {%- set bq_columns = edna_dbt_lib._get_columns_and_check_for_column_deletion(is_registered, bq_project, identifier, compiled_sql) -%}

      {%- do edna_dbt_lib._upsert_dataproduct_entry(product_id, domain, dataproduct_group, bq_project, bq_dataset, identifier, dbt_id, owner, bq_columns, model_definition_columns, labels)-%}
    {%- endif -%}
  {%- endif -%}
{%- endmacro -%}
