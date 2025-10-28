# edna dbt Library

This repository contains reusable macros and materializations for dbt projects.

## Table of Contents

- [edna dbt Library](#edna-dbt-library)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Macros](#macros)
    - [Core Macros](#core-macros)
      - [`generate_schema_name(custom_schema_name, node)`](#generate_schema_namecustom_schema_name-node)
      - [`generate_alias_name(custom_alias_name=none, node=none)`](#generate_alias_namecustom_alias_namenone-nodenone)
    - [Materializations](#materializations)
      - [`incremental_log`](#incremental_log)
    - [Product Registration](#product-registration)
      - [`validate_dataproduct()`](#validate_dataproduct)
      - [`register_dataproduct_metadata()`](#register_dataproduct_metadata)
      - [Helper Macros](#helper-macros)
        - [`is_defined(item)`](#is_defineditem)
        - [`create_tmp_relation(compiled_sql, target_relation)`](#create_tmp_relationcompiled_sql-target_relation)
        - [`_string_or_null(stringvalue)`](#_string_or_nullstringvalue)
        - [`get_deployed_relation(target_relation)`](#get_deployed_relationtarget_relation)
        - [`_get_sizeinfo(target_relation)`](#_get_sizeinfotarget_relation)
        - [`_get_formated_columns(target_relation)`](#_get_formated_columnstarget_relation)
        - [`_get_formated_labels(label_dict)`](#_get_formated_labelslabel_dict)
        - [`_upsert_dataproduct_entry(description, display_name, domain, dataproduct_group, bq_dataset, bq_tablename, dbt_id, owner, columns, labels, size_info, preview_where_clause, version, versionDescription, name)`](#_upsert_dataproduct_entrydescription-display_name-domain-dataproduct_group-bq_dataset-bq_tablename-dbt_id-owner-columns-labels-size_info-preview_where_clause-version-versiondescription-name)
        - [`_validate_preview_where_clause(preview_where_clause)`](#_validate_preview_where_clausepreview_where_clause)
        - [`_validate_is_in_dataproduct_dataset(target_relation)`](#_validate_is_in_dataproduct_datasettarget_relation)
        - [`_is_registered_dataproduct(target_relation)`](#_is_registered_dataproducttarget_relation)
        - [`_check_for_column_deletion_and_descriptions(compiled_sql, target_relation, is_registered)`](#_check_for_column_deletion_and_descriptionscompiled_sql-target_relation-is_registered)
        - [`_get_missing_columns(target_columns, new_columns)`](#_get_missing_columnstarget_columns-new_columns)
        - [`_get_columns_from_relation(relation)`](#_get_columns_from_relationrelation)
        - [`_validate_semantic_versioning(v)`](#_validate_semantic_versioningv)
    - [Utils](#utils)
      - [Binary Conversions](#binary-conversions)
        - [`bytes4_to_int32(byte_value)`](#bytes4_to_int32byte_value)
        - [`base64_map(b64_str, index, zero_based=False)`](#base64_mapb64_str-index-zero_basedfalse)
        - [`hex_map(hex_str, index, zero_based=False)`](#hex_maphex_str-index-zero_basedfalse)
        - [`reverse_hex_bytes(hex_expr, add_0x=False)`](#reverse_hex_byteshex_expr-add_0xfalse)
        - [`hex_to_int(hex_val)`](#hex_to_inthex_val)
        - [`digit_to_bitstring(digit, base)`](#digit_to_bitstringdigit-base)
        - [`value_to_bitstring(value, width)`](#value_to_bitstringvalue-width)
        - [`bitstring_to_int(bitstr)`](#bitstring_to_intbitstr)
        - [`double_unbiased_exponent(exp_bits)`](#double_unbiased_exponentexp_bits)
        - [`double_mantissa(frac_bits)`](#double_mantissafrac_bits)
        - [`double_from_components(sign, mantissa, unbiased_exponent)`](#double_from_componentssign-mantissa-unbiased_exponent)
      - [UUID Generation](#uuid-generation)
        - [`uuid_v5(name_expr, namespace_uuid)`](#uuid_v5name_expr-namespace_uuid)
      - [Log Helpers](#log-helpers)
        - [`bq_ids_for_relation(relation)`](#bq_ids_for_relationrelation)
        - [`log_model_event(log_table_id, relation, event_type, window_start, window_end, ids=None, event_ts=None, message=None)`](#log_model_eventlog_table_id-relation-event_type-window_start-window_end-idsnone-event_tsnone-messagenone)
        - [`get_last_successful_run_window_end(log_table_id, table_id, default='0001-01-01 00:00:00 UTC')`](#get_last_successful_run_window_endlog_table_id-table_id-default0001-01-01-000000-utc)
        - [`cloud_env_sql_values()`](#cloud_env_sql_values)
        - [`log_model_run_started_pre_hook(relation=this, message=None, backfill_interval_days=None)`](#log_model_run_started_pre_hookrelationthis-messagenone-backfill_interval_daysnone)
        - [`log_model_run_succeeded_post_hook(relation=this, message=None, backfill_interval_days=None)`](#log_model_run_succeeded_post_hookrelationthis-messagenone-backfill_interval_daysnone)
      - [Quote Replace](#quote-replace)
        - [`quote_replace(string)`](#quote_replacestring)
  - [Examples](#examples)
    - [Usage](#usage)
    - [Product Registration](#product-registration-1)
    - [Using Core Macros](#using-core-macros)
    - [Using Utils](#using-utils)

## Overview

The library provides utilities for schema and alias generation, binary conversions, UUID generation, logging helpers, and product registration functionalities.

## Macros

- **Core Macros**: Schema and alias generation utilities
- **Materializations**: `incremental_log` for BigQuery incremental loads with logging
- **Product Registration**: Dataproduct validation and metadata registration helpers
- **Utils**: Binary conversions, UUID generation, logging helpers, and string utilities

### Core Macros

#### `generate_schema_name(custom_schema_name, node)`

Generates schema names based on the dbt environment and node fully qualified name (FQN).
Handles different environments like CI, DEV, and PROD, and constructs schema names using project name, dataproduct group, and layer.

#### `generate_alias_name(custom_alias_name=none, node=none)`

Generates alias names for models, optionally incorporating versioning information from dataproduct configuration or node version.

### Materializations

#### `incremental_log`

A BigQuery-specific materialization for incremental loads that includes logging of model run events. Supports partitioning and clustering configurations. Can be configured with `backfill_interval_days` to limit the maximum time window for backfilling data from the last successful run.

### Product Registration

#### `validate_dataproduct()`

Validates dataproduct configurations, including preview where clauses, dataset membership, column changes, and semantic versioning.

#### `register_dataproduct_metadata()`

Registers or updates dataproduct metadata in the dataplatform, including description, columns, labels, size information, and versioning details.

#### Helper Macros

##### `is_defined(item)`

Checks if an item is defined, not null, and not an empty string.

##### `create_tmp_relation(compiled_sql, target_relation)`

Creates a temporary view relation for the given compiled SQL.

##### `_string_or_null(stringvalue)`

Returns the string value wrapped in quotes if defined, otherwise 'null'.

##### `get_deployed_relation(target_relation)`

Returns the deployed relation for the target. Adjusts schema names only in dev/ci environments based on invocation context and FQN structure; uses the standard schema otherwise.

##### `_get_sizeinfo(target_relation)`

Retrieves row count and size in bytes for a BigQuery table.

##### `_get_formated_columns(target_relation)`

Formats column information (field path, data type, description) for metadata registration.

##### `_get_formated_labels(label_dict)`

Formats label key-value pairs for metadata registration.

##### `_upsert_dataproduct_entry(description, display_name, domain, dataproduct_group, bq_dataset, bq_tablename, dbt_id, owner, columns, labels, size_info, preview_where_clause, version, versionDescription, name)`

Performs an upsert operation to register or update a dataproduct entry in the metadata store.

##### `_validate_preview_where_clause(preview_where_clause)`

Validates the syntax of a preview where clause.

##### `_validate_is_in_dataproduct_dataset(target_relation)`

Checks if the target relation is in a dataproduct dataset.

##### `_is_registered_dataproduct(target_relation)`

Determines if a dataproduct is already registered.

##### `_check_for_column_deletion_and_descriptions(compiled_sql, target_relation, is_registered)`

Validates column changes, checking for deletions and ensuring descriptions are present.

##### `_get_missing_columns(target_columns, new_columns)`

Identifies columns that are missing from the new schema compared to the target.

##### `_get_columns_from_relation(relation)`

Retrieves column information from a relation using INFORMATION_SCHEMA.

##### `_validate_semantic_versioning(v)`

Validates that a version string follows semantic versioning format.

### Utils

#### Binary Conversions

##### `bytes4_to_int32(byte_value)`

Converts a 4-byte value to a 32-bit integer.

##### `base64_map(b64_str, index, zero_based=False)`

Extracts a character from a base64 string at the specified index.

##### `hex_map(hex_str, index, zero_based=False)`

Extracts a character from a hexadecimal string at the specified index.

##### `reverse_hex_bytes(hex_expr, add_0x=False)`

Reverses the byte order of a hexadecimal expression.

##### `hex_to_int(hex_val)`

Converts a hexadecimal value to an integer.

##### `digit_to_bitstring(digit, base)`

Converts a digit to its bitstring representation for a given base.

##### `value_to_bitstring(value, width)`

Converts a value to a bitstring of specified width.

##### `bitstring_to_int(bitstr)`

Converts a bitstring to an integer.

##### `double_unbiased_exponent(exp_bits)`

Calculates the unbiased exponent for a double-precision floating point from exponent bits.

##### `double_mantissa(frac_bits)`

Calculates the mantissa for a double-precision floating point from fraction bits.

##### `double_from_components(sign, mantissa, unbiased_exponent)`

Constructs a double-precision floating point value from its components.

#### UUID Generation

##### `uuid_v5(name_expr, namespace_uuid)`

Generates a UUID v5 based on a name expression and namespace UUID.

#### Log Helpers

##### `bq_ids_for_relation(relation)`

Extracts BigQuery-specific identifiers (log_table_id, table_id) for a relation.

##### `log_model_event(log_table_id, relation, event_type, window_start, window_end, ids=None, event_ts=None, message=None)`

Logs a model event to the specified log table.

##### `get_last_successful_run_window_end(log_table_id, table_id, default='0001-01-01 00:00:00 UTC')`

Retrieves the end timestamp of the last successful run for a table.

##### `cloud_env_sql_values()`

Returns SQL values representing the current cloud environment.

##### `apply_backfill_interval_limit(backfill_interval_days, window_start, window_end)`

Applies a backfill interval limit to the window_end timestamp. If backfill_interval_days is provided and greater than 0, and window_start is available, the window_end will be capped at window_start + backfill_interval_days to prevent excessive data loading.

##### `log_model_run_started_pre_hook(relation=this, message=None, backfill_interval_days=None)`

Pre-hook macro to log the start of a model run.

##### `log_model_run_succeeded_post_hook(relation=this, message=None, backfill_interval_days=None)`

Post-hook macro to log the successful completion of a model run.

#### Quote Replace

##### `quote_replace(string)`

Escapes single and double quotes in a string for safe SQL usage.

## Examples

### Usage

To use these macros in your dbt project, include this library as a dependency in your `packages.yml`:

```yaml
packages:
  - package: 3lvia/edna-dbt-library
    version: [latest version]
```

Then, reference the macros using the `edna_dbt_lib` namespace, e.g., `{{ edna_dbt_lib.generate_schema_name(custom_schema_name, node) }}`.

### Product Registration

In your model config, add:

```yaml
+dataproduct:
  validate: true
  register: true
```

Then, the macros will handle validation and registration automatically.

### Using Core Macros

```sql
-- In a model file
{{ config(
  schema=edna_dbt_lib.generate_schema_name(var('custom_schema'), this),
  alias=edna_dbt_lib.generate_alias_name(var('custom_alias'), this)
) }}

SELECT * FROM source_table
```

### Using Utils

```sql
SELECT
  {{ edna_dbt_lib.uuid_v5('example_name', '6ba7b810-9dad-11d1-80b4-00c04fd430c8') }} AS generated_uuid,
  {{ edna_dbt_lib.hex_to_int('FF') }} AS int_value
FROM my_table
```
