# dbt standards (EDNA data platform, BigQuery)

## This is a template file used for dbt development in Elvia

Read `DOMAIN_CONTEXT.md` for repository-specific domain guidance. That file is owned by the repository team and is
preserved by the sync. Follow this shared standard unless a domain-specific exception is explicitly documented there.

## Before you finish
- Check all the boxes in the checklist in 6. 

---

# dbt Conventions

These are shared guidelines for dbt projects on the EDNA data platform (BigQuery), reusable across
domains. Follow these rules when generating or editing dbt models (`.sql`) or YAML files in this
repository. Replace `<domain>` and `<source>` below with the actual domain/source name used in the
repository you are working in.

## 1. Layering (Medallion / dbt structure)

The platform uses three main layers, following dbt's recommendations and further split in practice:

| EDNA          | Medallion | dbt          |
|---------------|-----------|--------------|
| Raw           | Bronze    | Sources      |
| Curated       | Silver    | Staging      |
| Curated       | Silver    | Intermediate |
| Data products | Gold      | Marts        |

- **Raw**: raw data straight from the source system, with no/minimal transformation. Should mirror
  the source as closely as possible. No end-user access.
- **Curated**: transformed tables/views that are not yet ready to be consumed as data products.
  Split into separate folders in the dbt project, prefixed by layer:
  - **Staging** (`stg_`): minimal transformation to make raw data readable/usable (decoding,
    pivoting, timestamp conversion, changing partitioning). No joins, derivations, or calculations
    here. Not always necessary — it's fine to go directly from raw to intermediate.
  - **Intermediate** (`int_`): modular, reusable building blocks used to build data products. Uses
    correct, standardized, and consistent terminology, ideally aligned with the domain's own
    information model (e.g. CIM), where the domain has chosen one. Should be reused across multiple
    data products to avoid repeating the same transformation.
- **Data products**: finished, consumable models (for end users, reports, or systems/applications).
  High data quality requirements apply.

### Which layers can read from which (important when choosing `ref()`/`source()`)
- **Raw** is never read directly by any layer other than staging/intermediate, and only via
  `source()`.
- **Staging** can only read `source()` from raw in the same data product group. Can be read by
  intermediate in the same group, and partially/with restrictions by intermediate in other groups
  in the same domain.
- **Intermediate** can read from raw, staging, and intermediate in the same and other data product
  groups *within the same domain*, but **never from another domain**. Can be read by staging,
  intermediate, and data products in the same domain.
- **Data products** can read from intermediate and from other data products — including across
  other data product groups and other domains. Can only be read by other data products.
- To read data from another domain, a dedicated data product **must** be created in that domain —
  never go directly via `source()`/`ref()` into another domain's raw/staging/intermediate layer.

### Data product group split
- Data product groups are normally split by source system — each source system typically has its
  own data product group in BigQuery.
- For data products spanning multiple source systems: create a dedicated data product group for
  this, where the raw dataset is often empty (the raw data still lives in the source's own data
  product group).

### Dataset naming convention (set automatically by the platform)
- Raw: `<domain>_<source>_raw`
- Curated: `<domain>_<source>_curated`
- Data products: `<domain>_<source>` (no suffix)

## 2. Naming rules

### General
- **One language per model** — do not mix Norwegian and English within the same model.
- **camelCase** for field names in curated and data products (e.g. `usagePointId`,
  `installedTime`).
  - `snake_case`, `PascalCase`, and `ALL_UPPERCASE` are **not allowed** for field names, except in
    the `_raw` dataset.
- **`_raw` dataset**: keep field names and style exactly as in the source (no rewriting).
- **`_curated` dataset**: for temporary transformations. When copying from a source without control
  over field names, it's fine to map 1:1 to the source's naming standard.
- **Table/product names**: always `snake_case`, lowercase. Example: `int_transaction_hour`.

### Model prefixes
| Layer        | Prefix  | Style       |
|--------------|---------|-------------|
| Staging      | `stg_`  | snake_case  |
| Intermediate | `int_`  | snake_case  |
| Data product | `dim_`, `fct_`, `agg_`, `kpi_`, `tld_` | snake_case |

- `dim_` – dimension tables
- `fct_` – fact tables
- `agg_` – aggregate tables
- `kpi_` – tables for specific KPIs (when `agg_` doesn't fit)
- `tld_` – tailored, for specific purposes, reports, orders, etc.

### Boolean fields
Prefix with `has`, `is`, `does`, `needs`, etc.:
```sql
-- Preferred
SELECT
    fictional AS isFictional,
    hanPort   AS hasHanPort,
    connected AS isConnected
-- Not preferred
SELECT
    fictional,
    hanPort,
    connected
```

### Time and date
- **TIMESTAMP** fields: suffix `Time` (not `At` — works poorly with verbs, e.g. `measurementAt`).
  Always UTC (BigQuery always returns TIMESTAMP in UTC). Examples: `installedTime`, `changedTime`,
  `measurementTime`.
- **DATE** fields: suffix `Date`. Examples: `registeredDate`, `deliveryDate`.
- **DATETIME** should be avoided where possible — use TIMESTAMP instead. Only when a timezone other
  than UTC is explicitly required:
  - Use one and the same timezone per column (convert if necessary).
  - Specify the timezone in the value where possible. Preferred format:
    `'%Y-%m-%dT%H:%M:%E3S%z'` (e.g. `2023-06-20T08:52:48.088+0200`).
  - Suffix `Time` + timezone code, e.g. `Utc` or `Nor`. Example: `createdTimeUtc`,
    `createdTimeNor`.

### Id, name, type
Always specify what kind of id/name/type it is — never leave generic field names as-is:
```sql
-- Preferred
SELECT
    id   AS meterModelId,
    name AS meterModelName,
    type AS meterModelType
-- Not preferred
SELECT id, name, type
```

## 3. Materialization

**Rule of thumb (start simple, escalate as needed):**
> Start with a view. If the query gets too slow → make it a table. If the table takes too long to
> build → make it incremental.

| Type | Use when |
|------|----------|
| **View** | Need for up-to-date/near-real-time data, simple/fast queries with little computation, underlying table has suitable partitioning/clustering, a simple transformation layer expected to change frequently |
| **Table** | Underlying data rarely/never updates, high performance requirements for frequent queries, heavy transformations/aggregations |
| **Incremental** | Large data volumes, underlying table updated regularly, no strict real-time requirement, high performance requirements |
| **Materialized view** | Pre-computed aggregations, need for both fresh data and fast queries, only a single physical source table |

- **Table pitfall**: a heavy, static table model can become costly if triggered accidentally.
  Consider using `incremental` instead, with `WHERE FALSE` in the query, so that accidental runs
  are free and existing data is left unchanged.
- **Incremental strategies in BigQuery**: `merge`, `insert_overwrite`, `microbatch`.
  - `merge`: requires custom logic for what is new/changed (`unique_key`, filtering e.g. on
    `insertTime`). Avoid a full table scan on large tables — use a lookback window instead of an
    unbounded `SELECT MAX(...) FROM {{ this }}` (only works if source and target are partitioned on
    the same column).
  - `insert_overwrite`: overwrites entire partitions with data from selected source partitions.
    Requires the same partition column in source and target. Risk of losing "late arriving data"
    outside the lookback window.
  - `microbatch`: same principle as `insert_overwrite`, but without manual lookback logic —
    `batch_size` must match `granularity` in `partition_by`. BigQuery does not yet run batches in
    parallel.
- **Materialized view — limitations**: must read from a physical table (not a view), does not
  support window functions (`ROW_NUMBER()`, `RANK()`, `LAST_VALUE()`, etc.), limited JOIN/UNION
  support, no column descriptions (consider adding a logical view on top for data product use), no
  `CURRENT_DATE()`/`CURRENT_TIME()`/`RAND()`, and inherits the partition column from the underlying
  table.

## 4. YAML — documentation and tests

- **One `.yml` file per model/logical area**
- The `.yml` file must have the **same name** as the model:
  ```
  int_periodic_volumes.sql
  int_periodic_volumes.yml
  ```
- All models and fields must have **good descriptions** in `description`. `name` in the YAML must
  match the field name in the SQL model exactly.
- Example:
  ```yaml
  models:
    - name: int_periodic_volumes
      description: |
        Intermediate view for periodic readings/volumes.
        All versions of each periodic reading are included in this view.
        Column names beginning with an underscore (_) are not part of the
        information model and are applicable to change without notice.
      columns:
        - name: usagePointId
          description: |
            Usage point ID. Any free text that names the object.
  ```
- Use dbt tests (built-in and custom) actively to ensure data quality and catch errors early. This
  also applies to sources and staging models.

## 5. Publishing data products

A model becomes a data product only when it lives in the `dataproduct` folder and has a
`dataproduct` config block:

```sql
{{
    config(
        meta = {
            'dataproduct': {
                'owner': 'MSI Data',
                'displayName': 'Earth fault statistics',
                'version': '1.0.0',
                'versionDescription': 'Initial version'
            }
        },
        materialized = 'view'
    )
}}
```

- `owner` is **mandatory** (the Jira project of the team responsible). A description is required on
  the model and on **every column** before it can be published.
- Published data products are append-only: you may add new fields, but never delete or change
  existing ones. A breaking change requires a new semantic version (`major.minor.(build).(revision)`)
  with an updated `versionDescription` instead.
- If materialized as a `view`, it must be an **authorized view** to get the expected access control.

## Checklist when generating dbt code

- [ ] Correct layer chosen (staging/intermediate/data product) and correct prefix (`stg_`, `int_`,
      `dim_`, `fct_`, `agg_`, `kpi_`, `tld_`)
- [ ] Only allowed `ref()`/`source()` relationships between layers and domains (see section 1)
- [ ] Field names in camelCase (except `_raw`), snake_case for table/product names
- [ ] Boolean fields prefixed with `is`/`has`/`does`/`needs`
- [ ] Time fields suffixed correctly (`Time`, `Date`, or `TimeUtc`/`TimeNor` where applicable) and
      in UTC where possible
- [ ] `id`/`name`/`type` fields named specifically (`meterModelId`, etc.)
- [ ] Correct materialization considered (view → table → incremental), and partition column
      checked for incremental models
- [ ] No hardcoded environment/dataset names in SQL — use `{{ target.name }}`/`ref()`/`source()`
- [ ] Dedicated `.yml` file with the same name as each model, including descriptions for all fields in that model; reuse suitable descriptions from sources or upstream
  models.
- [ ] Check for relevant and good tests in `.yml` files
- [ ] Data products have a `meta.dataproduct` config block with a mandatory `owner`, and a
      description on the model and every column
- [ ] Breaking changes to a published data product bump the semantic version instead of editing
      existing fields in place
- [ ] Preform `make lint-sql` on changed .sql and .yml files
- [ ] Run dbt parse on the whole project
