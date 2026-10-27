# Central Project Memory: Olist CDC on Databricks

**Owner:** Laurence Igbokwe (`laurence702 <akaigbokwelaurence@gmail.com>`)  
**Status:** Dev bundle DEPLOYED and verified on Databricks Free Edition serverless; initial load and incremental CDC proven (2026-10-26). Next: silver/gold dbt models.

## Architecture decisions

- The original Snowflake-oriented brief is implemented as a Databricks-native pipeline: PostgreSQL -> Unity Catalog Delta bronze -> future silver/gold dbt models.
- Watermark control state is a Delta table. The watermark advances only after the bronze write succeeds.
- A five-minute overlap makes boundary replay safe. Bronze stores a content hash and uses Delta `MERGE`, so retries and overlap rows do not duplicate.
- Database credentials are fetched at runtime from a Databricks secret scope; no secrets are stored in repository files.
- Deployment is declarative through `databricks.yml` and `resources/cdc_job.yml`.
- The Free Edition workspace is serverless-only, so the dev job uses a serverless job environment (`environment_version: "5"`) rather than an existing cluster.
- The PostgreSQL source identifier validator now permits safe schema-qualified tables (for example, `public.orders`); these map to bronze names such as `bronze_public_orders`.
- Lakebase requires TLS, so the PostgreSQL read options include `sslmode=require`.

## Session ledger

### 2026-10-23 — Initial foundation

- Found only `build.txt`; there was no existing code repository or deployment configuration.
- Created a Databricks bundle, Python CDC wheel, source-identifier safety checks, unit tests, deployment guide, and operational runbook.
- Source compilation and manual identifier-safety smoke checks passed. Full pytest and wheel-build checks are pending because this sandbox has neither `pytest` nor `setuptools`, and package-network access is blocked. The Databricks CLI is also absent, so bundle validation/deployment has not run.
- Outstanding inputs: Databricks workspace URL/authentication, Unity Catalog target names, approved existing cluster ID, secret scope, PostgreSQL schema/primary keys, and JDBC driver availability.

### 2026-10-26 — Free Edition development environment prepared

- Databricks CLI v1.22.1 installed by user and authenticated via OAuth profile `cdc-dev`; workspace host is `https://dbc-2662b8bc-8bb0.cloud.databricks.com` (AWS, Free Edition).
- `databricks catalogs list` shows `workspace` as the usable managed catalog. `databricks clusters list` is empty, as expected: Free Edition only offers serverless compute.
- Unity Catalog development schema created and verified: `workspace.olist_cdc_dev`.
- Updated the bundle from an `existing_cluster_id`/Maven-JDBC design to a serverless Python-wheel job with environment version 5. Removed the cluster and Maven-coordinate bundle variables.
- Created Lakebase Autoscaling project `olist-cdc-source`, branch `production`, database `databricks_postgres`, primary compute 1 CU. High availability was attempted but is unsupported on this workspace tier; keep it disabled.
- Lakebase source tables and initial seed rows created: `public.customers` (PK `customer_id`) and `public.orders` (PK `order_id`), both with UTC `updated_at` timestamps. A native Postgres reader role `cdc_reader` was created with read-only grants on these tables.
- Databricks secret scope `olist_cdc` exists with names only: `olist_host`, `olist_port`, `olist_database`, `olist_user`, `olist_password`. Secret values must never be recorded in this file or chat.
- Current development deployment values: catalog `workspace`, schema `olist_cdc_dev`, secret scope `olist_cdc`, source list `public.customers,public.orders`.
- Local `.venv` created; tests pass: `8 passed` after the serverless/source-schema/TLS changes.
- Bundle validation passed:
  `BUNDLE_VAR_source_tables='public.customers,public.orders' databricks bundle validate --profile cdc-dev --target dev --var="postgres_secret_scope=olist_cdc" --var="catalog=workspace" --var="schema=olist_cdc_dev"`

### 2026-10-26 — Deployment and CDC proof (bronze complete)

- Deploy fixes made: artifact build uses `.venv/bin/python` (system python lacked setuptools); wheel path corrected to `../dist/*.whl` and moved into the serverless job `environments[].spec.dependencies` (not task libraries); `cdc-run` CLI now accepts both hyphen and underscore flag spellings because Databricks passes job parameters as underscore flags.
- Job `olist_cdc_job` (id 178870800079194): initial run succeeded (~105 s). Bronze: `bronze_public_customers` 3 rows, `bronze_public_orders` 3 rows; `cdc_watermarks` matched source maxima. The serverless task reached Lakebase directly (no UC JDBC connection needed).
- Incremental proof: `public.orders.order_001` set to `order_status='cdc_demo_updated'` with fresh UTC `updated_at` (12:07:27Z). Rerun 77809525088403 succeeded. Orders bronze Delta v2 MERGE inserted exactly 1 row (4 rows, 3 distinct ids; the old and new versions of order_001 coexist, so bronze is a change history, silver must pick the latest per key). Customers v2 MERGE inserted 0 rows, so the 5-minute replay overlap was deduplicated by content hash. Orders watermark advanced to 2026-10-26T12:07:27.145Z.
- SQL Warehouse for verification: Serverless Starter Warehouse (`7b448eb5dc1a3a02`), queried via the SQL statements API using the `cdc-dev` profile.

### 2026-10-27 — Silver layer with dbt-databricks (complete)

- Installed `dbt-databricks` in `.venv`.
- Configured `dbt_project.yml` and `profiles.yml` connecting via the Databricks CLI OAuth profile `cdc-dev`, targeting catalog `workspace`, schema `olist_cdc_dev`, and SQL warehouse `7b448eb5dc1a3a02`.
- Implemented `silver_customers` and `silver_orders` models deduplicating bronze records to the single latest row per key (`customer_id`, `order_id`) ordered by `updated_at DESC, _cdc_ingested_at DESC`.
- Added generic tests (`unique`, `not_null`) and singular reconciliation tests (`assert_silver_customers_matches_distinct_bronze`, `assert_silver_orders_matches_distinct_bronze`, `assert_silver_orders_latest_status_preserved`).
- Ran `dbt build`: 2 models and 12 tests passed (14/14 green). Verified `order_001` retains latest status `cdc_demo_updated`.

### Next actions (start here)

1. Build gold marts (daily orders aggregate, customer revenue lifetime value).
2. Add a schedule to `olist_cdc_job` (paused by default in dev).
3. Add the remaining Olist source tables to `source_tables` and Lakebase.
4. Consider a delete strategy (soft-delete flag), since `updated_at` polling cannot see hard deletes.

