# Olist CDC on Databricks

An idempotent, watermark-based CDC pipeline that extracts PostgreSQL order-lifecycle changes into Unity Catalog Delta tables. It is deployed as a Databricks Declarative Automation Bundle (DAB) and intentionally uses a small overlap window (`updated_at >= watermark`) so boundary records are replayed safely.

## What is implemented

- Databricks Job defined as code, with isolated `dev` and `prod` targets.
- JDBC extraction from PostgreSQL; credentials come only from a Databricks secret scope.
- One Delta bronze table per source table and a Delta control table (`cdc_watermarks`).
- Bronze uses a content hash and Delta `MERGE` to make overlap reads and retries idempotent. The watermark advances only after that merge succeeds.
- Single concurrent run to avoid competing watermark writers.

The current source contract requires every source table to have a UTC `updated_at` timestamp. Watermark CDC does **not** detect hard deletes; use a source soft-delete column or migrate to Postgres logical replication/Debezium if delete capture is required.

## Prerequisites

1. A Unity Catalog-enabled Databricks workspace and an approved existing cluster with network access to PostgreSQL. Set its ID at deployment time; this avoids binding the bundle to one cloud vendor or instance type.
2. Databricks CLI authenticated to the target workspace (`databricks auth login --host <workspace-url>`).
3. A secret scope with five secrets. For prefix `olist_`: `olist_host`, `olist_port`, `olist_database`, `olist_user`, `olist_password`.
4. On Databricks Free Edition, PostgreSQL uses the serverless-bundled connector. The PostgreSQL host must be reachable from Databricks serverless compute.
5. Local build tooling: `python -m pip install setuptools wheel pytest`.

## Deploy

```bash
python -m pip install setuptools wheel pytest
python -m pip install -e .
pytest
databricks bundle validate --target dev \
  --var="postgres_secret_scope=YOUR_SCOPE" \
  --var="catalog=workspace" \
  --var="schema=olist_cdc_dev"
databricks bundle deploy --target dev \
  --var="postgres_secret_scope=YOUR_SCOPE" \
  --var="catalog=workspace" \
  --var="schema=olist_cdc_dev"
databricks bundle run olist_cdc_job --target dev
```

For production, review the compute policy, set a production catalog/schema, then repeat with `--target prod`. Do not put passwords or tokens in `databricks.yml`, a `.tfvars`, or git.

## Recovery and operations

- **Replay a source:** `DELETE FROM <catalog>.<schema>.cdc_watermarks WHERE source_table = 'orders'`; the next run re-extracts its history.
- **Inspect progress:** query `<catalog>.<schema>.cdc_watermarks` and the job run log.
- **Backfill safely:** run a separate catalog/schema first, validate counts and maximum `updated_at`, then promote downstream consumers.

## Silver Layer & Transformations (dbt-databricks)

The bronze tables retain the raw CDC stream, where repeated updates create multiple rows per primary key (for example, `order_001` holds both initial `approved` and later `cdc_demo_updated` rows).

The silver layer transforms and conforms these streams into clean, current-state business entities:

1. **Deduplication Strategy:** Each silver model computes a deterministic window rank:
   ```sql
   ROW_NUMBER() OVER (
       PARTITION BY <primary_key>
       ORDER BY updated_at DESC, _cdc_ingested_at DESC
   )
   ```
   Filtering on `_row_num = 1` guarantees exactly one row per entity representing its latest state. The `_cdc_ingested_at` column acts as an immutable tiebreaker for updates arriving with identical timestamps.
2. **Delta Lake Storage:** Silver models materialise as native Delta tables in Unity Catalog (`workspace.olist_cdc_dev`), providing ACID transactions, time travel, and file compaction.
3. **Zero-Secret Authentication:** The dbt project authenticates via the Databricks CLI OAuth profile (`cdc-dev`) connecting to the Serverless SQL Warehouse (`7b448eb5dc1a3a02`). No static credentials, tokens, or passwords exist in repository files.

### Running Silver Models

```bash
DATABRICKS_CONFIG_PROFILE=cdc-dev .venv/bin/dbt build --profiles-dir .
```

### Data Quality & Reconciliation Tests

The test suite validates:
- **Schema tests:** `unique` and `not_null` constraints on primary keys (`customer_id`, `order_id`) and critical audit columns (`updated_at`).
- **Row count reconciliation:** Singular data tests assert that silver row counts match `COUNT(DISTINCT primary_key)` in bronze, proving that no records vanished during deduplication.
- **State freshness:** Asserts that entity updates (such as `order_001` transitioning to `cdc_demo_updated`) correctly reflect the latest source state.

