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

## Next implementation slice

Build silver deduplication and dbt models (including SCD Type 2 customers and order facts) after confirming the actual PostgreSQL table schemas and primary keys. They cannot be safely inferred from the high-level brief alone.
