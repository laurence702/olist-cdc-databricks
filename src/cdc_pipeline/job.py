from __future__ import annotations

import argparse
import logging

from cdc_pipeline.config import PipelineConfig
from cdc_pipeline.pipeline import WatermarkCdcPipeline


def parse_args() -> PipelineConfig:
    parser = argparse.ArgumentParser(description="Run watermark CDC from PostgreSQL to Delta.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    # Databricks Jobs renders named parameters with underscores. Keep the
    # hyphenated aliases for direct local invocations.
    parser.add_argument("--secret-scope", "--secret_scope", dest="secret_scope", required=True)
    parser.add_argument("--secret-prefix", "--secret_prefix", dest="secret_prefix", default="olist_")
    parser.add_argument("--source-tables", "--source_tables", dest="source_tables", required=True)
    parser.add_argument("--lookback-minutes", "--lookback_minutes", dest="lookback_minutes", type=int, default=5)
    args = parser.parse_args()
    return PipelineConfig(
        catalog=args.catalog,
        schema=args.schema,
        secret_scope=args.secret_scope,
        secret_prefix=args.secret_prefix,
        source_tables=tuple(table.strip() for table in args.source_tables.split(",") if table.strip()),
        lookback_minutes=args.lookback_minutes,
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config = parse_args()
    from pyspark.sql import SparkSession

    WatermarkCdcPipeline(SparkSession.builder.getOrCreate(), config).run()


if __name__ == "__main__":
    main()
