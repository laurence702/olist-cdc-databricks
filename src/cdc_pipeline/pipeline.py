from __future__ import annotations

import logging
from datetime import datetime, timezone

from cdc_pipeline.config import PipelineConfig, qualified

LOGGER = logging.getLogger(__name__)
EPOCH = "1970-01-01 00:00:00"


class WatermarkCdcPipeline:
    """Idempotent JDBC-to-Delta extraction. A watermark advances only after MERGE succeeds."""

    def __init__(self, spark, config: PipelineConfig):
        self.spark = spark
        self.config = config

    def run(self) -> None:
        self._create_namespace_and_control_table()
        for source_table in self.config.source_tables:
            self._extract_and_merge(source_table)

    def _dbutils(self):
        from pyspark.dbutils import DBUtils

        return DBUtils(self.spark)

    def _secret(self, key: str) -> str:
        return self._dbutils().secrets.get(self.config.secret_scope, f"{self.config.secret_prefix}{key}")

    def _create_namespace_and_control_table(self) -> None:
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{self.config.catalog}`.`{self.config.schema}`")
        self.spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {qualified(self.config.namespace, 'cdc_watermarks')} (
                source_table STRING NOT NULL,
                watermark TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            ) USING DELTA"""
        )

    def _watermark(self, source_table: str) -> str:
        rows = self.spark.sql(
            f"SELECT watermark FROM {qualified(self.config.namespace, 'cdc_watermarks')} "
            f"WHERE source_table = {self._literal(source_table)} ORDER BY updated_at DESC LIMIT 1"
        ).collect()
        return str(rows[0].watermark) if rows else EPOCH

    def _extract_and_merge(self, source_table: str) -> None:
        self._validate_source_table(source_table)
        watermark = self._watermark(source_table)
        query = (
            f"(SELECT * FROM {source_table} WHERE updated_at >= "
            f"(TIMESTAMP '{watermark}' - INTERVAL '{self.config.lookback_minutes} minutes') "
            f"ORDER BY updated_at ASC) AS cdc_source"
        )
        # PostgreSQL is a serverless-bundled connector. This avoids attaching a
        # Maven JDBC JAR, which is not supported as a cluster library on
        # serverless compute.
        changes = (
            self.spark.read.format("postgresql")
            .option("host", self._secret("host"))
            .option("port", self._secret("port"))
            .option("database", self._secret("database"))
            .option("sslmode", "require")
            .option("dbtable", query)
            .option("user", self._secret("user"))
            .option("password", self._secret("password"))
            .load()
        )
        if changes.isEmpty():
            LOGGER.info("No changes for %s after %s", source_table, watermark)
            return
        from pyspark.sql import functions as F

        target = qualified(self.config.namespace, f"bronze_{source_table.replace('.', '_')}")
        changes = changes.dropDuplicates().withColumn(
            "_cdc_record_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in changes.columns])), 256)
        ).withColumn("_cdc_ingested_at", F.current_timestamp())
        if not self.spark.catalog.tableExists(target.replace("`", "")):
            changes.limit(0).write.format("delta").option("mergeSchema", "true").saveAsTable(target)
        changes.createOrReplaceTempView("cdc_changes")
        self.spark.sql(
            f"""MERGE INTO {target} AS target
                USING cdc_changes AS source
                ON target._cdc_record_hash = source._cdc_record_hash
                WHEN NOT MATCHED THEN INSERT *"""
        )
        next_watermark = changes.agg({"updated_at": "max"}).first()[0]
        self._advance_watermark(source_table, next_watermark)
        LOGGER.info("Extracted %s rows from %s; watermark advanced to %s", changes.count(), source_table, next_watermark)

    def _advance_watermark(self, source_table: str, watermark) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        control = qualified(self.config.namespace, "cdc_watermarks")
        self.spark.sql(
            f"DELETE FROM {control} WHERE source_table = {self._literal(source_table)}"
        )
        self.spark.sql(
            f"INSERT INTO {control} VALUES ({self._literal(source_table)}, TIMESTAMP '{watermark}', TIMESTAMP '{now}')"
        )

    @staticmethod
    def _literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    @staticmethod
    def _validate_source_table(table: str) -> None:
        parts = table.split(".")
        if len(parts) not in (1, 2) or any(not part.replace("_", "").isalnum() for part in parts):
            raise ValueError(f"Unsafe PostgreSQL table name: {table}")
