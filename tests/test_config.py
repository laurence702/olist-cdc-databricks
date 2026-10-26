import pytest

from cdc_pipeline.config import qualified
from cdc_pipeline.pipeline import WatermarkCdcPipeline


def test_qualified_quotes_three_part_identifier():
    assert qualified("main.olist_cdc", "bronze_orders") == "`main`.`olist_cdc`.`bronze_orders`"


@pytest.mark.parametrize("namespace", ["main", "main.schema.extra", "main.bad-name"])
def test_qualified_rejects_unsafe_identifier(namespace):
    with pytest.raises(ValueError):
        qualified(namespace, "orders")


def test_source_table_allows_a_schema_qualified_identifier():
    WatermarkCdcPipeline._validate_source_table("public.orders")


@pytest.mark.parametrize("table", ["orders; drop table x", "public.sales.orders", "orders-name"])
def test_source_table_rejects_sql_injection(table):
    with pytest.raises(ValueError):
        WatermarkCdcPipeline._validate_source_table(table)
