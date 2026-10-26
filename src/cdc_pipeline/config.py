from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str
    schema: str
    secret_scope: str
    secret_prefix: str
    source_tables: tuple[str, ...]
    lookback_minutes: int = 5

    @property
    def namespace(self) -> str:
        return f"{self.catalog}.{self.schema}"


def qualified(namespace: str, name: str) -> str:
    """Return a Unity Catalog-safe three-part table name."""
    parts = (*namespace.split("."), name)
    if len(parts) != 3 or any(not part.replace("_", "").isalnum() for part in parts):
        raise ValueError(f"Unsafe Unity Catalog identifier: {namespace}.{name}")
    return ".".join(f"`{part}`" for part in parts)
