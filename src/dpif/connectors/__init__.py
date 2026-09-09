"""Databricks connector abstractions (offline-first)."""

from dpif.connectors.base import DatabricksConnector
from dpif.connectors.offline import OfflineDatabricksConnector

__all__ = ["DatabricksConnector", "OfflineDatabricksConnector"]
