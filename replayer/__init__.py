"""
Replayer module for streaming CDC-to-Kafka topics back to SQL Server tables.

This module provides tools for replaying Kafka topics produced by CDC-to-Kafka
back into SQL Server tables, useful for creating table copies, migrating data,
or maintaining read replicas.

Example usage:
    python -m replayer --help
"""

from .cli import main
from .models import LsnPosition, OrderedOperation, Progress, ReplayConfig
from .table_metadata import FollowModeTableMetadata, TableMetadata

__all__ = [
    'main',
    'LsnPosition',
    'OrderedOperation',
    'Progress',
    'ReplayConfig',
    'TableMetadata',
    'FollowModeTableMetadata',
]
