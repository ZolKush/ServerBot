"""SQLite persistence boundary and explicit legacy JSON compatibility tools.

Importing this package never opens or mutates application data. Runtime code
must construct :class:`SQLiteBackend` after acquiring the process lock.
"""

from .backend import SplitJsonBackend
from .errors import (
    CommittedTransactionError,
    MigrationError,
    PersistenceError,
    PreparedTransactionError,
    RecoveryError,
    SchemaError,
    StorageConflictError,
)
from .interfaces import UnitOfWork
from .snapshots import BackendSnapshot, StoreSnapshot
from .sqlite_backend import SQLiteBackend
from .unit_of_work import JsonUnitOfWork

__all__ = [
    "BackendSnapshot",
    "CommittedTransactionError",
    "JsonUnitOfWork",
    "MigrationError",
    "PersistenceError",
    "PreparedTransactionError",
    "RecoveryError",
    "SchemaError",
    "SplitJsonBackend",
    "SQLiteBackend",
    "StorageConflictError",
    "StoreSnapshot",
    "UnitOfWork",
]
