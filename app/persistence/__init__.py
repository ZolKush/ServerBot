"""Split JSON persistence boundary.

Importing this package never opens or mutates application data. Runtime code
must construct :class:`SplitJsonBackend` after acquiring the process lock.
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
    "StorageConflictError",
    "StoreSnapshot",
    "UnitOfWork",
]
