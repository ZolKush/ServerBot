"""Domain errors raised by the split JSON persistence backend."""


class PersistenceError(RuntimeError):
    """Base class for persistence failures."""


class SchemaError(PersistenceError):
    """A persisted document does not match its declared schema."""


class StorageConflictError(PersistenceError):
    """The on-disk revision or target layout conflicts with the operation."""


class RecoveryError(PersistenceError):
    """A prepared transaction cannot be recovered safely."""


class MigrationError(PersistenceError):
    """The one-shot v4 migration cannot proceed safely."""


class DuplicateJsonKeyError(SchemaError):
    """A JSON object contains a duplicate key."""
