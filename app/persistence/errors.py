"""Domain errors raised by the split JSON persistence backend."""


class PersistenceError(RuntimeError):
    """Base class for persistence failures."""


class SchemaError(PersistenceError):
    """A persisted document does not match its declared schema."""


class StorageConflictError(PersistenceError):
    """The on-disk revision or target layout conflicts with the operation."""


class RecoveryError(PersistenceError):
    """A prepared transaction cannot be recovered safely."""


class PreparedTransactionError(RecoveryError):
    """A transaction crossed its durable commit point but still needs redo."""

    def __init__(self, transaction_id: str) -> None:
        self.transaction_id = transaction_id
        super().__init__(
            f"transaction {transaction_id} is durably prepared and requires recovery; the mutation must not be retried"
        )


class CommittedTransactionError(PersistenceError):
    """A transaction committed durably but its result could not be reloaded."""

    def __init__(self, transaction_id: str) -> None:
        self.transaction_id = transaction_id
        super().__init__(
            f"transaction {transaction_id} is durably committed but its result could not be reloaded; "
            "the mutation must not be retried"
        )


class MigrationError(PersistenceError):
    """The one-shot v4 migration cannot proceed safely."""


class DuplicateJsonKeyError(SchemaError):
    """A JSON object contains a duplicate key."""
