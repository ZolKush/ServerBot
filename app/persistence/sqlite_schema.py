"""Relational SQLite tables for application entities and revision metadata."""

from __future__ import annotations

import sqlite3

SCHEMA_VERSION = 1
DATABASE_FILENAME = "maintbot.sqlite3"

# Table names are fixed identifiers, never derived from external input.
MAPPING_TABLES = {
    "users.profiles": "user_profiles",
    "access.grants": "access_grants",
    "subscriptions.accounts": "subscription_accounts",
    "subscriptions.requests": "subscription_requests",
    "support.tickets": "tickets",
    "messaging.outbox": "outbox_events",
    "monitoring.dns_cache": "dns_cache",
    "monitoring.node_status_cache": "node_status_cache",
    "monitoring.docker_cache": "docker_cache",
    "monitoring.tls_state": "tls_state",
    "monitoring.fail2ban_cursors": "fail2ban_cursors",
}
DOCUMENT_TABLES = {
    "subscriptions.billing_settings": "billing_settings",
    "settings.help_and_contacts": "help_and_contacts",
    "maintenance.state": "maintenance_state",
}
INDEXED_STORES = {"subscriptions.requests", "support.tickets"}
INDEX_FIELDS = {
    "user_profiles": ("username",),
    "access_grants": ("role", "access_state"),
    "subscription_accounts": ("service_tier", "subscription_end_at"),
    "subscription_requests": ("user_id", "status", "kind"),
    "tickets": ("user_id", "status"),
    "outbox_events": ("origin", "created_at"),
}


def create_schema(connection: sqlite3.Connection) -> None:
    """Create an empty schema inside the caller's transaction."""

    connection.execute(
        "CREATE TABLE storage_metadata (singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
        "schema_version INTEGER NOT NULL, revision INTEGER NOT NULL CHECK(revision>=1), "
        "transaction_id TEXT NOT NULL, migration TEXT)"
    )
    connection.execute(
        "CREATE TABLE store_revisions (store_name TEXT PRIMARY KEY, "
        "revision INTEGER NOT NULL CHECK(revision>=1), next_id INTEGER CHECK(next_id>=0))"
    )
    for table in MAPPING_TABLES.values():
        fields = INDEX_FIELDS.get(table, ())
        extras = "".join(f", {field} TEXT" for field in fields)
        connection.execute(f"CREATE TABLE {table} (record_key TEXT PRIMARY KEY, payload TEXT NOT NULL{extras})")
        for field in fields:
            connection.execute(f"CREATE INDEX {table}_{field}_idx ON {table} ({field})")
    for table in DOCUMENT_TABLES.values():
        connection.execute(f"CREATE TABLE {table} (field_name TEXT PRIMARY KEY, payload TEXT NOT NULL)")
    connection.execute("CREATE TABLE ticket_histories (ticket_key TEXT PRIMARY KEY)")
    connection.execute(
        "CREATE TABLE ticket_messages (ticket_key TEXT NOT NULL REFERENCES ticket_histories(ticket_key) "
        "ON DELETE CASCADE, position INTEGER NOT NULL CHECK(position>=0), payload TEXT NOT NULL, "
        "PRIMARY KEY(ticket_key, position))"
    )
    connection.execute(
        "CREATE TABLE outbox_recipients (event_key TEXT NOT NULL REFERENCES outbox_events(record_key) "
        "ON DELETE CASCADE, recipient_key TEXT NOT NULL, status TEXT, next_attempt_at TEXT, "
        "payload TEXT NOT NULL, PRIMARY KEY(event_key, recipient_key))"
    )
    connection.execute("CREATE INDEX outbox_recipients_due_idx ON outbox_recipients(status, next_attempt_at)")
    connection.execute(
        "CREATE TABLE audit_events (position INTEGER PRIMARY KEY CHECK(position>=0), payload TEXT NOT NULL)"
    )


__all__ = [
    "DATABASE_FILENAME",
    "DOCUMENT_TABLES",
    "INDEXED_STORES",
    "INDEX_FIELDS",
    "MAPPING_TABLES",
    "SCHEMA_VERSION",
    "create_schema",
]
