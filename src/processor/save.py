import hashlib
import uuid

import psycopg
from psycopg.types.json import Jsonb

INSERT_EVENT = """
    INSERT INTO received_events
        (tenant_id, event_id, event_type, schema_version, payload, status, errors, occurred_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (tenant_id, event_id) DO NOTHING
"""

INSERT_BAD = """
    INSERT INTO bad_messages (body_hash, raw_body, error)
    VALUES (%s, %s, %s)
    ON CONFLICT (body_hash) DO NOTHING
"""


class PostgresStore:
    def __init__(self, conninfo):
        self.conninfo = conninfo
        self.conn = None

    def save_event(self, result):
        return self._insert(INSERT_EVENT, (
            result.tenant_id,
            uuid.UUID(result.event_id),
            result.event_type,
            result.schema_version,
            Jsonb(result.payload) if result.payload is not None else None,
            result.outcome.value,
            Jsonb(list(result.errors)) if result.errors else None,
            result.occurred_at,
        ))

    def save_bad_message(self, raw, error):
        return self._insert(INSERT_BAD, (hashlib.sha256(raw).digest(), raw, error))

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()

    def _insert(self, query, params):
        # returns False when the row already existed (ON CONFLICT DO NOTHING)
        with self._connection().cursor() as cur:
            cur.execute(query, params)
            return cur.rowcount == 1

    def _connection(self):
        # reconnect if postgres restarted; if this fails the message stays on the queue
        if self.conn is None or self.conn.closed or self.conn.broken:
            self.close()
            self.conn = psycopg.connect(self.conninfo, autocommit=True, connect_timeout=5)
        return self.conn
