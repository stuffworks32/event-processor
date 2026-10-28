import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any


class Outcome(str, Enum):
    VALID = "VALID"
    INVALID = "INVALID"
    BAD = "BAD"


@dataclass(frozen=True)
class ValidationResult:
    outcome: Outcome
    errors: tuple = ()
    tenant_id: str | None = None
    event_id: str | None = None
    event_type: str | None = None
    schema_version: str | None = None
    occurred_at: datetime | None = None
    payload: Any = None


class Validator:
    def __init__(self, registry):
        self.registry = registry

    def validate(self, raw):
        try:
            msg = json.loads(raw)
        except ValueError as e:
            return bad(f"invalid JSON: {e}")
        if not isinstance(msg, dict):
            return bad("not a JSON object")

        tenant_id = msg.get("tenant_id")
        if not isinstance(tenant_id, str) or not tenant_id.strip():
            return bad("missing tenant_id")
        try:
            event_id = str(uuid.UUID(msg.get("event_id")))
        except (TypeError, ValueError, AttributeError):
            return bad("event_id is not a UUID")

        event_type, version = msg.get("event_type"), msg.get("schema_version")
        errors = schema_errors(self.registry.envelope, msg)
        occurred_at = None

        if not errors:
            occurred_at, error = parse_time(msg["occurred_at"])
            if error:
                errors.append(error)

        if not errors:
            schema = self.registry.payload_validator(event_type, version)
            if schema is None:
                errors.append(f"No schema for {event_type} v{version}")
            else:
                errors = schema_errors(schema, msg["payload"], "payload")

        return ValidationResult(
            outcome=Outcome.INVALID if errors else Outcome.VALID,
            errors=tuple(errors),
            tenant_id=tenant_id,
            event_id=event_id,
            event_type=event_type if isinstance(event_type, str) else None,
            schema_version=version if isinstance(version, str) else None,
            occurred_at=occurred_at,
            payload=msg.get("payload"),
        )


def bad(error):
    return ValidationResult(Outcome.BAD, (error,))


def parse_time(value):
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None, "occurred_at: not an ISO-8601 timestamp"
    if parsed.tzinfo is None:
        return None, "occurred_at: timezone is required"
    return parsed, None


def schema_errors(schema, instance, prefix=""):
    errors = []
    for e in schema.iter_errors(instance):
        where = ".".join([prefix, *map(str, e.absolute_path)]).strip(".")
        errors.append(f"{where or '(root)'}: {e.message}")
    return errors
