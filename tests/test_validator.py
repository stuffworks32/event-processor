import json

from processor.validator import Outcome


def event(**changes):
    body = {
        "event_id": "3f6c2a9e-1b7d-4c8e-9a2f-5d1e8b7c4a10",
        "event_type": "card.blocked",
        "schema_version": "1",
        "tenant_id": "bank-a",
        "occurred_at": "2026-10-24T10:05:00Z",
        "payload": {"card_id": "card-9", "reason": "STOLEN"},
    }
    body.update(changes)
    return json.dumps(body).encode()


def test_valid_event(validator):
    result = validator.validate(event())
    assert result.outcome is Outcome.VALID


def test_bad_reason_is_invalid(validator):
    result = validator.validate(event(payload={"card_id": "card-9", "reason": "MISPLACED"}))
    assert result.outcome is Outcome.INVALID


def test_unknown_schema_version_is_invalid(validator):
    result = validator.validate(event(schema_version="9"))
    assert result.outcome is Outcome.INVALID


def test_missing_tenant_is_bad(validator):
    result = validator.validate(event(tenant_id=None))
    assert result.outcome is Outcome.BAD


def test_event_id_not_uuid_is_bad(validator):
    result = validator.validate(event(event_id="abc"))
    assert result.outcome is Outcome.BAD


def test_truncated_json_is_bad(validator):
    result = validator.validate(b'{"event_id": "3f6c2a9e')
    assert result.outcome is Outcome.BAD
