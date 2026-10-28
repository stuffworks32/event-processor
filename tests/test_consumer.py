import json
from unittest.mock import MagicMock

from processor.consumer import Consumer
from processor.validator import Outcome

EVENT = {
    "event_id": "3f6c2a9e-1b7d-4c8e-9a2f-5d1e8b7c4a10",
    "event_type": "card.blocked",
    "schema_version": "1",
    "tenant_id": "bank-a",
    "occurred_at": "2026-10-24T10:05:00Z",
    "payload": {"card_id": "card-9", "reason": "STOLEN"},
}


def handle(validator, body, is_new=True):
    sqs, save = MagicMock(), MagicMock()
    save.save_event.return_value = is_new
    message = {"MessageId": "m1", "ReceiptHandle": "m1", "Body": body}

    Consumer(sqs, "q", validator, save)._handle(message)
    return sqs, save


def test_valid_event_is_saved_and_deleted(validator):
    sqs, save = handle(validator, json.dumps(EVENT))

    assert save.save_event.call_args.args[0].outcome is Outcome.VALID
    sqs.delete_message.assert_called_once()


def test_invalid_event_is_saved_and_deleted(validator):
    invalid = {**EVENT, "payload": {"card_id": "card-9", "reason": "MISPLACED"}}
    sqs, save = handle(validator, json.dumps(invalid))

    assert save.save_event.call_args.args[0].outcome is Outcome.INVALID
    sqs.delete_message.assert_called_once()


def test_bad_message_is_quarantined_and_deleted(validator):
    sqs, save = handle(validator, '{"event_id": "trunc')

    save.save_bad_message.assert_called_once()
    save.save_event.assert_not_called()
    sqs.delete_message.assert_called_once()


def test_duplicate_is_still_deleted(validator):
    sqs, save = handle(validator, json.dumps(EVENT), is_new=False)

    sqs.delete_message.assert_called_once()
