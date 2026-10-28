"""Send a mix of test messages to the events queue.

docker compose run --rm processor python scripts/produce.py
"""

import json
import os
import uuid
from datetime import datetime, timezone

import boto3


def make_event(event_type, tenant_id, payload):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "schema_version": "1",
        "tenant_id": tenant_id,
        "occurred_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "payload": payload,
    }


def test_messages():
    authorized = make_event("transaction.authorized", "bank-a", {"account_id": "acc-1", "amount": 4500})
    blocked = make_event("card.blocked", "bank-b", {"card_id": "card-9", "reason": "STOLEN"})
    threshold = make_event("spend.threshold_exceeded", "bank-a",
                           {"account_id": "acc-1", "amount": 10500, "threshold": 10000})

    bad_reason = make_event("card.blocked", "bank-b", {"card_id": "card-7", "reason": "MISPLACED"})
    negative = make_event("transaction.authorized", "bank-c", {"account_id": "acc-2", "amount": -5})
    unknown_type = make_event("card.exploded", "bank-a", {"card_id": "card-1"})
    no_time = make_event("card.blocked", "bank-c", {"card_id": "card-3", "reason": "LOST"})
    del no_time["occurred_at"]

    no_tenant = make_event("card.blocked", "bank-a", {"card_id": "card-4", "reason": "LOST"})
    del no_tenant["tenant_id"]
    bad_id = make_event("card.blocked", "bank-a", {"card_id": "card-5", "reason": "LOST"})
    bad_id["event_id"] = "abc"
    truncated = json.dumps(make_event("transaction.authorized", "bank-a", {"account_id": "acc-3", "amount": 1}))[:60]

    first = json.dumps(authorized)
    return [
        ("valid transaction.authorized", first),
        ("valid card.blocked", json.dumps(blocked)),
        ("valid spend.threshold_exceeded", json.dumps(threshold)),
        ("invalid: bad reason", json.dumps(bad_reason)),
        ("invalid: negative amount", json.dumps(negative)),
        ("invalid: unknown event type", json.dumps(unknown_type)),
        ("invalid: missing occurred_at", json.dumps(no_time)),
        ("bad: missing tenant_id", json.dumps(no_tenant)),
        ("bad: event_id not a UUID", json.dumps(bad_id)),
        ("bad: truncated JSON", truncated),
        ("duplicate of the first event", first),
    ]


def main():
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "local")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "local")

    sqs = boto3.client(
        "sqs",
        endpoint_url=os.getenv("SQS_ENDPOINT_URL", "http://localhost:4566"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )
    queue_url = sqs.get_queue_url(QueueName=os.getenv("SQS_QUEUE_NAME", "events-queue"))["QueueUrl"]

    for label, body in test_messages():
        sqs.send_message(QueueUrl=queue_url, MessageBody=body)
        print(f"sent {label}")

    print("expect 3 VALID, 4 INVALID, 3 bad messages, 1 duplicate ignored")


if __name__ == "__main__":
    main()
