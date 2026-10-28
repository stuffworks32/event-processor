# event-processor

A small service that reads events from SQS, checks them against JSON Schema contracts, and writes them to Postgres so a separate sender service can deliver them to clients later.

Invalid events are still saved (with their errors), messages that can't even be identified go to a quarantine table, and nothing gets dropped silently. The reasoning behind all of this is in [DESIGN.md](DESIGN.md).

## Running it

You only need Docker (with Compose v2).

```bash
git clone https://github.com/stuffworks32/event-processor.git
cd event-processor
docker compose up --build -d
```

That starts three containers:

- `postgres` holds `received_events` and `bad_messages`
- `localstack` runs SQS locally, with `events-queue` and its dead-letter queue `events-dlq`
- `processor` is the service itself; it waits until Postgres is up and the queues exist

## Sending some events

```bash
docker compose run --rm processor python scripts/produce.py
```

This sends 11 messages that cover every path:

| What's sent | Where it should end up |
|---|---|
| one valid event of each type (3) | `received_events`, status `VALID` |
| bad enum, negative amount, unknown type, missing `occurred_at` (4) | `received_events`, status `INVALID` |
| no `tenant_id`, `event_id` that isn't a UUID, truncated JSON (3) | `bad_messages` |
| the first event again | nothing new, logged as a duplicate |

## Checking the results

```bash
docker compose exec postgres psql -U events -d events
```

```sql
SELECT status, count(*) FROM received_events GROUP BY status;   -- 3 VALID, 4 INVALID
SELECT event_type, errors FROM received_events WHERE status = 'INVALID';
SELECT error, loaded_at FROM bad_messages;                       -- 3 rows
```

Type `\q` to exit psql.

The queues should be empty afterwards:

```bash
docker compose exec localstack awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/events-queue \
  --attribute-names ApproximateNumberOfMessages
```

(Same command with `events-dlq` for the dead-letter queue.)

You can also just watch the logs: `docker compose logs -f processor`.

## Tests

```bash
docker compose run --rm --no-deps processor pytest
```

These are unit tests, so no other containers need to be running.

## Stopping and resetting

`docker compose stop processor` stops it gracefully: it finishes what it's working on and hands any unstarted messages back to the queue.

`docker compose down -v` removes everything, including the database volume. The next `up` recreates the tables and queues.

## Configuration

All set in `docker-compose.yml`:

| Variable | Value |
|---|---|
| `SQS_ENDPOINT_URL` | `http://localstack:4566` |
| `SQS_QUEUE_NAME` | `events-queue` |
| `AWS_REGION` | `us-east-1` |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | `local` (LocalStack doesn't check them) |
| `DATABASE_URL` | `postgresql://events:events@postgres:5432/events` |

The database credentials are only for local use.

## Layout

```
docker-compose.yml
Dockerfile
db/events.sql                 tables and indexes
localstack/create-queues.sh   creates the queues on startup
schemas/                      envelope.json + <event_type>/v<N>.json
src/processor/                the service
scripts/produce.py            sends test events
tests/
```

## Adding a new event type

Drop a schema at `schemas/<event_type>/v1.json` and restart the processor. No code changes.

## Notes

- LocalStack is pinned to 4.12. Newer versions need an auth token, and I didn't want anyone to have to sign up just to run this.
- All timestamps are UTC.