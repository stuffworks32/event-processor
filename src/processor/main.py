import logging
import signal
import time

import boto3

from processor.config import Config
from processor.consumer import Consumer
from processor.save import PostgresStore
from processor.schemas import SchemaRegistry
from processor.validator import Validator

logging.Formatter.converter = time.gmtime
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)sZ %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logging.getLogger("botocore").setLevel(logging.WARNING)
log = logging.getLogger("processor")


def main():
    config = Config.from_env()
    registry = SchemaRegistry(config.schemas_dir)
    log.info("schemas loaded: %s", sorted(registry.payloads))

    sqs = boto3.client("sqs", endpoint_url=config.sqs_endpoint_url, region_name=config.aws_region)
    queue_url = sqs.get_queue_url(QueueName=config.queue_name)["QueueUrl"]

    store = PostgresStore(config.database_url)
    consumer = Consumer(
        sqs, queue_url, Validator(registry), store,
        batch_size=config.batch_size,
        wait_time_seconds=config.wait_time_seconds,
    )

    def on_signal(signum, _frame):
        log.info("got %s, finishing up", signal.Signals(signum).name)
        consumer.stop()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    try:
        consumer.run()
    finally:
        store.close()
        log.info("stopped")


if __name__ == "__main__":
    main()
