import logging
import threading

from processor.validator import Outcome

log = logging.getLogger(__name__)


class Consumer:
    def __init__(self, sqs, queue_url, validator, store, batch_size=10, wait_time_seconds=5):
        self.sqs = sqs
        self.queue_url = queue_url
        self.validator = validator
        self.store = store
        self.batch_size = batch_size
        self.wait_time_seconds = wait_time_seconds
        self.stopping = threading.Event()

    def stop(self):
        self.stopping.set()

    def run(self):
        log.info("listening on %s", self.queue_url)
        while not self.stopping.is_set():
            messages = self._receive()
            for i, message in enumerate(messages):
                if self.stopping.is_set():
                    self._release(messages[i:])
                    break
                self._handle(message)
        log.info("consumer done")

    def _receive(self):
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=self.batch_size,
                WaitTimeSeconds=self.wait_time_seconds,
            )
            return response.get("Messages", [])
        except Exception:
            log.exception("receive failed, trying again")
            self.stopping.wait(1)
            return []

    def _handle(self, message):
        raw = message["Body"].encode("utf-8")
        try:
            self._save(raw)
        except Exception:
            log.exception("couldn't process %s, will retry", message.get("MessageId"))
            return

        try:
            self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=message["ReceiptHandle"])
        except Exception:
            log.exception("delete failed for %s", message.get("MessageId"))

    def _save(self, raw):
        result = self.validator.validate(raw)

        if result.outcome is Outcome.BAD:
            self.store.save_bad_message(raw, "; ".join(result.errors))
            log.warning("bad message -> %s", result.errors)
        elif not self.store.save_event(result):
            log.info("dup %s, skipping", result.event_id)
        elif result.outcome is Outcome.INVALID:
            log.warning("invalid %s: %s", result.event_id, result.errors)
        else:
            log.info("saved %s %s", result.event_type, result.event_id)

    def _release(self, messages):
        for message in messages:
            try:
                self.sqs.change_message_visibility(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=message["ReceiptHandle"],
                    VisibilityTimeout=0,
                )
            except Exception:
                log.warning("couldn't release %s", message.get("MessageId"))
