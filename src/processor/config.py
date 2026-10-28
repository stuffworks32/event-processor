import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_SCHEMAS = Path(__file__).resolve().parents[2] / "schemas"


@dataclass
class Config:
    sqs_endpoint_url: str | None
    queue_name: str
    aws_region: str
    database_url: str
    schemas_dir: Path
    batch_size: int
    wait_time_seconds: int

    @classmethod
    def from_env(cls):
        if not os.getenv("DATABASE_URL"):
            raise ValueError("DATABASE_URL is required")

        return cls(
            sqs_endpoint_url=os.getenv("SQS_ENDPOINT_URL") or None,
            queue_name=os.getenv("SQS_QUEUE_NAME", "events-queue"),
            aws_region=os.getenv("AWS_REGION", "us-east-1"),
            database_url=os.environ["DATABASE_URL"],
            schemas_dir=Path(os.getenv("SCHEMAS_DIR", DEFAULT_SCHEMAS)),
            batch_size=int(os.getenv("SQS_BATCH_SIZE", "10")),
            wait_time_seconds=int(os.getenv("SQS_WAIT_TIME_SECONDS", "5")),
        )
