from pathlib import Path

import pytest

from processor.schemas import SchemaRegistry
from processor.validator import Validator

SCHEMAS = Path(__file__).resolve().parents[1] / "schemas"


@pytest.fixture(scope="session")
def validator():
    return Validator(SchemaRegistry(SCHEMAS))
