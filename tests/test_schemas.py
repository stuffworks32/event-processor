import json

import pytest
from jsonschema.exceptions import SchemaError

from processor.schemas import SchemaRegistry


def test_broken_schema_fails_on_startup(tmp_path):
    (tmp_path / "envelope.json").write_text(json.dumps({"type": "object"}))
    (tmp_path / "card.blocked").mkdir()
    (tmp_path / "card.blocked" / "v1.json").write_text(json.dumps({"type": 123}))

    with pytest.raises(SchemaError):
        SchemaRegistry(tmp_path)
