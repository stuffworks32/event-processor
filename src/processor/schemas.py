import json

from jsonschema import Draft202012Validator


class SchemaRegistry:
    def __init__(self, schemas_dir):
        self.envelope = load(schemas_dir / "envelope.json")
        self.payloads = {
            (path.parent.name, path.stem[1:]): load(path)
            for path in schemas_dir.glob("*/v*.json")
        }
        if not self.payloads:
            raise ValueError(f"no payload schemas in {schemas_dir}")

    def payload_validator(self, event_type, schema_version):
        return self.payloads.get((event_type, schema_version))


def load(path):
    schema = json.loads(path.read_text())
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)
