from utils.models import EmailDraft
def test_schema_roundtrip():
    schema = EmailDraft.model_json_schema()
    assert "properties" in schema and "subject" in schema["properties"]
