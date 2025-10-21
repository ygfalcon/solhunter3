class ValidationError(Exception):
    def __init__(self, message: str = "", *, instance=None, schema=None) -> None:
        super().__init__(message)
        self.message = message
        self.instance = instance
        self.schema = schema


class Draft202012Validator:
    def __init__(self, schema):
        self.schema = schema

    def validate(self, _payload):
        payload = _payload or {}
        schema_props = self.schema.get("properties", {})
        version_schema = schema_props.get("schema_version")
        if isinstance(version_schema, dict) and "const" in version_schema:
            expected = version_schema["const"]
            if "schema_version" in payload and payload.get("schema_version") != expected:
                raise ValidationError(
                    f"schema_version {payload.get('schema_version')} != {expected}",
                    instance=payload,
                    schema=self.schema,
                )
        return True


class _ExceptionsModule:
    ValidationError = ValidationError


exceptions = _ExceptionsModule()
