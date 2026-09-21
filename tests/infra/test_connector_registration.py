import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("register_connector", ROOT / "infra/kafka-connect/register_connector.py")
register_connector = importlib.util.module_from_spec(spec)
spec.loader.exec_module(register_connector)

ENV = {"SOURCE_POSTGRES_USER": "u", "SOURCE_POSTGRES_PASSWORD": "p", "SOURCE_POSTGRES_DB": "d"}


def test_placeholders_are_substituted():
    connector = register_connector.render_connector(register_connector.CONNECTOR_FILE.read_text(), ENV)
    assert connector["config"]["database.user"] == "u"
    assert connector["config"]["database.dbname"] == "d"
    assert "${" not in json.dumps(connector)


def test_missing_variable_fails_loudly():
    with pytest.raises(KeyError):
        register_connector.render_connector(register_connector.CONNECTOR_FILE.read_text(), {})


def test_topic_matches_env_example_and_settings():
    connector = register_connector.render_connector(register_connector.CONNECTOR_FILE.read_text(), ENV)
    expected = f"{connector['config']['topic.prefix']}.{connector['config']['table.include.list']}"
    assert expected == "telecom.source.network_events"
    assert f"KAFKA_TOPIC_NETWORK_EVENTS={expected}" in (ROOT / ".env.example").read_text()


def test_decimal_handling_is_not_base64_default():
    connector = register_connector.render_connector(register_connector.CONNECTOR_FILE.read_text(), ENV)
    assert connector["config"]["decimal.handling.mode"] in {"double", "string"}
