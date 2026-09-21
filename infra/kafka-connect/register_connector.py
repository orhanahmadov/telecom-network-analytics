"""
Register (or update) the Debezium PostgreSQL source connector.

Uses PUT /connectors/<name>/config, which is create-or-update: running this
script twice is safe (idempotent). Standard library only - no curl/envsubst
needed on the host, so it behaves the same on Linux, macOS and Windows.

Usage (stack must be up and kafka-connect healthy):
    make register-connector
    # or:  python infra/kafka-connect/register_connector.py
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from string import Template

CONNECTOR_FILE = Path(__file__).with_name("postgres-source-connector.json")


def render_connector(template_text: str, env: dict[str, str]) -> dict:
    """Substitute ${VAR} placeholders from `env`; fail loudly on a missing variable."""
    # Template.substitute raises KeyError for an unknown placeholder - that is intended.
    return json.loads(Template(template_text).substitute(env))


def register(connect_url: str, connector: dict) -> dict:
    url = f"{connect_url.rstrip('/')}/connectors/{connector['name']}/config"
    request = urllib.request.Request(
        url,
        data=json.dumps(connector["config"]).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def main() -> int:
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:  # dotenv optional: variables may already be exported
        pass

    connect_url = os.environ.get("KAFKA_CONNECT_URL", "http://localhost:8083")
    try:
        connector = render_connector(CONNECTOR_FILE.read_text(), dict(os.environ))
    except KeyError as exc:
        print(f"Missing environment variable for connector config: {exc}", file=sys.stderr)
        return 1

    try:
        result = register(connect_url, connector)
    except urllib.error.URLError as exc:
        print(f"Could not reach Kafka Connect at {connect_url}: {exc}", file=sys.stderr)
        return 1

    print(f"Connector registered: {result.get('name')}")
    print(f"Check status: curl {connect_url}/connectors/{result.get('name')}/status")
    return 0


if __name__ == "__main__":
    sys.exit(main())
