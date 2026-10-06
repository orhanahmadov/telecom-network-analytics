"""
Create .env from .env.example, replacing every CHANGE_ME with a fresh random secret.

    python3 scripts/init_env.py          # creates .env (refuses to overwrite)
    python3 scripts/init_env.py --force  # regenerates .env (ALL secrets change)

Standard library only, so it runs before any virtualenv exists.
Note: regenerating secrets after the stack has been started once will not match the
passwords stored in the existing Docker volumes - use `make clean` first if you do.
"""

from __future__ import annotations

import argparse
import base64
import os
import secrets
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLACEHOLDER = "CHANGE_ME"


def _fernet_key() -> str:
    # Fernet key = url-safe base64 of 32 random bytes (44 chars, with padding).
    return base64.urlsafe_b64encode(os.urandom(32)).decode()


def _kafka_cluster_id() -> str:
    # Kafka wants a url-safe base64 UUID, 22 chars, WITHOUT padding.
    # Standard base64 (with '+', '/', '==') is rejected.
    return base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("=")


def generate_value(key: str) -> str:
    if key == "AIRFLOW_FERNET_KEY":
        return _fernet_key()
    if key == "KAFKA_CLUSTER_ID":
        return _kafka_cluster_id()
    # Hex only: safe inside URLs / connection strings (no special characters to escape).
    return secrets.token_hex(16)


def render_env(template_text: str) -> str:
    lines = []
    for line in template_text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key, _, value = stripped.partition("=")
            if value.strip() == PLACEHOLDER:
                line = f"{key}={generate_value(key)}"
        lines.append(line)
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--force", action="store_true", help="overwrite an existing .env")
    args = parser.parse_args(argv)

    template = ROOT / ".env.example"
    target = ROOT / ".env"
    if target.exists() and not args.force:
        print(f"{target} already exists - leaving it untouched (use --force to regenerate).")
        return 0

    target.write_text(render_env(template.read_text()))
    target.chmod(0o600)  # secrets: owner read/write only
    print(f"Created {target} with freshly generated secrets. It is git-ignored - never commit it.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
