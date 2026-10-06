"""Static checks on the infrastructure definition (no Docker required)."""

import re
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = ROOT / "infra" / "docker-compose.yml"
ENV_EXAMPLE = ROOT / ".env.example"

# one-shot container: exits after migrating the Airflow DB, so it has no healthcheck
ONE_SHOT = {"airflow-init"}


@pytest.fixture(scope="module")
def compose() -> dict:
    return yaml.safe_load(COMPOSE_FILE.read_text())


def _env_example_keys() -> set[str]:
    keys = set()
    for line in ENV_EXAMPLE.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            keys.add(line.split("=", 1)[0])
    return keys


def test_every_image_tag_is_pinned(compose):
    for name, service in compose["services"].items():
        image = service["image"]
        assert ":" in image, f"{name}: image {image!r} has no tag"
        assert not image.endswith(":latest"), f"{name}: floating latest tag"


def test_every_long_running_service_has_a_healthcheck(compose):
    for name, service in compose["services"].items():
        if name not in ONE_SHOT:
            assert "healthcheck" in service, f"{name} has no healthcheck"


def test_every_compose_variable_is_documented_in_env_example():
    # (?<!\$) skips `$${VAR}`: an escaped variable resolved inside the container, not by compose
    used = set(re.findall(r"(?<!\$)\$\{([A-Z][A-Z0-9_]*)", COMPOSE_FILE.read_text()))
    missing = used - _env_example_keys()
    assert not missing, f"used in docker-compose.yml but missing from .env.example: {sorted(missing)}"


def test_every_settings_variable_is_documented_in_env_example():
    used = set(re.findall(r'os\.getenv\("([A-Z][A-Z0-9_]*)"', (ROOT / "config" / "settings.py").read_text()))
    missing = used - _env_example_keys()
    assert not missing, f"read by config/settings.py but missing from .env.example: {sorted(missing)}"


def test_stateful_services_use_named_volumes(compose):
    declared = set(compose["volumes"])
    assert {"postgres-source-data", "postgres-data", "postgres-airflow-data", "kafka-data", "rustfs-data"} <= declared
    for name in ("postgres-source", "postgres", "postgres-airflow", "kafka", "rustfs"):
        assert any(v.split(":")[0] in declared for v in compose["services"][name]["volumes"]), name


def test_host_ports_are_bound_to_loopback_only(compose):
    for name, service in compose["services"].items():
        for port in service.get("ports", []):
            assert str(port).startswith("127.0.0.1:"), f"{name} publishes {port} on all interfaces"


def test_source_postgres_has_logical_replication_enabled(compose):
    assert "wal_level=logical" in compose["services"]["postgres-source"]["command"]


def test_env_example_holds_no_real_secrets():
    for line in ENV_EXAMPLE.read_text().splitlines():
        if re.match(r"^[A-Z_]*(PASSWORD|SECRET|KEY)[A-Z_]*=", line):
            assert line.split("=", 1)[1] in {"CHANGE_ME", "admin"} or line.startswith(("KAFKA_", "RUSTFS_ENDPOINT")), (
                line
            )


def test_env_file_is_not_tracked_by_git():
    if not (ROOT / ".git").exists():
        pytest.skip("not a git checkout")
    tracked = subprocess.run(["git", "ls-files", ".env"], cwd=ROOT, capture_output=True, text=True).stdout
    assert tracked.strip() == ""


def test_kafka_log_dir_is_on_the_named_volume(compose):
    # Regression guard: without KAFKA_LOG_DIRS the broker writes to a container-local temp dir
    # and every topic (and every Kafka Connect connector config) is lost on `docker compose down`.
    kafka = compose["services"]["kafka"]
    mount = next(v.split(":")[1] for v in kafka["volumes"] if v.startswith("kafka-data:"))
    assert kafka["environment"]["KAFKA_LOG_DIRS"] == mount
