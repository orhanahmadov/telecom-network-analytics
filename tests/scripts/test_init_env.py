import base64
import uuid

from scripts import init_env


def test_every_placeholder_is_replaced():
    rendered = init_env.render_env((init_env.ROOT / ".env.example").read_text())
    values = [
        line.split("=", 1)[1]
        for line in rendered.splitlines()
        if line.strip() and not line.startswith("#") and "=" in line
    ]
    assert "CHANGE_ME" not in values
    assert all(v for v in values), "an empty value would break docker compose"


def test_non_secret_values_are_preserved():
    rendered = init_env.render_env("POSTGRES_USER=telecom_app\nPOSTGRES_PASSWORD=CHANGE_ME\n")
    assert "POSTGRES_USER=telecom_app" in rendered
    assert "POSTGRES_PASSWORD=CHANGE_ME" not in rendered


def test_comments_are_preserved():
    assert "# a comment" in init_env.render_env("# a comment\nX=CHANGE_ME\n")


def test_fernet_key_is_valid():
    key = init_env.generate_value("AIRFLOW_FERNET_KEY")
    assert len(base64.urlsafe_b64decode(key)) == 32


def test_kafka_cluster_id_is_valid_for_kraft():
    # Regression guard: standard base64 ('+', '/', '==') is rejected by Kafka.
    for _ in range(200):
        cluster_id = init_env.generate_value("KAFKA_CLUSTER_ID")
        assert len(cluster_id) == 22 and "=" not in cluster_id and "+" not in cluster_id and "/" not in cluster_id
        assert len(base64.urlsafe_b64decode(cluster_id + "==")) == 16


def test_passwords_are_url_safe():
    value = init_env.generate_value("POSTGRES_PASSWORD")
    assert value.isalnum() and len(value) >= 32


def test_secrets_differ_between_runs():
    assert init_env.generate_value("POSTGRES_PASSWORD") != init_env.generate_value("POSTGRES_PASSWORD")
