from pathlib import Path

from src.ingestion.flume_agent import FlumeAgent
from src.ingestion.validators.event_validator import EventValidator


def test_flume_agent_initializes():
    agent = FlumeAgent()

    assert agent is not None


def test_flume_agent_exposes_lifecycle_operations():
    agent = FlumeAgent()

    assert hasattr(agent, "start")
    assert hasattr(agent, "stop")
    assert hasattr(agent, "status")
    assert hasattr(agent, "healthcheck")


def test_flume_configuration_exists():
    project_root = Path(__file__).resolve().parents[2]

    config_file = project_root / "configs" / "flume" / "flume-conf.properties"

    assert config_file.exists()
    assert config_file.is_file()


def test_flume_configuration_contains_source_and_sink():
    project_root = Path(__file__).resolve().parents[2]

    config_file = project_root / "configs" / "flume" / "flume-conf.properties"
    content = config_file.read_text(encoding="utf-8")

    assert "source" in content.lower()
    assert "sink" in content.lower()


def test_event_validator_accepts_sample_click(sample_click_event):
    validator = EventValidator()

    result = validator.validate(sample_click_event)

    assert result is not None


def test_event_validator_rejects_invalid_event():
    validator = EventValidator()

    invalid_event = {
        "event_id": "invalid-000001",
        "event_type": "UNKNOWN",
    }

    result = validator.validate(invalid_event)

    assert result is False or result is None