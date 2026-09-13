from pathlib import Path

from src.data_generator.gerador import EventGeneratorService


def test_generator_creates_output_file(tmp_path: Path):
    output_file = tmp_path / "events.jsonl"

    generator = EventGeneratorService(
        output_path=output_file,
        interval_seconds=0,
        out_of_order_probability=0.0,
    )

    generator.generate_event()

    assert output_file.exists()
    assert output_file.stat().st_size > 0


def test_generator_writes_valid_jsonl(tmp_path: Path):
    import json

    output_file = tmp_path / "events.jsonl"

    generator = EventGeneratorService(
        output_path=output_file,
        interval_seconds=0,
        out_of_order_probability=0.0,
    )

    generator.generate_event()

    lines = output_file.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 1

    event = json.loads(lines[0])

    assert "event_id" in event
    assert "event_type" in event
    assert "event_timestamp" in event
    assert "ingestion_timestamp" in event


def test_generator_creates_multiple_events(tmp_path: Path):
    import json

    output_file = tmp_path / "events.jsonl"

    generator = EventGeneratorService(
        output_path=output_file,
        interval_seconds=0,
        out_of_order_probability=0.0,
    )

    events = [generator.generate_event() for _ in range(5)]

    lines = output_file.read_text(encoding="utf-8").splitlines()

    assert len(events) == 5
    assert len(lines) == 5

    parsed_events = [json.loads(line) for line in lines]

    assert all(event["event_id"] for event in parsed_events)
    assert len({event["event_id"] for event in parsed_events}) == 5