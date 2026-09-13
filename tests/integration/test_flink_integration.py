from pathlib import Path

from src.streaming.flink_job import FlinkStreamingJob


def test_flink_job_initializes():
    job = FlinkStreamingJob()

    assert job is not None


def test_flink_configuration_exists():
    project_root = Path(__file__).resolve().parents[2]

    config_file = project_root / "configs" / "flink" / "flink-conf.yaml"

    assert config_file.exists()
    assert config_file.is_file()


def test_flink_configuration_enables_event_time():
    project_root = Path(__file__).resolve().parents[2]

    config_file = project_root / "configs" / "flink" / "flink-conf.yaml"
    content = config_file.read_text(encoding="utf-8")

    assert "checkpoint" in content.lower()
    assert "watermark" in content.lower() or "event" in content.lower()


def test_flink_pipeline_contains_required_components():
    job = FlinkStreamingJob()

    pipeline = job.build_pipeline()

    assert "source" in pipeline
    assert "watermark" in pipeline
    assert "window" in pipeline
    assert "sinks" in pipeline


def test_flink_pipeline_uses_sliding_window():
    job = FlinkStreamingJob()

    pipeline = job.build_pipeline()

    assert pipeline["window"]["type"] == "sliding"
    assert pipeline["window"]["size_seconds"] > 0
    assert pipeline["window"]["slide_seconds"] > 0


def test_flink_pipeline_has_hbase_and_hdfs_sinks():
    job = FlinkStreamingJob()

    pipeline = job.build_pipeline()

    assert "hbase" in pipeline["sinks"]
    assert "hdfs" in pipeline["sinks"]