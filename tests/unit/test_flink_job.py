from src.streaming.flink_job import FlinkStreamingJob


def test_flink_job_creates_default_pipeline():
    job = FlinkStreamingJob()

    pipeline = job.build_pipeline()

    assert pipeline is not None


def test_flink_job_pipeline_contains_streaming_configuration():
    job = FlinkStreamingJob()

    pipeline = job.build_pipeline()

    assert "source" in pipeline
    assert "watermark" in pipeline
    assert "window" in pipeline
    assert "sinks" in pipeline


def test_flink_job_pipeline_configures_sliding_window():
    job = FlinkStreamingJob()

    pipeline = job.build_pipeline()

    window = pipeline["window"]

    assert window["type"] == "sliding"
    assert window["size_seconds"] > 0
    assert window["slide_seconds"] > 0
    assert window["slide_seconds"] <= window["size_seconds"]


def test_flink_job_pipeline_configures_watermarks():
    job = FlinkStreamingJob()

    pipeline = job.build_pipeline()

    watermark = pipeline["watermark"]

    assert watermark["enabled"] is True
    assert watermark["max_out_of_orderness_seconds"] >= 0


def test_flink_job_pipeline_has_streaming_sinks():
    job = FlinkStreamingJob()

    pipeline = job.build_pipeline()

    sinks = pipeline["sinks"]

    assert "hbase" in sinks
    assert "hdfs" in sinks