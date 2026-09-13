from pathlib import Path

from src.batch.spark_session import SparkSessionFactory
from src.batch.spark_job import SparkBatchJob


def test_spark_session_factory_initializes():
    factory = SparkSessionFactory()

    assert factory is not None


def test_spark_batch_job_initializes():
    job = SparkBatchJob()

    assert job is not None


def test_spark_configuration_exists():
    project_root = Path(__file__).resolve().parents[2]

    config_dir = project_root / "configs" / "spark"

    assert (config_dir / "spark-defaults.conf").exists()
    assert (config_dir / "hive-site.xml").exists()
    assert (config_dir / "log4j.properties").exists()


def test_spark_configuration_contains_hdfs_support():
    project_root = Path(__file__).resolve().parents[2]

    config_file = project_root / "configs" / "spark" / "spark-defaults.conf"
    content = config_file.read_text(encoding="utf-8")

    assert "hdfs" in content.lower()


def test_spark_configuration_contains_shuffle_settings():
    project_root = Path(__file__).resolve().parents[2]

    config_file = project_root / "configs" / "spark" / "spark-defaults.conf"
    content = config_file.read_text(encoding="utf-8")

    assert "shuffle" in content.lower()


def test_spark_job_exposes_execution_lifecycle():
    job = SparkBatchJob()

    assert hasattr(job, "run")
    assert hasattr(job, "healthcheck")


def test_spark_etl_modules_exist():
    project_root = Path(__file__).resolve().parents[2]

    etl_dir = project_root / "src" / "batch" / "etl"

    assert (etl_dir / "raw_to_clean.py").exists()
    assert (etl_dir / "clean_to_curated.py").exists()
    assert (etl_dir / "daily_aggregations.py").exists()
    assert (etl_dir / "feature_engineering.py").exists()