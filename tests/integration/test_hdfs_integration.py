from pathlib import Path

from src.storage.hdfs.hdfs_client import HDFSClient
from src.storage.hdfs.paths import HDFSPaths


def test_hdfs_client_initializes():
    client = HDFSClient()

    assert client is not None


def test_hdfs_paths_exposes_raw_and_processed_locations():
    paths = HDFSPaths()

    assert paths is not None
    assert hasattr(paths, "raw")
    assert hasattr(paths, "clean")
    assert hasattr(paths, "curated")


def test_hdfs_configuration_files_exist():
    project_root = Path(__file__).resolve().parents[2]

    hadoop_config = project_root / "configs" / "hadoop"

    assert (hadoop_config / "core-site.xml").exists()
    assert (hadoop_config / "hdfs-site.xml").exists()
    assert (hadoop_config / "mapred-site.xml").exists()
    assert (hadoop_config / "yarn-site.xml").exists()


def test_hdfs_raw_data_directories_exist():
    project_root = Path(__file__).resolve().parents[2]

    raw_dir = project_root / "data" / "raw"

    assert raw_dir.exists()
    assert (raw_dir / "clicks").exists()
    assert (raw_dir / "carts").exists()
    assert (raw_dir / "orders").exists()
    assert (raw_dir / "deliveries").exists()


def test_hdfs_processed_directories_exist():
    project_root = Path(__file__).resolve().parents[2]

    processed_dir = project_root / "data" / "processed"

    assert processed_dir.exists()
    assert (processed_dir / "clean").exists()
    assert (processed_dir / "curated").exists()


def test_hdfs_client_handles_sample_path():
    client = HDFSClient(
        base_path="/data/raw",
    )

    assert client.base_path == "/data/raw"