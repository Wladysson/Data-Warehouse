from src.storage.hdfs.hdfs_client import HDFSClient
from src.storage.hbase.hbase_client import HBaseClient
from src.storage.hive.hive_client import HiveClient


def test_hdfs_client_initializes():
    client = HDFSClient()

    assert client is not None


def test_hbase_client_initializes():
    client = HBaseClient()

    assert client is not None


def test_hive_client_initializes():
    client = HiveClient()

    assert client is not None


def test_hdfs_client_exposes_file_operations():
    client = HDFSClient()

    assert hasattr(client, "exists")
    assert hasattr(client, "mkdir")
    assert hasattr(client, "delete")


def test_hbase_client_exposes_table_operations():
    client = HBaseClient()

    assert hasattr(client, "create_table")
    assert hasattr(client, "delete_table")
    assert hasattr(client, "table_exists")


def test_hive_client_exposes_database_operations():
    client = HiveClient()

    assert hasattr(client, "database_exists")
    assert hasattr(client, "create_database")
    assert hasattr(client, "drop_database")


def test_hdfs_client_handles_local_path_configuration():
    client = HDFSClient(
        base_path="/data/raw",
    )

    assert client.base_path == "/data/raw"


def test_hbase_client_handles_table_configuration():
    client = HBaseClient(
        table_prefix="ecommerce",
    )

    assert client.table_prefix == "ecommerce"


def test_hive_client_handles_database_configuration():
    client = HiveClient(
        database="ecommerce",
    )

    assert client.database == "ecommerce"