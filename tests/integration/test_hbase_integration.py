from pathlib import Path

from src.storage.hbase.hbase_client import HBaseClient
from src.storage.hbase.tables import HBaseTables


def test_hbase_client_initializes():
    client = HBaseClient()

    assert client is not None


def test_hbase_tables_initializes():
    tables = HBaseTables()

    assert tables is not None


def test_hbase_configuration_exists():
    project_root = Path(__file__).resolve().parents[2]

    config_dir = project_root / "configs" / "hbase"

    assert (config_dir / "hbase-site.xml").exists()
    assert (config_dir / "hbase-env.sh").exists()


def test_hbase_schema_file_exists():
    project_root = Path(__file__).resolve().parents[2]

    schema_file = project_root / "sql" / "hbase" / "schema.hbase"

    assert schema_file.exists()
    assert schema_file.is_file()


def test_hbase_table_creation_script_exists():
    project_root = Path(__file__).resolve().parents[2]

    script = project_root / "sql" / "hbase" / "create_tables.hbase"

    assert script.exists()
    assert script.is_file()


def test_hbase_seed_data_exists():
    project_root = Path(__file__).resolve().parents[2]

    seed_file = project_root / "sql" / "hbase" / "seed_data.hbase"

    assert seed_file.exists()
    assert seed_file.is_file()


def test_hbase_schema_contains_streaming_tables():
    project_root = Path(__file__).resolve().parents[2]

    schema_file = project_root / "sql" / "hbase" / "schema.hbase"
    content = schema_file.read_text(encoding="utf-8")

    assert "streaming_metrics" in content
    assert "streaming_alerts" in content
    assert "event_realtime" in content