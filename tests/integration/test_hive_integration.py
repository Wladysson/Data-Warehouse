from pathlib import Path

from src.storage.hive.hive_client import HiveClient
from src.storage.hive.databases import HiveDatabases
from src.storage.hive.tables import HiveTables


def test_hive_client_initializes():
    client = HiveClient()

    assert client is not None


def test_hive_databases_initializes():
    databases = HiveDatabases()

    assert databases is not None


def test_hive_tables_initializes():
    tables = HiveTables()

    assert tables is not None


def test_hive_configuration_exists():
    project_root = Path(__file__).resolve().parents[2]

    config_dir = project_root / "configs" / "hive"

    assert (config_dir / "hive-site.xml").exists()
    assert (config_dir / "hive-env.sh").exists()


def test_hive_database_ddl_exists():
    project_root = Path(__file__).resolve().parents[2]

    ddl_dir = project_root / "sql" / "hive" / "ddl"

    assert (ddl_dir / "01_create_database.sql").exists()
    assert (ddl_dir / "02_create_dimensions.sql").exists()
    assert (ddl_dir / "03_create_facts.sql").exists()
    assert (ddl_dir / "04_create_external_tables.sql").exists()
    assert (ddl_dir / "05_create_views.sql").exists()


def test_hive_dml_scripts_exist():
    project_root = Path(__file__).resolve().parents[2]

    dml_dir = project_root / "sql" / "hive" / "dml"

    assert (dml_dir / "load_daily_partitions.sql").exists()
    assert (dml_dir / "refresh_views.sql").exists()
    assert (dml_dir / "backfill.sql").exists()


def test_hive_ddl_contains_data_warehouse_tables():
    project_root = Path(__file__).resolve().parents[2]

    facts_file = (
        project_root
        / "sql"
        / "hive"
        / "ddl"
        / "03_create_facts.sql"
    )

    content = facts_file.read_text(encoding="utf-8")

    assert "fact_sales" in content
    assert "fact_clicks" in content
    assert "fact_carts" in content
    assert "fact_deliveries" in content


def test_hive_views_file_contains_analytical_views():
    project_root = Path(__file__).resolve().parents[2]

    views_file = (
        project_root
        / "sql"
        / "hive"
        / "ddl"
        / "05_create_views.sql"
    )

    content = views_file.read_text(encoding="utf-8")

    assert "vw_sales_daily" in content
    assert "vw_sales_by_product" in content
    assert "vw_customer_sales" in content
    assert "vw_delivery_performance" in content