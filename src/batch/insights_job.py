from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

logger = logging.getLogger("insights_job")


@dataclass(frozen=True, slots=True)
class InsightsJobConfig:
    application_name: str = "ecommerce-batch-insights"
    input_path: str = "hdfs://namenode:9000/data/raw/events"
    clean_path: str = "hdfs://namenode:9000/data/processed/clean"
    curated_path: str = "hdfs://namenode:9000/data/processed/curated"
    facts_database: str = "ecommerce"
    analytics_database: str = "ecommerce_analytics"
    master: str = "local[*]"
    shuffle_partitions: int = 8
    local_mode: bool = False
    top_n: int = 20


def build_spark_session(config: InsightsJobConfig) -> Any:
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(config.application_name)
        .master(config.master)
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    if not config.local_mode:
        builder = (
            builder.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        )

    spark = builder.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession criada: app=%s master=%s local_mode=%s",
                config.application_name, config.master, config.local_mode)
    return spark


def read_raw_events(spark: Any, config: InsightsJobConfig) -> Any:
    logger.info("Lendo eventos brutos de: %s", config.input_path)

    dataframe = (
        spark.read.option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.json*")
        .json(config.input_path)
    )

    total = dataframe.count()
    if total == 0:
        raise RuntimeError(
            f"Nenhum evento encontrado em '{config.input_path}'. "
            "Confirme com Jamile/Flume o caminho e formato dos arquivos."
        )
    logger.info("Eventos brutos lidos: %s", total)
    return dataframe


def run_etl(spark: Any, raw_df: Any, config: InsightsJobConfig) -> Any:
    from src.batch.etl.clean_to_curated import CleanToCurated
    from src.batch.etl.raw_to_clean import RawToClean

    raw_to_clean = RawToClean()
    clean_df = raw_to_clean.clean(raw_df)

    invalid_count = raw_df.count() - clean_df.count()
    logger.info("ETL raw->clean: %s válidos, %s descartados.",
                clean_df.count(), invalid_count)

    raw_to_clean.write_clean(clean_df, path=config.clean_path, partition_by=["event_type"])

    clean_to_curated = CleanToCurated()
    curated_df = clean_to_curated.curate(clean_df)

    clean_to_curated.write_curated(curated_df, path=config.curated_path, partition_by=["event_date"])
    curated_df.cache()

    logger.info("ETL clean->curated concluído: %s registros.", curated_df.count())
    return curated_df


def split_by_event_type(spark: Any, curated_df: Any) -> Dict[str, Any]:
    from src.batch.transformations import CartTransformations, ClickTransformations
    from src.batch.transformations.delivery_transformations import DeliveryTransformations
    from src.batch.transformations.sales_transformations import SalesTransformations

    return {
        "click": ClickTransformations().transform(curated_df),
        "cart": CartTransformations().transform(curated_df),
        "sales": SalesTransformations().transform(curated_df),
        "delivery": DeliveryTransformations().transform(curated_df),
    }


def build_business_insights(events: Dict[str, Any], config: InsightsJobConfig) -> Dict[str, Any]:
    from pyspark.sql import functions as F
    from src.batch.aggregations.cart_aggregation import CartAggregation
    from src.batch.aggregations.click_aggregation import ClickAggregation
    from src.batch.aggregations.delivery_aggregation import DeliveryAggregation
    from src.batch.aggregations.sales_aggregation import SalesAggregation
    from src.batch.joins.sales_delivery_join import SalesDeliveryJoin

    clicks, carts, sales, deliveries = (
        events["click"], events["cart"], events["sales"], events["delivery"]
    )

    insights: Dict[str, Any] = {}

    insights["top_clicked_products"] = ClickAggregation().aggregate_by_product(clicks).limit(config.top_n)
    insights["top_cart_products"] = CartAggregation().aggregate_by_product(carts).limit(config.top_n)
    insights["orders_by_status"] = SalesAggregation().aggregate_by_status(sales)
    insights["deliveries_by_status"] = DeliveryAggregation().aggregate_by_status(deliveries)
    insights["daily_sales_summary"] = SalesAggregation().aggregate_daily(sales)

    # JOIN distribuído (wide dependency): vendas x entregas
    sales_delivery_join = SalesDeliveryJoin()
    sales_delivery_df = sales_delivery_join.join(sales, deliveries)
    sales_delivery_df.cache()

    insights["sales_delivery_enriched"] = sales_delivery_df
    insights["sales_by_delivery_status"] = sales_delivery_join.create_logistics_sales_summary(sales, deliveries)

    # Funil de conversão click -> carrinho -> pedido por produto
    clicks_by_product = clicks.groupBy("product_id").agg(F.count("*").alias("clicks"))
    carts_by_product = carts.groupBy("product_id").agg(F.count("*").alias("cart_adds"))
    orders_by_product = sales.groupBy("product_id").agg(
        F.count("*").alias("orders"), F.sum("total_amount").alias("revenue")
    )

    conversion_funnel = (
        clicks_by_product.join(carts_by_product, "product_id", "left")
        .join(orders_by_product, "product_id", "left")
        .na.fill(0, subset=["clicks", "cart_adds", "orders"])
        .withColumn("click_to_cart_rate",
                    F.when(F.col("clicks") > 0, F.round(F.col("cart_adds") / F.col("clicks"), 4)).otherwise(F.lit(0.0)))
        .withColumn("cart_to_order_rate",
                    F.when(F.col("cart_adds") > 0, F.round(F.col("orders") / F.col("cart_adds"), 4)).otherwise(F.lit(0.0)))
        .withColumn("overall_conversion_rate",
                    F.when(F.col("clicks") > 0, F.round(F.col("orders") / F.col("clicks"), 4)).otherwise(F.lit(0.0)))
        .orderBy(F.desc("orders"))
    )

    insights["conversion_funnel"] = conversion_funnel
    return insights


def persist_facts(spark: Any, events: Dict[str, Any], config: InsightsJobConfig) -> None:
    from src.batch.hive.hive_loader import HiveLoader, HiveLoaderConfig

    loader = HiveLoader(spark, HiveLoaderConfig(database=config.facts_database))

    table_map = {
        "fact_clicks": events["click"],
        "fact_carts": events["cart"],
        "fact_sales": events["sales"],
        "fact_deliveries": events["delivery"],
    }

    for table_name, dataframe in table_map.items():
        loader.load_overwrite(
            dataframe, table_name,
            partition_by=["event_date"] if "event_date" in dataframe.columns else None,
        )
        logger.info("Tabela Hive de fato atualizada: %s.%s", config.facts_database, table_name)


def persist_insights(spark: Any, insights: Dict[str, Any], config: InsightsJobConfig) -> None:
    from src.batch.hive.hive_loader import HiveLoader, HiveLoaderConfig

    loader = HiveLoader(spark, HiveLoaderConfig(database=config.analytics_database))

    for table_name, dataframe in insights.items():
        loader.load_overwrite(dataframe, table_name)
        logger.info("Indicador persistido no Hive: %s.%s", config.analytics_database, table_name)


def show_results(insights: Dict[str, Any], limit: int = 10) -> None:
    for name, dataframe in insights.items():
        print("\n" + "=" * 78)
        print(f"Indicador: {name}")
        print("=" * 78)
        dataframe.show(limit, truncate=False)


def run(config: InsightsJobConfig, show: bool = True) -> Dict[str, Any]:
    spark = build_spark_session(config)
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{config.facts_database}`")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{config.analytics_database}`")

        raw_df = read_raw_events(spark, config)
        curated_df = run_etl(spark, raw_df, config)
        events = split_by_event_type(spark, curated_df)
        insights = build_business_insights(events, config)

        persist_facts(spark, events, config)
        persist_insights(spark, insights, config)

        if show:
            show_results(insights, limit=config.top_n)

        logger.info("Job Spark concluído com sucesso.")
        return insights
    finally:
        spark.stop()


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Job batch Spark do Data Warehouse de e-commerce.")
    parser.add_argument("--input-path", default=None, help="Caminho HDFS ou local dos eventos brutos.")
    parser.add_argument("--database", default=None, help="Database Hive dos indicadores.")
    parser.add_argument("--local", action="store_true", help="Executa em local[*] com metastore embarcado.")
    parser.add_argument("--show", action="store_true", default=True, help="Imprime os indicadores no console.")
    parser.add_argument("--no-show", dest="show", action="store_false", help="Não imprime os indicadores.")
    parser.add_argument("--top-n", type=int, default=20, help="Quantidade de linhas nos rankings.")
    return parser.parse_args(argv)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    args = parse_args()

    config = InsightsJobConfig()

    if args.local:
        config = InsightsJobConfig(
            master="local[*]",
            local_mode=True,
            input_path=args.input_path or "data/raw",
            clean_path="data/processed/clean",
            curated_path="data/processed/curated",
            top_n=args.top_n,
        )
    else:
        config = InsightsJobConfig(
            input_path=args.input_path or config.input_path,
            top_n=args.top_n,
        )

    if args.database:
        config = InsightsJobConfig(**{**config.__dict__, "analytics_database": args.database})

    run(config, show=args.show)


if __name__ == "__main__":
    main()