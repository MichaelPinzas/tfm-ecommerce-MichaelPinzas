"""
Transformación Silver → Gold Layer
Crea fact_daily_metrics: métricas diarias agregadas para análisis de negocio
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg,
    when, to_date, dayofweek, lpad, concat, lit
)
from pathlib import Path
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark_session():
    """Crear sesión Spark optimizada para Gold layer"""
    spark = SparkSession.builder \
        .appName("Silver to Gold - Daily Metrics") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_fact_daily_metrics(df_silver):
    """
    Crea fact_daily_metrics a partir de datos Silver

    Args:
        df_silver: DataFrame de Silver Layer

    Returns:
        DataFrame con métricas diarias (62 registros)
    """
    logger.info("\n🔨 Calculando métricas diarias...")

    # Agregaciones diarias
    fact_daily = df_silver.groupBy("year", "month", "day") \
        .agg(
            # Conteos de eventos
            count("*").alias("total_events"),
            countDistinct("user_id").alias("unique_users"),
            countDistinct("user_session").alias("unique_sessions"),
            countDistinct("product_id").alias("unique_products"),

            # Conteos por tipo de evento
            spark_sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
            spark_sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("total_carts"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),

            # Métricas de revenue
            spark_sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("total_revenue"),

            # Precio promedio
            avg("price").alias("avg_price")
        )

    # Crear columna DATE correctamente usando lpad para formato YYYY-MM-DD
    logger.info("\n📅 Creando columna de fecha...")

    fact_daily = fact_daily \
        .withColumn("date", to_date(
            concat(
                col("year").cast("string"),
                lit("-"),
                lpad(col("month").cast("string"), 2, "0"),
                lit("-"),
                lpad(col("day").cast("string"), 2, "0")
            ),
            "yyyy-MM-dd"
        )) \
        .withColumn("day_of_week", dayofweek("date")) \
        .withColumn("is_weekend",
                   (dayofweek("date").isin([1, 7])).cast("boolean"))

    # Calcular métricas derivadas
    logger.info("\n📊 Calculando métricas derivadas...")

    fact_daily = fact_daily \
        .withColumn("conversion_rate",
                   when(col("total_views") > 0,
                        (col("total_purchases") / col("total_views")) * 100)
                   .otherwise(0)) \
        .withColumn("cart_abandonment_rate",
                   when(col("total_carts") > 0,
                        ((col("total_carts") - col("total_purchases")) / col("total_carts")) * 100)
                   .otherwise(0)) \
        .withColumn("avg_order_value",
                   when(col("total_purchases") > 0,
                        col("total_revenue") / col("total_purchases"))
                   .otherwise(0)) \
        .withColumn("events_per_user",
                   when(col("unique_users") > 0,
                        col("total_events") / col("unique_users"))
                   .otherwise(0)) \
        .withColumn("purchases_per_user",
                   when(col("unique_users") > 0,
                        col("total_purchases") / col("unique_users"))
                   .otherwise(0)) \
        .withColumn("cart_to_purchase_rate",
                   when(col("total_carts") > 0,
                        (col("total_purchases") / col("total_carts")) * 100)
                   .otherwise(0))

    # Ordenar columnas y seleccionar
    fact_daily = fact_daily.select(
        # Identificadores temporales
        "date", "year", "month", "day", "day_of_week", "is_weekend",

        # Conteos base
        "total_events", "unique_users", "unique_sessions", "unique_products",

        # Eventos por tipo
        "total_views", "total_carts", "total_purchases",

        # Revenue
        "total_revenue", "avg_price", "avg_order_value",

        # Métricas de conversión
        "conversion_rate", "cart_abandonment_rate", "cart_to_purchase_rate",

        # Engagement
        "events_per_user", "purchases_per_user"
    ).orderBy("date")

    return fact_daily


def main(silver_path, gold_path):
    """Función principal"""
    spark = get_spark_session()

    logger.info("=" * 80)
    logger.info("CREANDO GOLD LAYER - fact_daily_metrics")
    logger.info("=" * 80)

    # 1. LEER SILVER LAYER
    logger.info(f"\n📖 Leyendo Silver Layer desde: {silver_path}")
    df_silver = spark.read.parquet(str(silver_path))

    total_records = df_silver.count()
    logger.info(f"   Total registros Silver: {total_records:,}")

    # 2. CREAR FACT_DAILY_METRICS
    fact_daily = create_fact_daily_metrics(df_silver)

    # 3. MOSTRAR PREVIEW
    logger.info("\n📋 PREVIEW de fact_daily_metrics (primeros 5 días):")
    fact_daily.select(
        "date", "total_events", "unique_users", "total_purchases",
        "conversion_rate", "total_revenue"
    ).show(5, truncate=False)

    # 4. GUARDAR EN GOLD
    output_path = gold_path / "fact_daily_metrics"
    logger.info(f"\n💾 Guardando fact_daily_metrics en: {output_path}")

    fact_daily.coalesce(1).write \
        .mode("overwrite") \
        .parquet(str(output_path))

    # 5. VALIDACIÓN
    logger.info("\n✅ VALIDACIÓN:")
    saved_count = spark.read.parquet(str(output_path)).count()
    logger.info(f"   Registros guardados: {saved_count}")
    logger.info(f"   Esperado: ~62 días (oct-nov 2019)")

    # 6. ESTADÍSTICAS FINALES
    logger.info("\n📊 ESTADÍSTICAS GENERALES:")
    stats = fact_daily.select(
        spark_sum("total_events").alias("total_events"),
        spark_sum("total_purchases").alias("total_purchases"),
        spark_sum("total_revenue").alias("total_revenue"),
        avg("conversion_rate").alias("avg_conversion_rate")
    ).collect()[0]

    logger.info(f"   Total eventos: {stats.total_events:,}")
    logger.info(f"   Total compras: {stats.total_purchases:,}")
    logger.info(f"   Revenue total: ${stats.total_revenue:,.2f}")
    logger.info(f"   Conversión promedio: {stats.avg_conversion_rate:.2f}%")

    logger.info("\n" + "=" * 80)
    logger.info("✅ FACT_DAILY_METRICS CREADO EXITOSAMENTE")
    logger.info("=" * 80)

    spark.stop()


if __name__ == "__main__":
    # Configurar rutas
    project_root = Path(__file__).parent.parent.parent
    silver_path = project_root / "data" / "silver" / "events"
    gold_path = project_root / "data" / "gold"

    # Crear directorio Gold si no existe
    gold_path.mkdir(parents=True, exist_ok=True)

    # Ejecutar transformación
    main(silver_path, gold_path)