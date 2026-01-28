"""
Validación completa de Silver Layer
Octubre + Noviembre 2019 (2 meses completos)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, min, max, avg, sum as spark_sum
from pathlib import Path
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark_session():
    """Crear sesión Spark"""
    spark = SparkSession.builder \
        .appName("Validación Silver - Oct + Nov 2019") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def validate_silver_layer(silver_path):
    """
    Valida la calidad y completitud de los datos en Silver
    para octubre y noviembre 2019
    """
    spark = get_spark_session()

    logger.info("=" * 80)
    logger.info("VALIDACIÓN COMPLETA - SILVER LAYER")
    logger.info("Octubre + Noviembre 2019 (2 meses)")
    logger.info("=" * 80)

    # Leer todos los datos de Silver
    df = spark.read.parquet(str(silver_path))

    # 1. CONTEO TOTAL DE REGISTROS
    total_records = df.count()
    logger.info(f"\n📊 TOTAL REGISTROS: {total_records:,}")

    # Registros por mes
    records_by_month = df.groupBy("year", "month") \
        .agg(count("*").alias("records")) \
        .orderBy("year", "month") \
        .collect()

    logger.info("\n📅 REGISTROS POR MES:")
    for row in records_by_month:
        logger.info(f"  {row.year}-{row.month:02d}: {row.records:,} registros")

    # 2. VALIDACIÓN DE ESQUEMA
    logger.info("\n🔍 VALIDACIÓN DE ESQUEMA:")
    logger.info(f"  Columnas totales: {len(df.columns)}")
    expected_columns = [
        'event_time', 'event_type', 'product_id', 'category_id', 'category_code',
        'brand', 'price', 'user_id', 'user_session',
        'hour', 'day_of_week', 'is_weekend', 'time_of_day',
        'category_l1', 'category_l2', 'category_l3',
        'conversion_stage',
        'year', 'month', 'day'
    ]

    missing_cols = set(expected_columns) - set(df.columns)
    if missing_cols:
        logger.warning(f"  ⚠️  Columnas faltantes: {missing_cols}")
    else:
        logger.info(f"  ✅ Todas las columnas esperadas presentes ({len(expected_columns)})")

    # 3. VALIDACIÓN DE NULOS EN CAMPOS CRÍTICOS
    logger.info("\n🚫 VALIDACIÓN DE NULOS (campos críticos):")
    critical_fields = ['event_time', 'event_type', 'product_id', 'price', 'user_id']

    for field in critical_fields:
        null_count = df.filter(col(field).isNull()).count()
        logger.info(f"  {field}: {null_count} nulls")
        if null_count > 0:
            logger.warning(f"    ⚠️  Campo crítico con nulls!")

    # 4. DISTRIBUCIÓN DE EVENTOS
    logger.info("\n📈 DISTRIBUCIÓN DE TIPOS DE EVENTO:")
    event_dist = df.groupBy("event_type") \
        .agg(count("*").alias("count")) \
        .orderBy(col("count").desc()) \
        .collect()

    for row in event_dist:
        pct = (row['count'] / total_records) * 100
        logger.info(f"  {row.event_type}: {row['count']:,} ({pct:.2f}%)")

    # 5. FUNNEL DE CONVERSIÓN
    logger.info("\n🎯 FUNNEL DE CONVERSIÓN:")
    conversion_dist = df.groupBy("conversion_stage") \
        .agg(count("*").alias("count")) \
        .orderBy(col("count").desc()) \
        .collect()

    for row in conversion_dist:
        pct = (row['count'] / total_records) * 100
        logger.info(f"  {row.conversion_stage}: {row['count']:,} ({pct:.2f}%)")

    # 6. ESTADÍSTICAS DE USUARIOS Y PRODUCTOS
    logger.info("\n👥 ESTADÍSTICAS DE USUARIOS Y PRODUCTOS:")
    unique_users = df.select("user_id").distinct().count()
    unique_products = df.select("product_id").distinct().count()
    unique_brands = df.select("brand").distinct().count()

    logger.info(f"  Usuarios únicos: {unique_users:,}")
    logger.info(f"  Productos únicos: {unique_products:,}")
    logger.info(f"  Marcas únicas: {unique_brands:,}")

    # 7. ESTADÍSTICAS DE PRECIOS
    logger.info("\n💰 ESTADÍSTICAS DE PRECIOS:")
    price_stats = df.select(
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        avg("price").alias("avg_price")
    ).collect()[0]

    logger.info(f"  Precio mínimo: ${price_stats.min_price:.2f}")
    logger.info(f"  Precio máximo: ${price_stats.max_price:.2f}")
    logger.info(f"  Precio promedio: ${price_stats.avg_price:.2f}")

    # 8. DISTRIBUCIÓN TEMPORAL
    logger.info("\n⏰ DISTRIBUCIÓN TEMPORAL:")
    time_dist = df.groupBy("time_of_day") \
        .agg(count("*").alias("count")) \
        .orderBy(col("count").desc()) \
        .collect()

    for row in time_dist:
        pct = (row['count'] / total_records) * 100
        logger.info(f"  {row.time_of_day}: {row['count']:,} ({pct:.2f}%)")

    # 9. TOP CATEGORÍAS
    logger.info("\n🏷️  TOP 10 CATEGORÍAS (NIVEL 1):")
    top_cats = df.groupBy("category_l1") \
        .agg(count("*").alias("count")) \
        .orderBy(col("count").desc()) \
        .limit(10) \
        .collect()

    for i, row in enumerate(top_cats, 1):
        pct = (row['count'] / total_records) * 100
        logger.info(f"  {i}. {row.category_l1}: {row['count']:,} ({pct:.2f}%)")

    # 10. ANÁLISIS POR DÍA - IDENTIFICAR PICOS
    logger.info("\n📊 TOP 10 DÍAS CON MÁS TRÁFICO:")
    daily_traffic = df.groupBy("year", "month", "day") \
        .agg(count("*").alias("records")) \
        .orderBy(col("records").desc()) \
        .limit(10) \
        .collect()

    for i, row in enumerate(daily_traffic, 1):
        date_str = f"{row.year}-{row.month:02d}-{row.day:02d}"
        logger.info(f"  {i}. {date_str}: {row.records:,} registros")

    # 11. COMPARACIÓN OCTUBRE VS NOVIEMBRE
    logger.info("\n🔄 COMPARACIÓN OCTUBRE VS NOVIEMBRE:")
    oct_data = df.filter((col("year") == 2019) & (col("month") == 10))
    nov_data = df.filter((col("year") == 2019) & (col("month") == 11))

    oct_count = oct_data.count()
    nov_count = nov_data.count()

    logger.info(f"  Octubre 2019: {oct_count:,} registros")
    logger.info(f"  Noviembre 2019: {nov_count:,} registros")

    if nov_count > oct_count:
        diff_pct = ((nov_count - oct_count) / oct_count) * 100
        logger.info(f"  📈 Noviembre tiene {diff_pct:.2f}% MÁS tráfico que octubre")
    else:
        diff_pct = ((oct_count - nov_count) / oct_count) * 100
        logger.info(f"  📉 Noviembre tiene {diff_pct:.2f}% MENOS tráfico que octubre")

    # Conversión por mes
    logger.info("\n  Tasa de conversión por mes:")
    for month_name, month_df in [("Octubre", oct_data), ("Noviembre", nov_data)]:
        purchases = month_df.filter(col("event_type") == "purchase").count()
        total = month_df.count()
        conversion_rate = (purchases / total) * 100
        logger.info(f"    {month_name}: {conversion_rate:.2f}%")

    # 12. VALIDACIÓN DE PARTICIONES
    logger.info("\n📁 VALIDACIÓN DE PARTICIONES:")
    partitions = df.select("year", "month", "day").distinct().count()
    logger.info(f"  Total de particiones (días): {partitions}")
    logger.info(f"  Esperado: 61 días (31 oct + 30 nov)")

    if partitions == 61:
        logger.info("  ✅ Todos los días presentes")
    else:
        logger.warning(f"  ⚠️  Faltan {61 - partitions} días")

    logger.info("\n" + "=" * 80)
    logger.info("✅ VALIDACIÓN COMPLETADA")
    logger.info("=" * 80)

    spark.stop()


if __name__ == "__main__":
    # Configurar ruta
    project_root = Path(__file__).parent.parent.parent
    silver_path = project_root / "data" / "silver" / "events"

    # Ejecutar validación
    validate_silver_layer(silver_path)