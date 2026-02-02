"""
Validación Gold Layer - fact_hourly_metrics
Responde PREGUNTA 4: ¿Cuál es el patrón horario de compras vs navegación?
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum, max as spark_max, min as spark_min
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark_session():
    """Crear sesión Spark"""
    spark = SparkSession.builder \
        .appName("Validate Gold - Hourly Metrics") \
        .config("spark.driver.memory", "3g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def validate_fact_hourly_metrics(gold_path):
    """Validar fact_hourly_metrics y responder Pregunta 4"""
    spark = get_spark_session()
    
    logger.info("=" * 80)
    logger.info("VALIDACIÓN GOLD LAYER - fact_hourly_metrics")
    logger.info("=" * 80)
    
    # Leer Gold Layer
    logger.info(f"\n📖 Leyendo Gold Layer: {gold_path}")
    df_hourly = spark.read.parquet(str(gold_path))
    
    total_hours = df_hourly.count()
    logger.info(f"   Total registros por hora: {total_hours}")
    logger.info(f"   Esperado: ~1,488 registros (24 horas × 62 días)")
    
    # =========================================================================
    # PREGUNTA 4: ¿Cuál es el patrón horario de compras vs navegación?
    # =========================================================================
    logger.info("\n" + "=" * 80)
    logger.info("PREGUNTA 4: ¿Cuál es el patrón horario de compras vs navegación?")
    logger.info("=" * 80)
    
    # Análisis por HORA del día (0-23)
    logger.info("\n📊 ANÁLISIS POR HORA DEL DÍA (0-23):")
    
    hourly_patterns = df_hourly.groupBy("hour").agg(
        spark_sum("total_events").alias("total_events"),
        spark_sum("total_views").alias("total_views"),
        spark_sum("total_purchases").alias("total_purchases"),
        spark_sum("total_revenue").alias("total_revenue"),
        avg("conversion_rate").alias("avg_conversion_rate")
    ).orderBy("hour")
    
    logger.info("\n🕐 DISTRIBUCIÓN HORARIA (0-23h):")
    hourly_patterns.show(24, truncate=False)
    
    # Encontrar picos
    best_hour_traffic = hourly_patterns.orderBy(col("total_events").desc()).first()
    best_hour_conversion = hourly_patterns.orderBy(col("avg_conversion_rate").desc()).first()
    best_hour_revenue = hourly_patterns.orderBy(col("total_revenue").desc()).first()
    
    logger.info(f"\n💡 HORAS PICO:")
    logger.info(f"   🚀 Mayor tráfico: {best_hour_traffic.hour}h con {best_hour_traffic.total_events:,} eventos")
    logger.info(f"   💰 Mayor revenue: {best_hour_revenue.hour}h con ${best_hour_revenue.total_revenue:,.2f}")
    logger.info(f"   📈 Mayor conversión: {best_hour_conversion.hour}h con {best_hour_conversion.avg_conversion_rate:.2f}%")
    
    # Análisis por TIME_OF_DAY
    logger.info("\n" + "=" * 80)
    logger.info("ANÁLISIS POR FRANJA HORARIA (madrugada/mañana/tarde/noche)")
    logger.info("=" * 80)
    
    time_patterns = df_hourly.groupBy("time_of_day").agg(
        spark_sum("total_events").alias("total_events"),
        spark_sum("total_views").alias("total_views"),
        spark_sum("total_purchases").alias("total_purchases"),
        spark_sum("total_revenue").alias("total_revenue"),
        avg("conversion_rate").alias("avg_conversion_rate")
    )
    
    # Calcular porcentajes
    total_events_all = time_patterns.select(spark_sum("total_events")).collect()[0][0]
    total_purchases_all = time_patterns.select(spark_sum("total_purchases")).collect()[0][0]
    
    time_patterns_with_pct = time_patterns.withColumn(
        "pct_events",
        (col("total_events") / total_events_all * 100)
    ).withColumn(
        "pct_purchases",
        (col("total_purchases") / total_purchases_all * 100)
    )
    
    # Ordenar por orden lógico del día
    from pyspark.sql.functions import when
    time_patterns_ordered = time_patterns_with_pct.withColumn(
        "order",
        when(col("time_of_day") == "madrugada", 1)
        .when(col("time_of_day") == "mañana", 2)
        .when(col("time_of_day") == "tarde", 3)
        .otherwise(4)
    ).orderBy("order")
    
    logger.info("\n🌅 PATRONES POR FRANJA HORARIA:")
    time_patterns_ordered.select(
        "time_of_day", "total_events", "pct_events", 
        "total_purchases", "pct_purchases", "avg_conversion_rate", "total_revenue"
    ).show(truncate=False)
    
    # Obtener datos para análisis
    madrugada = time_patterns_ordered.filter(col("time_of_day") == "madrugada").collect()[0]
    manana = time_patterns_ordered.filter(col("time_of_day") == "mañana").collect()[0]
    tarde = time_patterns_ordered.filter(col("time_of_day") == "tarde").collect()[0]
    noche = time_patterns_ordered.filter(col("time_of_day") == "noche").collect()[0]
    
    logger.info(f"\n💡 HALLAZGOS CLAVE:")
    logger.info(f"   🌙 Madrugada (0-6h): {madrugada.pct_events:.1f}% tráfico, {madrugada.pct_purchases:.1f}% compras, {madrugada.avg_conversion_rate:.2f}% conversión")
    logger.info(f"   🌅 Mañana (6-12h): {manana.pct_events:.1f}% tráfico, {manana.pct_purchases:.1f}% compras, {manana.avg_conversion_rate:.2f}% conversión")
    logger.info(f"   ☀️ Tarde (12-18h): {tarde.pct_events:.1f}% tráfico, {tarde.pct_purchases:.1f}% compras, {tarde.avg_conversion_rate:.2f}% conversión")
    logger.info(f"   🌃 Noche (18-24h): {noche.pct_events:.1f}% tráfico, {noche.pct_purchases:.1f}% compras, {noche.avg_conversion_rate:.2f}% conversión")
    
    # Análisis de insights
    logger.info(f"\n🔍 INSIGHTS:")
    
    # ¿Cuándo navegan vs cuándo compran?
    max_traffic_time = max(
        [("madrugada", madrugada.pct_events),
         ("mañana", manana.pct_events),
         ("tarde", tarde.pct_events),
         ("noche", noche.pct_events)],
        key=lambda x: x[1]
    )
    
    max_purchase_time = max(
        [("madrugada", madrugada.pct_purchases),
         ("mañana", manana.pct_purchases),
         ("tarde", tarde.pct_purchases),
         ("noche", noche.pct_purchases)],
        key=lambda x: x[1]
    )
    
    max_conversion_time = max(
        [("madrugada", madrugada.avg_conversion_rate),
         ("mañana", manana.avg_conversion_rate),
         ("tarde", tarde.avg_conversion_rate),
         ("noche", noche.avg_conversion_rate)],
        key=lambda x: x[1]
    )
    
    logger.info(f"   → Mayor NAVEGACIÓN: {max_traffic_time[0]} ({max_traffic_time[1]:.1f}% del tráfico)")
    logger.info(f"   → Mayor COMPRA: {max_purchase_time[0]} ({max_purchase_time[1]:.1f}% de las compras)")
    logger.info(f"   → Mejor CONVERSIÓN: {max_conversion_time[0]} ({max_conversion_time[1]:.2f}%)")
    
    if max_traffic_time[0] != max_purchase_time[0]:
        logger.info(f"   ⚠️ DESCUBRIMIENTO: Usuarios navegan en {max_traffic_time[0]} pero compran más en {max_purchase_time[0]}")
    
    # Análisis fin de semana vs días de semana por hora
    logger.info("\n" + "=" * 80)
    logger.info("ANÁLISIS: FIN DE SEMANA vs DÍAS DE SEMANA POR HORA")
    logger.info("=" * 80)
    
    weekend_hourly = df_hourly.filter(col("is_weekend") == True).groupBy("hour").agg(
        avg("conversion_rate").alias("conversion"),
        spark_sum("total_purchases").alias("purchases")
    ).orderBy("hour")
    
    weekday_hourly = df_hourly.filter(col("is_weekend") == False).groupBy("hour").agg(
        avg("conversion_rate").alias("conversion"),
        spark_sum("total_purchases").alias("purchases")
    ).orderBy("hour")
    
    logger.info("\n📊 TOP 5 HORAS - FIN DE SEMANA:")
    weekend_hourly.orderBy(col("conversion").desc()).show(5, truncate=False)
    
    logger.info("\n📊 TOP 5 HORAS - DÍAS DE SEMANA:")
    weekday_hourly.orderBy(col("conversion").desc()).show(5, truncate=False)
    
    # =========================================================================
    # VALIDACIÓN DE CALIDAD
    # =========================================================================
    logger.info("\n" + "=" * 80)
    logger.info("VALIDACIÓN DE CALIDAD DE DATOS")
    logger.info("=" * 80)
    
    # Verificar rango de horas
    hour_range = df_hourly.select(
        spark_min("hour").alias("min_hour"),
        spark_max("hour").alias("max_hour")
    ).collect()[0]
    
    logger.info(f"\n🕐 Rango de horas:")
    logger.info(f"   Mínimo: {hour_range.min_hour}h")
    logger.info(f"   Máximo: {hour_range.max_hour}h")
    logger.info(f"   Esperado: 0-23h")
    
    # Stats generales
    logger.info("\n📊 ESTADÍSTICAS GENERALES:")
    overall = df_hourly.select(
        avg("conversion_rate").alias("avg_conversion"),
        spark_sum("total_events").alias("total_events"),
        spark_sum("total_purchases").alias("total_purchases"),
        spark_sum("total_revenue").alias("total_revenue")
    ).collect()[0]
    
    logger.info(f"   Conversión promedio: {overall.avg_conversion:.2f}%")
    logger.info(f"   Total eventos: {overall.total_events:,}")
    logger.info(f"   Total compras: {overall.total_purchases:,}")
    logger.info(f"   Revenue total: ${overall.total_revenue:,.2f}")
    
    logger.info("\n" + "=" * 80)
    logger.info("✅ VALIDACIÓN COMPLETADA - PREGUNTA 4 RESPONDIDA")
    logger.info("=" * 80)
    
    spark.stop()


if __name__ == "__main__":
    # Configurar ruta
    project_root = Path(__file__).parent.parent.parent
    gold_path = project_root / "data" / "gold" / "fact_hourly_metrics"
    
    # Ejecutar validación
    validate_fact_hourly_metrics(gold_path)
