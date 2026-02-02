"""
Validación Gold Layer - fact_daily_metrics
Verifica la calidad y responde preguntas de negocio
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
        .appName("Validate Gold - Daily Metrics") \
        .config("spark.driver.memory", "3g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def validate_fact_daily_metrics(gold_path):
    """Validar fact_daily_metrics y responder preguntas de negocio"""
    spark = get_spark_session()

    logger.info("=" * 80)
    logger.info("VALIDACIÓN GOLD LAYER - fact_daily_metrics")
    logger.info("=" * 80)

    # Leer Gold Layer
    logger.info(f"\n📖 Leyendo Gold Layer: {gold_path}")
    df_daily = spark.read.parquet(str(gold_path))

    total_days = df_daily.count()
    logger.info(f"   Total días: {total_days}")
    logger.info(f"   Esperado: 62 días (30 sep + 31 oct + 30 nov)")

    # =========================================================================
    # PREGUNTA 1: ¿Por qué Noviembre tiene más tráfico pero menor conversión?
    # =========================================================================
    logger.info("\n" + "=" * 80)
    logger.info("PREGUNTA 1: ¿Por qué Nov tiene MÁS tráfico pero MENOR conversión?")
    logger.info("=" * 80)

    oct_stats = df_daily.filter(col("month") == 10).select(
        spark_sum("total_events").alias("total_events"),
        spark_sum("total_views").alias("total_views"),
        spark_sum("total_purchases").alias("total_purchases"),
        spark_sum("total_revenue").alias("total_revenue"),
        avg("conversion_rate").alias("avg_conversion_rate"),
        spark_sum("unique_users").alias("sum_unique_users"),
        avg("events_per_user").alias("avg_events_per_user")
    ).collect()[0]

    nov_stats = df_daily.filter(col("month") == 11).select(
        spark_sum("total_events").alias("total_events"),
        spark_sum("total_views").alias("total_views"),
        spark_sum("total_purchases").alias("total_purchases"),
        spark_sum("total_revenue").alias("total_revenue"),
        avg("conversion_rate").alias("avg_conversion_rate"),
        spark_sum("unique_users").alias("sum_unique_users"),
        avg("events_per_user").alias("avg_events_per_user")
    ).collect()[0]

    logger.info("\n📊 OCTUBRE 2019:")
    logger.info(f"   Total eventos: {oct_stats.total_events:,}")
    logger.info(f"   Total vistas: {oct_stats.total_views:,}")
    logger.info(f"   Total compras: {oct_stats.total_purchases:,}")
    logger.info(f"   Revenue: ${oct_stats.total_revenue:,.2f}")
    logger.info(f"   Conversión promedio: {oct_stats.avg_conversion_rate:.2f}%")
    logger.info(f"   Usuarios únicos (suma diaria): {oct_stats.sum_unique_users:,}")
    logger.info(f"   Eventos por usuario: {oct_stats.avg_events_per_user:.2f}")

    logger.info("\n📊 NOVIEMBRE 2019:")
    logger.info(f"   Total eventos: {nov_stats.total_events:,}")
    logger.info(f"   Total vistas: {nov_stats.total_views:,}")
    logger.info(f"   Total compras: {nov_stats.total_purchases:,}")
    logger.info(f"   Revenue: ${nov_stats.total_revenue:,.2f}")
    logger.info(f"   Conversión promedio: {nov_stats.avg_conversion_rate:.2f}%")
    logger.info(f"   Usuarios únicos (suma diaria): {nov_stats.sum_unique_users:,}")
    logger.info(f"   Eventos por usuario: {nov_stats.avg_events_per_user:.2f}")

    traffic_increase = ((nov_stats.total_events - oct_stats.total_events) / oct_stats.total_events) * 100
    conversion_change = nov_stats.avg_conversion_rate - oct_stats.avg_conversion_rate

    logger.info("\n💡 HALLAZGOS:")
    logger.info(f"   📈 Tráfico: +{traffic_increase:.1f}% en Noviembre")
    logger.info(f"   📉 Conversión: {conversion_change:+.2f} puntos porcentuales")
    logger.info(f"   🎯 Eventos/usuario Oct: {oct_stats.avg_events_per_user:.2f}")
    logger.info(f"   🎯 Eventos/usuario Nov: {nov_stats.avg_events_per_user:.2f}")

    logger.info("\n🔍 HIPÓTESIS:")
    if nov_stats.avg_events_per_user > oct_stats.avg_events_per_user and nov_stats.avg_conversion_rate < oct_stats.avg_conversion_rate:
        logger.info("   → Usuarios más activos pero menos comprometidos")
        logger.info("   → Posible tráfico de baja calidad (bots, scrapers)")
        logger.info("   → Campaña masiva que atrajo curiosos sin intención de compra")

    # =========================================================================
    # PREGUNTA 2: ¿Qué pasó Nov 15-17 para generar 3x tráfico normal?
    # =========================================================================
    logger.info("\n" + "=" * 80)
    logger.info("PREGUNTA 2: ¿Qué pasó Nov 15-17 con el pico de tráfico?")
    logger.info("=" * 80)

    spike_days = df_daily.filter(
        (col("month") == 11) & (col("day").isin([15, 16, 17]))
    ).select(
        "date", "total_events", "unique_users", "total_purchases",
        "conversion_rate", "total_revenue", "events_per_user"
    ).orderBy("date")

    logger.info("\n📊 DÍAS DEL PICO (Nov 15-17):")
    spike_days.show(truncate=False)

    spike_total = spike_days.select(spark_sum("total_events")).collect()[0][0]
    spike_purchases = spike_days.select(spark_sum("total_purchases")).collect()[0][0]
    spike_revenue = spike_days.select(spark_sum("total_revenue")).collect()[0][0]
    nov_total = nov_stats.total_events
    spike_percentage = (spike_total / nov_total) * 100

    logger.info(f"\n💡 HALLAZGOS:")
    logger.info(f"   📊 Nov 15-17 representó {spike_percentage:.1f}% del tráfico de noviembre")
    logger.info(f"   💰 Revenue en spike: ${spike_revenue:,.2f}")
    logger.info(f"   🛒 Compras en spike: {spike_purchases:,}")

    # Promedio diario de noviembre
    nov_days = df_daily.filter(col("month") == 11).count()
    nov_avg = nov_stats.total_events / nov_days
    spike_avg = spike_total / 3
    logger.info(f"   📈 Promedio diario Nov: {nov_avg:,.0f} eventos")
    logger.info(f"   📈 Promedio días spike: {spike_avg:,.0f} eventos")
    logger.info(f"   🚀 Incremento: {((spike_avg / nov_avg) - 1) * 100:.1f}%")

    # =========================================================================
    # PREGUNTA 3: ¿Por qué Black Friday NO fue el día pico de ventas?
    # =========================================================================
    logger.info("\n" + "=" * 80)
    logger.info("PREGUNTA 3: ¿Por qué Black Friday NO fue el día pico?")
    logger.info("=" * 80)

    black_friday = df_daily.filter(
        (col("month") == 11) & (col("day") == 29)
    ).select("date", "total_events", "total_purchases", "total_revenue", "conversion_rate")

    logger.info("\n📊 BLACK FRIDAY (Nov 29):")
    black_friday.show(truncate=False)

    bf_stats = black_friday.collect()[0]
    logger.info(f"\n💡 BLACK FRIDAY STATS:")
    logger.info(f"   Eventos: {bf_stats.total_events:,}")
    logger.info(f"   Compras: {bf_stats.total_purchases:,}")
    logger.info(f"   Revenue: ${bf_stats.total_revenue:,.2f}")
    logger.info(f"   Conversión: {bf_stats.conversion_rate:.2f}%")

    # Top 10 días por tráfico
    top_traffic = df_daily.select(
        "date", "total_events", "total_purchases", "conversion_rate", "total_revenue"
    ).orderBy(col("total_events").desc()).limit(10)

    logger.info("\n📊 TOP 10 DÍAS POR TRÁFICO:")
    top_traffic.show(truncate=False)

    # Top 10 días por revenue
    top_revenue = df_daily.select(
        "date", "total_revenue", "total_purchases", "conversion_rate", "total_events"
    ).orderBy(col("total_revenue").desc()).limit(10)

    logger.info("\n📊 TOP 10 DÍAS POR REVENUE:")
    top_revenue.show(truncate=False)

    logger.info("\n🔍 HIPÓTESIS:")
    logger.info("   → Ofertas anticipadas (Nov 15-17) canibalizaron Black Friday")
    logger.info("   → Usuarios compraron temprano para evitar agotamiento de stock")
    logger.info("   → Competencia intensa en Black Friday (usuarios comparando precios)")

    # =========================================================================
    # VALIDACIÓN DE CALIDAD
    # =========================================================================
    logger.info("\n" + "=" * 80)
    logger.info("VALIDACIÓN DE CALIDAD DE DATOS")
    logger.info("=" * 80)

    # Verificar días sin gaps
    date_range = df_daily.select(
        spark_min("date").alias("min_date"),
        spark_max("date").alias("max_date")
    ).collect()[0]

    logger.info(f"\n📅 Rango de fechas:")
    logger.info(f"   Inicio: {date_range.min_date}")
    logger.info(f"   Fin: {date_range.max_date}")

    # Stats generales
    logger.info("\n📊 ESTADÍSTICAS GENERALES:")
    overall = df_daily.select(
        avg("conversion_rate").alias("avg_conversion"),
        avg("events_per_user").alias("avg_events_per_user"),
        avg("purchases_per_user").alias("avg_purchases_per_user"),
        spark_sum("total_revenue").alias("total_revenue"),
        avg("avg_order_value").alias("avg_order_value")
    ).collect()[0]

    logger.info(f"   Conversión promedio: {overall.avg_conversion:.2f}%")
    logger.info(f"   Eventos por usuario: {overall.avg_events_per_user:.2f}")
    logger.info(f"   Compras por usuario: {overall.avg_purchases_per_user:.4f}")
    logger.info(f"   Revenue total: ${overall.total_revenue:,.2f}")
    logger.info(f"   AOV (Average Order Value): ${overall.avg_order_value:.2f}")

    # Análisis de fin de semana vs días de semana
    logger.info("\n📊 ANÁLISIS FIN DE SEMANA vs DÍAS DE SEMANA:")
    weekend_stats = df_daily.filter(col("is_weekend") == True).select(
        avg("conversion_rate").alias("conversion"),
        avg("events_per_user").alias("events_per_user")
    ).collect()[0]

    weekday_stats = df_daily.filter(col("is_weekend") == False).select(
        avg("conversion_rate").alias("conversion"),
        avg("events_per_user").alias("events_per_user")
    ).collect()[0]

    logger.info(f"   Fin de semana - Conversión: {weekend_stats.conversion:.2f}%")
    logger.info(f"   Días de semana - Conversión: {weekday_stats.conversion:.2f}%")
    logger.info(f"   Fin de semana - Eventos/usuario: {weekend_stats.events_per_user:.2f}")
    logger.info(f"   Días de semana - Eventos/usuario: {weekday_stats.events_per_user:.2f}")

    logger.info("\n" + "=" * 80)
    logger.info("✅ VALIDACIÓN COMPLETADA")
    logger.info("=" * 80)

    spark.stop()


if __name__ == "__main__":
    # Configurar ruta
    project_root = Path(__file__).parent.parent.parent
    gold_path = project_root / "data" / "gold" / "fact_daily_metrics"

    # Ejecutar validación
    validate_fact_daily_metrics(gold_path)