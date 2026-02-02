"""
Validación Gold Layer - fact_category_metrics
Responde PREGUNTA 5: ¿Qué categorías tienen mejor conversión?
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark_session():
    """Crear sesión Spark"""
    spark = SparkSession.builder \
        .appName("Validate Gold - Category Metrics") \
        .config("spark.driver.memory", "3g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def validate_fact_category_metrics(gold_path):
    """Validar fact_category_metrics y responder Pregunta 5"""
    spark = get_spark_session()
    
    logger.info("=" * 80)
    logger.info("VALIDACIÓN GOLD LAYER - fact_category_metrics")
    logger.info("=" * 80)
    
    # Leer Gold Layer
    logger.info(f"\n📖 Leyendo Gold Layer: {gold_path}")
    df_category = spark.read.parquet(str(gold_path))
    
    total_categories = df_category.count()
    logger.info(f"   Total categorías: {total_categories}")
    
    # =========================================================================
    # PREGUNTA 5: ¿Qué categorías tienen mejor conversión?
    # =========================================================================
    logger.info("\n" + "=" * 80)
    logger.info("PREGUNTA 5: ¿Qué categorías tienen mejor conversión?")
    logger.info("=" * 80)
    
    # Mostrar todas las categorías ordenadas por diferentes métricas
    logger.info("\n📊 TODAS LAS CATEGORÍAS - Vista completa:")
    df_category.select(
        "category_l1", "total_purchases", "total_revenue",
        "conversion_rate", "avg_order_value", "cart_abandonment_rate"
    ).show(100, truncate=False)
    
    # =========================================================================
    # TOP CATEGORÍAS POR CONVERSIÓN
    # =========================================================================
    logger.info("\n" + "=" * 80)
    logger.info("TOP CATEGORÍAS POR CONVERSIÓN")
    logger.info("=" * 80)
    
    top_conversion = df_category.orderBy(col("conversion_rate").desc()).limit(10)
    
    logger.info("\n🏆 TOP 10 CATEGORÍAS POR TASA DE CONVERSIÓN:")
    top_conversion.select(
        "category_l1", "conversion_rate", "total_views", "total_purchases",
        "total_revenue", "avg_order_value"
    ).show(10, truncate=False)
    
    # Analizar top 3
    top_3_conv = top_conversion.collect()[:3]
    logger.info(f"\n💡 TOP 3 CONVERSIÓN:")
    for i, cat in enumerate(top_3_conv, 1):
        logger.info(f"   {i}. {cat.category_l1}: {cat.conversion_rate:.2f}%")
        logger.info(f"      - Vistas: {cat.total_views:,}")
        logger.info(f"      - Compras: {cat.total_purchases:,}")
        logger.info(f"      - AOV: ${cat.avg_order_value:.2f}")
    
    # =========================================================================
    # TOP CATEGORÍAS POR REVENUE
    # =========================================================================
    logger.info("\n" + "=" * 80)
    logger.info("TOP CATEGORÍAS POR REVENUE")
    logger.info("=" * 80)
    
    top_revenue = df_category.orderBy(col("total_revenue").desc()).limit(10)
    
    logger.info("\n💰 TOP 10 CATEGORÍAS POR REVENUE:")
    top_revenue.select(
        "category_l1", "total_revenue", "pct_revenue", "total_purchases",
        "avg_order_value", "conversion_rate"
    ).show(10, truncate=False)
    
    # Analizar top 3 revenue
    top_3_rev = top_revenue.collect()[:3]
    logger.info(f"\n💡 TOP 3 REVENUE:")
    for i, cat in enumerate(top_3_rev, 1):
        logger.info(f"   {i}. {cat.category_l1}: ${cat.total_revenue:,.2f} ({cat.pct_revenue:.1f}% del total)")
        logger.info(f"      - Compras: {cat.total_purchases:,}")
        logger.info(f"      - AOV: ${cat.avg_order_value:.2f}")
        logger.info(f"      - Conversión: {cat.conversion_rate:.2f}%")
    
    # =========================================================================
    # ANÁLISIS DE ABANDONO DE CARRITO
    # =========================================================================
    logger.info("\n" + "=" * 80)
    logger.info("ANÁLISIS DE ABANDONO DE CARRITO POR CATEGORÍA")
    logger.info("=" * 80)
    
    # Categorías con mayor abandono
    top_abandonment = df_category.orderBy(col("cart_abandonment_rate").desc()).limit(10)
    
    logger.info("\n🛒 TOP 10 CATEGORÍAS CON MAYOR ABANDONO DE CARRITO:")
    top_abandonment.select(
        "category_l1", "cart_abandonment_rate", "total_carts",
        "total_purchases", "conversion_rate"
    ).show(10, truncate=False)
    
    # Categorías con menor abandono
    bottom_abandonment = df_category.orderBy(col("cart_abandonment_rate").asc()).limit(10)
    
    logger.info("\n✅ TOP 10 CATEGORÍAS CON MENOR ABANDONO DE CARRITO:")
    bottom_abandonment.select(
        "category_l1", "cart_abandonment_rate", "total_carts",
        "total_purchases", "conversion_rate"
    ).show(10, truncate=False)
    
    # =========================================================================
    # ANÁLISIS DE AOV (Average Order Value)
    # =========================================================================
    logger.info("\n" + "=" * 80)
    logger.info("ANÁLISIS DE AOV (Average Order Value) POR CATEGORÍA")
    logger.info("=" * 80)
    
    top_aov = df_category.orderBy(col("avg_order_value").desc()).limit(10)
    
    logger.info("\n💎 TOP 10 CATEGORÍAS POR AOV (ticket promedio):")
    top_aov.select(
        "category_l1", "avg_order_value", "total_purchases",
        "total_revenue", "conversion_rate"
    ).show(10, truncate=False)
    
    # =========================================================================
    # RESUMEN EJECUTIVO
    # =========================================================================
    logger.info("\n" + "=" * 80)
    logger.info("RESUMEN EJECUTIVO - PREGUNTA 5")
    logger.info("=" * 80)
    
    # Mejor categoría overall (balance conversión + revenue)
    best_overall = df_category.orderBy(
        (col("conversion_rate") * col("total_revenue")).desc()
    ).first()
    
    logger.info(f"\n🏆 MEJOR CATEGORÍA OVERALL:")
    logger.info(f"   Categoría: {best_overall.category_l1}")
    logger.info(f"   Conversión: {best_overall.conversion_rate:.2f}%")
    logger.info(f"   Revenue: ${best_overall.total_revenue:,.2f}")
    logger.info(f"   AOV: ${best_overall.avg_order_value:.2f}")
    logger.info(f"   Abandono: {best_overall.cart_abandonment_rate:.2f}%")
    
    # Stats generales
    logger.info(f"\n📊 ESTADÍSTICAS GENERALES:")
    stats = df_category.select(
        avg("conversion_rate").alias("avg_conversion"),
        avg("cart_abandonment_rate").alias("avg_abandonment"),
        avg("avg_order_value").alias("avg_aov"),
        spark_sum("total_revenue").alias("total_revenue"),
        spark_sum("total_purchases").alias("total_purchases")
    ).collect()[0]
    
    logger.info(f"   Conversión promedio: {stats.avg_conversion:.2f}%")
    logger.info(f"   Abandono promedio: {stats.avg_abandonment:.2f}%")
    logger.info(f"   AOV promedio: ${stats.avg_aov:.2f}")
    logger.info(f"   Revenue total: ${stats.total_revenue:,.2f}")
    logger.info(f"   Compras totales: {stats.total_purchases:,}")
    
    # =========================================================================
    # INSIGHTS Y RECOMENDACIONES
    # =========================================================================
    logger.info("\n" + "=" * 80)
    logger.info("🔍 INSIGHTS Y RECOMENDACIONES")
    logger.info("=" * 80)
    
    # Obtener datos para análisis
    top_conv_cat = top_conversion.first()
    top_rev_cat = top_revenue.first()
    top_aov_cat = top_aov.first()
    
    logger.info(f"\n💡 INSIGHTS CLAVE:")
    logger.info(f"   1. MEJOR CONVERSIÓN: {top_conv_cat.category_l1} ({top_conv_cat.conversion_rate:.2f}%)")
    logger.info(f"      → Usuarios saben qué quieren, deciden rápido")
    logger.info(f"   ")
    logger.info(f"   2. MAYOR REVENUE: {top_rev_cat.category_l1} (${top_rev_cat.total_revenue:,.2f})")
    logger.info(f"      → Categoría estrella del negocio")
    logger.info(f"   ")
    logger.info(f"   3. MAYOR AOV: {top_aov_cat.category_l1} (${top_aov_cat.avg_order_value:.2f})")
    logger.info(f"      → Productos premium, alto valor por transacción")
    
    logger.info(f"\n📈 RECOMENDACIONES:")
    logger.info(f"   → Invertir en marketing de categoría '{top_rev_cat.category_l1}' (genera más revenue)")
    logger.info(f"   → Optimizar checkout de '{top_conv_cat.category_l1}' (ya convierte bien)")
    logger.info(f"   → Upselling en '{top_aov_cat.category_l1}' (usuarios dispuestos a pagar más)")
    
    logger.info("\n" + "=" * 80)
    logger.info("✅ VALIDACIÓN COMPLETADA - PREGUNTA 5 RESPONDIDA")
    logger.info("=" * 80)
    
    spark.stop()


if __name__ == "__main__":
    # Configurar ruta
    project_root = Path(__file__).parent.parent.parent
    gold_path = project_root / "data" / "gold" / "fact_category_metrics"
    
    # Ejecutar validación
    validate_fact_category_metrics(gold_path)
