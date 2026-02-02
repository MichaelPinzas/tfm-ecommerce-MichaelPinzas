"""
Transformación Silver → Gold Layer - Category Metrics
Crea fact_category_metrics: métricas por categoría para análisis de productos
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, 
    when, to_date, lpad, concat, lit
)
from pathlib import Path
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark_session():
    """Crear sesión Spark optimizada para Gold layer"""
    spark = SparkSession.builder \
        .appName("Silver to Gold - Category Metrics") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_fact_category_metrics(df_silver):
    """
    Crea fact_category_metrics a partir de datos Silver
    
    Args:
        df_silver: DataFrame de Silver Layer
    
    Returns:
        DataFrame con métricas por categoría
    """
    logger.info("\n🔨 Calculando métricas por categoría...")
    
    # Filtrar registros con categoría válida (usando category_l1)
    df_with_category = df_silver.filter(
        col("category_l1").isNotNull() & 
        (col("category_l1") != "unknown")
    )
    
    # Agregaciones por categoría
    fact_category = df_with_category.groupBy("category_l1") \
        .agg(
            # Conteos de eventos
            count("*").alias("total_events"),
            countDistinct("user_id").alias("unique_users"),
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
    
    # Calcular métricas derivadas
    logger.info("\n📊 Calculando métricas derivadas...")
    
    fact_category = fact_category \
        .withColumn("conversion_rate", 
                   when(col("total_views") > 0,
                        (col("total_purchases") / col("total_views")) * 100)
                   .otherwise(0)) \
        .withColumn("cart_abandonment_rate",
                   when(col("total_carts") > 0,
                        ((col("total_carts") - col("total_purchases")) / col("total_carts")) * 100)
                   .otherwise(0)) \
        .withColumn("add_to_cart_rate",
                   when(col("total_views") > 0,
                        (col("total_carts") / col("total_views")) * 100)
                   .otherwise(0)) \
        .withColumn("cart_to_purchase_rate",
                   when(col("total_carts") > 0,
                        (col("total_purchases") / col("total_carts")) * 100)
                   .otherwise(0)) \
        .withColumn("avg_order_value",
                   when(col("total_purchases") > 0,
                        col("total_revenue") / col("total_purchases"))
                   .otherwise(0)) \
        .withColumn("revenue_per_user",
                   when(col("unique_users") > 0,
                        col("total_revenue") / col("unique_users"))
                   .otherwise(0))
    
    # Calcular porcentaje del total
    total_revenue = fact_category.select(spark_sum("total_revenue")).collect()[0][0]
    total_purchases = fact_category.select(spark_sum("total_purchases")).collect()[0][0]
    
    fact_category = fact_category \
        .withColumn("pct_revenue", (col("total_revenue") / total_revenue * 100)) \
        .withColumn("pct_purchases", (col("total_purchases") / total_purchases * 100))
    
    # Ordenar columnas y seleccionar
    fact_category = fact_category.select(
        # Identificador
        "category_l1",
        
        # Conteos base
        "total_events", "unique_users", "unique_products",
        
        # Eventos por tipo
        "total_views", "total_carts", "total_purchases",
        
        # Revenue
        "total_revenue", "avg_price", "avg_order_value", "revenue_per_user",
        
        # Métricas de conversión
        "conversion_rate", "add_to_cart_rate", "cart_to_purchase_rate", "cart_abandonment_rate",
        
        # Porcentajes
        "pct_revenue", "pct_purchases"
    ).orderBy(col("total_revenue").desc())
    
    return fact_category


def main(silver_path, gold_path):
    """Función principal"""
    spark = get_spark_session()
    
    logger.info("=" * 80)
    logger.info("CREANDO GOLD LAYER - fact_category_metrics")
    logger.info("=" * 80)
    
    # 1. LEER SILVER LAYER
    logger.info(f"\n📖 Leyendo Silver Layer desde: {silver_path}")
    df_silver = spark.read.parquet(str(silver_path))
    
    total_records = df_silver.count()
    logger.info(f"   Total registros Silver: {total_records:,}")
    
    # Contar registros con categoría válida
    valid_category = df_silver.filter(
        col("category_l1").isNotNull() & 
        (col("category_l1") != "unknown")
    ).count()
    
    logger.info(f"   Registros con categoría válida: {valid_category:,}")
    logger.info(f"   Porcentaje: {(valid_category/total_records)*100:.1f}%")
    
    # 2. CREAR FACT_CATEGORY_METRICS
    fact_category = create_fact_category_metrics(df_silver)
    
    # 3. MOSTRAR PREVIEW
    logger.info("\n📋 PREVIEW de fact_category_metrics:")
    fact_category.select(
        "category_l1", "total_purchases", "total_revenue",
        "conversion_rate", "avg_order_value"
    ).show(20, truncate=False)
    
    # 4. GUARDAR EN GOLD
    output_path = gold_path / "fact_category_metrics"
    logger.info(f"\n💾 Guardando fact_category_metrics en: {output_path}")
    
    fact_category.coalesce(1).write \
        .mode("overwrite") \
        .parquet(str(output_path))
    
    # 5. VALIDACIÓN
    logger.info("\n✅ VALIDACIÓN:")
    saved_count = spark.read.parquet(str(output_path)).count()
    logger.info(f"   Categorías guardadas: {saved_count}")
    
    # 6. ESTADÍSTICAS FINALES
    logger.info("\n📊 ESTADÍSTICAS POR CATEGORÍA:")
    stats = fact_category.select(
        spark_sum("total_purchases").alias("total_purchases"),
        spark_sum("total_revenue").alias("total_revenue"),
        avg("conversion_rate").alias("avg_conversion_rate")
    ).collect()[0]
    
    logger.info(f"   Total compras: {stats.total_purchases:,}")
    logger.info(f"   Revenue total: ${stats.total_revenue:,.2f}")
    logger.info(f"   Conversión promedio: {stats.avg_conversion_rate:.2f}%")
    
    logger.info("\n" + "=" * 80)
    logger.info("✅ FACT_CATEGORY_METRICS CREADO EXITOSAMENTE")
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
