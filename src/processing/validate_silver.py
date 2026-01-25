"""
Script de validación Silver
Valida los datos transformados en la capa Silver
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct
from pathlib import Path

def validate_silver():
    # Crear sesión Spark
    spark = SparkSession.builder \
        .appName("Validate Silver") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    # Leer datos Silver
    silver_path = Path("C:/Users/micha/Documents/tfm-ecommerce/data/silver/events")
    print(f"Leyendo datos desde: {silver_path}")
    
    df = spark.read.parquet(str(silver_path))
    
    print("\n" + "="*80)
    print("VALIDACIÓN DE DATOS SILVER")
    print("="*80)
    
    # Schema
    print("\n📋 SCHEMA (20 columnas esperadas):")
    df.printSchema()
    
    # Conteo total
    total = df.count()
    print(f"\n📊 TOTAL REGISTROS: {total:,}")
    
    # Registros por día
    print("\n📅 REGISTROS POR DÍA:")
    df.groupBy("year", "month", "day") \
        .count() \
        .orderBy("year", "month", "day") \
        .show(10, truncate=False)
    
    # Distribución event_type
    print("\n🎯 DISTRIBUCIÓN EVENT_TYPE:")
    df.groupBy("event_type") \
        .count() \
        .withColumn("percentage", (col("count") / total * 100)) \
        .orderBy(col("count").desc()) \
        .show(truncate=False)
    
    # Distribución conversion_stage
    print("\n🛒 DISTRIBUCIÓN CONVERSION STAGE:")
    df.groupBy("conversion_stage") \
        .count() \
        .withColumn("percentage", (col("count") / total * 100)) \
        .orderBy(col("count").desc()) \
        .show(truncate=False)
    
    # Top 10 categorías L1
    print("\n📦 TOP 10 CATEGORÍAS L1:")
    df.groupBy("category_l1") \
        .count() \
        .orderBy(col("count").desc()) \
        .show(10, truncate=False)
    
    # Distribución temporal
    print("\n⏰ DISTRIBUCIÓN TIME_OF_DAY:")
    df.groupBy("time_of_day") \
        .count() \
        .withColumn("percentage", (col("count") / total * 100)) \
        .orderBy(col("count").desc()) \
        .show(truncate=False)
    
    # Estadísticas de precios
    print("\n💰 ESTADÍSTICAS DE PRECIOS:")
    df.select("price").summary("min", "max", "mean", "stddev").show(truncate=False)
    
    # Usuarios y productos únicos
    print("\n👥 ESTADÍSTICAS ÚNICAS:")
    print(f"Usuarios únicos: {df.select(countDistinct('user_id')).collect()[0][0]:,}")
    print(f"Productos únicos: {df.select(countDistinct('product_id')).collect()[0][0]:,}")
    print(f"Marcas únicas: {df.select(countDistinct('brand')).collect()[0][0]:,}")
    
    # Verificar nulls en columnas críticas
    print("\n⚠️  VERIFICACIÓN DE NULLS (columnas críticas):")
    critical_cols = ['event_time', 'event_type', 'product_id', 'user_id', 
                     'price', 'conversion_stage', 'category_l1']
    for col_name in critical_cols:
        null_count = df.filter(col(col_name).isNull()).count()
        print(f"  {col_name}: {null_count} nulls")
    
    print("\n" + "="*80)
    print("✅ VALIDACIÓN COMPLETADA")
    print("="*80)
    
    spark.stop()

if __name__ == "__main__":
    validate_silver()
