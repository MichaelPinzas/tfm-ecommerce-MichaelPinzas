"""
Script de prueba del PIPELINE COMPLETO Bronze → Silver
Ejecuta todas las transformaciones en secuencia y valida el resultado final
"""

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from bronze_to_silver import (
    get_spark_session, 
    clean_data, 
    enrich_temporal,
    parse_categories,
    calculate_conversion_stage
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Prueba el pipeline completo Bronze→Silver con todas las transformaciones"""
    
    # Configurar rutas
    project_root = Path(__file__).parent.parent.parent
    bronze_path = project_root / "data" / "bronze"
    
    logger.info("="*80)
    logger.info("PRUEBA: PIPELINE COMPLETO BRONZE → SILVER")
    logger.info("="*80)
    
    # Crear sesión Spark
    spark = get_spark_session("Test Complete Pipeline")
    
    # Leer muestra de datos (100k registros)
    logger.info(f"\nLeyendo muestra desde: {bronze_path}")
    table = pq.read_table(
        str(bronze_path),
        columns=['event_time', 'event_type', 'product_id', 'user_id', 
                'category_code', 'brand', 'price']
    )
    
    # Convertir timestamp a microsegundos
    table = table.cast(table.schema.set(
        table.schema.get_field_index('event_time'),
        pa.field('event_time', pa.timestamp('us', tz='UTC'))
    ))
    
    # Tomar muestra
    pdf = table.to_pandas().head(100000)
    logger.info(f"Muestra: {len(pdf):,} registros")
    
    # Convertir a Spark DataFrame
    df_bronze = spark.createDataFrame(pdf)
    
    logger.info(f"\nColumnas iniciales (Bronze): {len(df_bronze.columns)}")
    logger.info(f"Registros iniciales: {df_bronze.count():,}")
    
    # ========================================================================
    # EJECUTAR PIPELINE COMPLETO
    # ========================================================================
    
    logger.info("\n" + "="*80)
    logger.info("PASO 1/4: clean_data()")
    logger.info("="*80)
    df_clean = clean_data(df_bronze)
    logger.info(f"Registros después de limpieza: {df_clean.count():,}")
    
    logger.info("\n" + "="*80)
    logger.info("PASO 2/4: enrich_temporal()")
    logger.info("="*80)
    df_temporal = enrich_temporal(df_clean)
    logger.info(f"Columnas después de enriquecimiento temporal: {len(df_temporal.columns)}")
    
    logger.info("\n" + "="*80)
    logger.info("PASO 3/4: parse_categories()")
    logger.info("="*80)
    df_categories = parse_categories(df_temporal)
    logger.info(f"Columnas después de parsing de categorías: {len(df_categories.columns)}")
    
    logger.info("\n" + "="*80)
    logger.info("PASO 4/4: calculate_conversion_stage()")
    logger.info("="*80)
    df_silver = calculate_conversion_stage(df_categories)
    logger.info(f"Columnas finales (Silver): {len(df_silver.columns)}")
    
    # ========================================================================
    # VALIDACIONES FINALES
    # ========================================================================
    
    logger.info("\n" + "="*80)
    logger.info("VALIDACIONES DEL RESULTADO FINAL")
    logger.info("="*80)
    
    # 1. Verificar todas las columnas esperadas
    logger.info("\n1. Columnas en el DataFrame Silver:")
    expected_columns = {
        # Originales de Bronze
        'event_time', 'event_type', 'product_id', 'user_id', 
        'category_code', 'brand', 'price',
        # Agregadas por enrich_temporal
        'hour', 'day_of_week', 'is_weekend', 'time_of_day',
        # Agregadas por parse_categories
        'category_l1', 'category_l2', 'category_l3',
        # Agregada por calculate_conversion_stage
        'conversion_stage'
    }
    
    actual_columns = set(df_silver.columns)
    
    logger.info(f"\n   Columnas esperadas: {len(expected_columns)}")
    logger.info(f"   Columnas actuales: {len(actual_columns)}")
    
    missing = expected_columns - actual_columns
    extra = actual_columns - expected_columns
    
    if missing:
        logger.error(f"\n   ❌ Columnas faltantes: {missing}")
    else:
        logger.info(f"\n   ✅ Todas las columnas esperadas están presentes")
    
    if extra:
        logger.warning(f"\n   ⚠️ Columnas adicionales: {extra}")
    
    # 2. Schema del DataFrame Silver
    logger.info("\n2. Schema del DataFrame Silver:")
    df_silver.printSchema()
    
    # 3. Estadísticas básicas
    logger.info("\n3. Estadísticas básicas:")
    total_records = df_silver.count()
    logger.info(f"   Total registros: {total_records:,}")
    
    # Distribución por event_type
    logger.info("\n   Distribución por event_type:")
    event_dist = df_silver.groupBy("event_type").count() \
        .orderBy("count", ascending=False) \
        .collect()
    for row in event_dist:
        pct = (row['count'] / total_records) * 100
        logger.info(f"      {row['event_type']:>10s}: {row['count']:>7,} ({pct:>5.2f}%)")
    
    # Distribución por time_of_day
    logger.info("\n   Distribución por time_of_day:")
    time_dist = df_silver.groupBy("time_of_day").count() \
        .orderBy("time_of_day") \
        .collect()
    for row in time_dist:
        pct = (row['count'] / total_records) * 100
        logger.info(f"      {row['time_of_day']:>10s}: {row['count']:>7,} ({pct:>5.2f}%)")
    
    # Distribución por conversion_stage
    logger.info("\n   Distribución por conversion_stage:")
    stage_dist = df_silver.groupBy("conversion_stage").count() \
        .orderBy("count", ascending=False) \
        .collect()
    for row in stage_dist:
        pct = (row['count'] / total_records) * 100
        logger.info(f"      {row['conversion_stage']:>15s}: {row['count']:>7,} ({pct:>5.2f}%)")
    
    # Top 5 categorías L1
    logger.info("\n   Top 5 categorías nivel 1:")
    top_cats = df_silver.groupBy("category_l1").count() \
        .orderBy("count", ascending=False) \
        .limit(5) \
        .collect()
    for row in top_cats:
        pct = (row['count'] / total_records) * 100
        logger.info(f"      {row['category_l1']:>15s}: {row['count']:>7,} ({pct:>5.2f}%)")
    
    # 4. Verificar calidad de datos
    logger.info("\n4. Validaciones de calidad:")
    
    # No debe haber nulls en columnas críticas
    critical_cols = ['event_type', 'product_id', 'user_id', 'price', 
                     'brand', 'category_code', 'conversion_stage']
    
    all_clean = True
    for col_name in critical_cols:
        null_count = df_silver.filter(df_silver[col_name].isNull()).count()
        if null_count > 0:
            logger.error(f"   ❌ {col_name}: {null_count:,} nulls encontrados")
            all_clean = False
        else:
            logger.info(f"   ✅ {col_name}: 0 nulls")
    
    if all_clean:
        logger.info("\n   ✅ TODOS LOS CAMPOS CRÍTICOS ESTÁN LIMPIOS")
    
    # 5. Mostrar ejemplos del resultado final
    logger.info("\n5. Ejemplos de registros Silver (muestra aleatoria):")
    logger.info("\n   Columnas originales + nuevas:")
    df_silver.select(
        "event_time", "event_type", "user_id", "product_id", 
        "hour", "time_of_day", "is_weekend",
        "category_l1", "category_l2",
        "conversion_stage", "price"
    ).show(10, truncate=False)
    
    # 6. Resumen final
    logger.info("\n" + "="*80)
    logger.info("RESUMEN DEL PIPELINE")
    logger.info("="*80)
    logger.info(f"\n✅ Registros procesados:")
    logger.info(f"   Bronze (inicial):  {len(pdf):>10,}")
    logger.info(f"   Silver (final):    {total_records:>10,}")
    logger.info(f"   Filtrados:         {len(pdf) - total_records:>10,} ({((len(pdf) - total_records) / len(pdf) * 100):.2f}%)")
    
    logger.info(f"\n✅ Columnas agregadas:")
    logger.info(f"   Bronze:  {len(df_bronze.columns)} columnas")
    logger.info(f"   Silver: {len(df_silver.columns)} columnas")
    logger.info(f"   Nuevas:  {len(df_silver.columns) - len(df_bronze.columns)} columnas")
    
    logger.info(f"\n✅ Transformaciones aplicadas:")
    logger.info(f"   1. Limpieza de datos (precios, duplicados, nulls)")
    logger.info(f"   2. Enriquecimiento temporal (4 columnas)")
    logger.info(f"   3. Parsing de categorías (3 niveles)")
    logger.info(f"   4. Cálculo de conversion stage (funnel)")
    
    logger.info("\n" + "="*80)
    logger.info("✅ PIPELINE COMPLETO EJECUTADO EXITOSAMENTE")
    logger.info("="*80)
    
    spark.stop()

if __name__ == "__main__":
    main()
