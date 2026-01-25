"""
Script de prueba para enrich_temporal()
Valida que las columnas temporales se generen correctamente
"""

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from bronze_to_silver import get_spark_session, clean_data, enrich_temporal
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Prueba la función enrich_temporal con una muestra de datos"""
    
    # Configurar rutas
    project_root = Path(__file__).parent.parent.parent
    bronze_path = project_root / "data" / "bronze"
    
    logger.info("="*80)
    logger.info("PRUEBA: enrich_temporal()")
    logger.info("="*80)
    
    # Crear sesión Spark
    spark = get_spark_session("Test Enrich Temporal")
    
    # Leer muestra de datos (primeros 50k registros)
    logger.info(f"Leyendo muestra desde: {bronze_path}")
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
    pdf = table.to_pandas().head(50000)
    logger.info(f"Muestra: {len(pdf):,} registros")
    
    # Convertir a Spark DataFrame
    df = spark.createDataFrame(pdf)
    
    # Aplicar clean_data primero
    df = clean_data(df)
    
    # Aplicar enrich_temporal
    logger.info("\n" + "="*80)
    logger.info("EJECUTANDO: enrich_temporal()")
    logger.info("="*80)
    df_enriched = enrich_temporal(df)
    
    # Validaciones
    logger.info("\n" + "="*80)
    logger.info("VALIDACIONES")
    logger.info("="*80)
    
    # 1. Verificar que se agregaron las columnas
    expected_cols = ['hour', 'day_of_week', 'is_weekend', 'time_of_day']
    actual_cols = df_enriched.columns
    
    logger.info("\n1. Columnas agregadas:")
    for col in expected_cols:
        if col in actual_cols:
            logger.info(f"   ✅ {col}")
        else:
            logger.error(f"   ❌ {col} NO ENCONTRADA")
    
    # 2. Verificar rangos de valores
    logger.info("\n2. Rangos de valores:")
    
    # hour debe estar entre 0-23
    hour_stats = df_enriched.select("hour").describe().collect()
    logger.info(f"   hour - min: {hour_stats[3][1]}, max: {hour_stats[4][1]}")
    
    # day_of_week debe estar entre 1-7
    dow_stats = df_enriched.select("day_of_week").describe().collect()
    logger.info(f"   day_of_week - min: {dow_stats[3][1]}, max: {dow_stats[4][1]}")
    
    # 3. Distribución de time_of_day
    logger.info("\n3. Distribución time_of_day:")
    time_dist = df_enriched.groupBy("time_of_day").count().orderBy("time_of_day").collect()
    for row in time_dist:
        pct = (row['count'] / df_enriched.count()) * 100
        logger.info(f"   {row['time_of_day']:>10s}: {row['count']:>6,} ({pct:>5.2f}%)")
    
    # 4. Proporción de fin de semana
    logger.info("\n4. Proporción is_weekend:")
    weekend_dist = df_enriched.groupBy("is_weekend").count().collect()
    for row in weekend_dist:
        pct = (row['count'] / df_enriched.count()) * 100
        label = "Weekend" if row['is_weekend'] else "Weekday"
        logger.info(f"   {label:>10s}: {row['count']:>6,} ({pct:>5.2f}%)")
    
    # 5. Mostrar ejemplos
    logger.info("\n5. Ejemplos de registros enriquecidos:")
    df_enriched.select(
        "event_time", "hour", "day_of_week", "is_weekend", "time_of_day"
    ).show(10, truncate=False)
    
    logger.info("\n" + "="*80)
    logger.info("✅ PRUEBA COMPLETADA")
    logger.info("="*80)
    
    spark.stop()

if __name__ == "__main__":
    main()
